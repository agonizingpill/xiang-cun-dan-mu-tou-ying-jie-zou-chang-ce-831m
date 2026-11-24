import json
from pathlib import Path
from traceback import print_exc

from core.queueing import QUEUE, broker
from core import resumable
from domain.learning import calculate, pair_distance, iter_pairs, build_matrix, clusterize

pulse = Path('/tmp/compute-heartbeat')

# 测试用中断闸门：置为未触发时，可续跑作业在进入 running 后、算距离前阻塞。
gate = None

def run_resumable(job):
    """可续跑比较：沿用发起时拍点快照，只补检查点里缺失的距离对。"""
    job_id = job['token']
    rows = resumable.load_snapshot(job_id)
    if rows is None:
        return
    # 终态任务（含另一路续跑已完成的）直接放行，绝不覆盖结论。
    record = resumable.set_running(job_id, job['owner'])
    if record is None or record['status'] in ('ready', 'rejected'):
        return
    if gate is not None:
        gate.wait()
    count = len(rows)
    total = resumable.total_pairs(count)
    try:
        for pair in iter_pairs(count):
            # HSETNX：已算过的对跳过且不覆盖；两路 worker 同时跑时只有一路真正计算。
            if resumable.checkpoint_distance(job_id, pair, pair_distance(rows, *pair)):
                done = broker.hlen(resumable._key(job_id, 'distances'))
                resumable.publish_progress(job_id, done, total)
        checked = resumable.distances(job_id)
        matrix = build_matrix(count, checked)
        if matrix is None:
            # 理论上不可达：所有对都已遍历。显式拒绝在半成品上归簇。
            raise ValueError('距离未算齐，拒绝归簇签认')
        result = clusterize(rows, matrix)
        resumable.finish_ready(job_id, result)
    except ValueError as exc:
        resumable.finish_rejected(job_id, str(exc))
    except Exception:
        print_exc()
        resumable.finish_rejected(job_id, '计算失败，请检查输入和 worker 日志')

def serve():
    while True:
        broker.ping()
        pulse.touch()
        item = broker.blmove(QUEUE, f'{QUEUE}:processing', timeout=2, src='LEFT', dest='RIGHT')
        if item is None:
            continue
        job = json.loads(item)
        try:
            if job.get('kind') == 'resumable_compare':
                # 状态与检查点由 resumable 模块维护。
                run_resumable(job)
            else:
                status = {'owner': job['owner'], 'input': job['input']}
                try:
                    status.update(status='ready', result=calculate(job['input']))
                except ValueError as exc:
                    status.update(status='rejected', detail=str(exc))
                except Exception:
                    print_exc()
                    status.update(status='rejected', detail='计算失败，请检查输入和 worker 日志')
                broker.set(f'compute:{job["token"]}', json.dumps(status, allow_nan=False), ex=86400)
        finally:
            # 无论哪条路径，消息处理完都移出处理队列，停机重启时再由入口回收。
            broker.lrem(f'{QUEUE}:processing', 1, item)
        pulse.touch()

if __name__ == '__main__':
    for raw in broker.lrange(f'{QUEUE}:processing', 0, -1):
        with broker.pipeline() as pipe:
            pipe.lpush(QUEUE, raw)
            pipe.lrem(f'{QUEUE}:processing', 1, raw)
            pipe.execute()
    serve()
