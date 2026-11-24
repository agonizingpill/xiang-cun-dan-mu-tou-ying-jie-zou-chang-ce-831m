"""可续跑的对场比较计算任务。

设计要点：
- 任务号由发起时的拍点快照（输入轨迹 code + timings + shift）派生，
  续跑与重复提交都落到同一个 token，沿用发起时的拍点而不是当前库内数据。
- 每个轨迹对的 DTW 距离以 HSETNX 写入检查点：中断恢复后只补未完成的距离对，
  两个 worker 同时续跑也不会重复计算或互相覆盖进度。
- 状态只允许 queued/running -> ready/rejected 单向流转，终态结论不可被旧任务覆盖。
"""
import hashlib
import json
import time

from redis import Redis

from core.config import REDIS_URL
from core.queueing import QUEUE, broker

TASK_TTL = 86400
LOCK_TTL_MS = 10000

# 复用 queueing 的连接，保证 worker 与 API 看到同一个 broker。
store: Redis = broker


def _key(job_id: str, suffix: str) -> str:
    return f'cmp:{job_id}:{suffix}'


def _snapshot_rows(payload):
    """提取发起时的拍点快照，行序固定：参考片段在前，输入素材在末位。"""
    rows = list(payload['references']) + [payload['track']]
    return [{
        'code': row['code'],
        'timings': list(row['timings']),
        'shift': row.get('shift', 0.0),
    } for row in rows]


def derive_job_id(payload) -> str:
    """由发起时拍点快照派生确定性任务号。"""
    basis = json.dumps(_snapshot_rows(payload), ensure_ascii=False, sort_keys=True,
                       separators=(',', ':'))
    return hashlib.sha256(basis.encode('utf-8')).hexdigest()


def total_pairs(count: int) -> int:
    return count * (count - 1) // 2


def _enqueue(pipe, job_id, owner, payload):
    item = json.dumps({'token': job_id, 'owner': owner,
                       'input': payload, 'kind': 'resumable_compare'})
    pipe.rpush(QUEUE, item)


def submit_resumable(user, payload):
    """幂等提交：同一拍点快照永远返回同一个任务号。

    已存在的任务（含中断遗留）直接复用，绝不重建、绝不重置进度；
    不存在则把发起时快照固化下来并入队。
    """
    job_id = derive_job_id(payload)
    snapshot = _snapshot_rows(payload)
    lock_key = _key(job_id, 'lock')
    compute_key = _key(job_id, 'compute')
    # 短锁只保护“查状态—建任务”这一小段临界区，防两人同时提交各建一份。
    if not store.set(lock_key, user['sub'], nx=True, px=LOCK_TTL_MS):
        # 另一请求正在建同一任务：等它落键后直接复用，保证两人拿到同一比较。
        for _ in range(20):
            existing = store.get(compute_key)
            if existing is not None:
                return {'token': job_id, 'status': json.loads(existing)['status'], 'resumed': True}
            time.sleep(0.05)
        raise RuntimeError('比较任务正在建立，请稍后重试')
    try:
        raw = store.get(compute_key)
        if raw is not None:
            return {'token': job_id, 'status': json.loads(raw)['status'], 'resumed': True}
        record = {'owner': user['sub'], 'status': 'queued', 'track': snapshot[-1]['code'],
                  'progress': {'done': 0, 'total': total_pairs(len(snapshot))}}
        with store.pipeline() as pipe:
            pipe.set(compute_key, json.dumps(record), ex=TASK_TTL)
            # 发起时拍点快照独立固化，续跑只认这份数据。
            pipe.set(_key(job_id, 'input'), json.dumps(snapshot), ex=TASK_TTL)
            _enqueue(pipe, job_id, user['sub'], payload)
            pipe.execute()
        return {'token': job_id, 'status': 'queued', 'resumed': False}
    finally:
        store.delete(lock_key)


def resume(user, job_id):
    """续跑：沿用发起时拍点，只在任务未结束时补一条队列消息。"""
    compute_key = _key(job_id, 'compute')
    raw = store.get(compute_key)
    if raw is None:
        from fastapi import HTTPException
        raise HTTPException(404, detail='比较任务不存在或已过期')
    record = json.loads(raw)
    if record['status'] in ('ready', 'rejected'):
        return {'token': job_id, 'status': record['status'], 'resumed': True,
                'finished': True}
    snapshot = json.loads(store.get(_key(job_id, 'input')))
    payload = {'track': snapshot[-1], 'references': snapshot[:-1]}
    with store.pipeline() as pipe:
        _enqueue(pipe, job_id, record['owner'], payload)
        # 中断可能停在 running：续跑先回到 queued，等待 worker 接手。
        if record['status'] != 'queued':
            record['status'] = 'queued'
            pipe.set(compute_key, json.dumps(record), ex=TASK_TTL, xx=True)
        pipe.execute()
    return {'token': job_id, 'status': 'queued', 'resumed': True, 'finished': False}


def load_snapshot(job_id):
    raw = store.get(_key(job_id, 'input'))
    if raw is None:
        return None
    return json.loads(raw)


def get_compute(job_id):
    raw = store.get(_key(job_id, 'compute'))
    return None if raw is None else json.loads(raw)


def set_running(job_id, owner):
    """标记任务开跑。

    queued -> running；已经是 running（另一路 worker 接手重复消息）时放行，
    逐对循环里的 HSETNX 会保证同一距离对只算一次。
    ready/rejected 终态返回终态记录，由调用方直接退出，绝不重算覆盖结论。
    """
    compute_key = _key(job_id, 'compute')
    raw = store.get(compute_key)
    if raw is None:
        return None
    record = json.loads(raw)
    if record['status'] in ('ready', 'rejected'):
        return record
    if record['status'] == 'queued':
        record['status'] = 'running'
        store.set(compute_key, json.dumps(record), ex=TASK_TTL, xx=True)
    return record


def checkpoint_distance(job_id, pair_key, value):
    """逐对距离检查点：已算过的对不重算、不覆盖（HSETNX 天然幂等）。"""
    field = f'{pair_key[0]}:{pair_key[1]}'
    return bool(store.hsetnx(_key(job_id, 'distances'), field, json.dumps(value)))


def distances(job_id):
    raw = store.hgetall(_key(job_id, 'distances'))
    return {tuple(map(int, field.split(':'))): json.loads(value) for field, value in raw.items()}


def publish_progress(job_id, done, total):
    compute_key = _key(job_id, 'compute')
    record = json.loads(store.get(compute_key))
    if record['status'] in ('ready', 'rejected'):
        return record
    record['status'] = 'running'
    record['progress'] = {'done': done, 'total': total}
    store.set(compute_key, json.dumps(record), ex=TASK_TTL, xx=True)
    return record


def finish_ready(job_id, result):
    """只允许非终态任务写入 ready；已存在的结论绝不被覆盖。"""
    compute_key = _key(job_id, 'compute')
    raw = store.get(compute_key)
    if raw is None:
        return None
    record = json.loads(raw)
    if record['status'] in ('ready', 'rejected'):
        return record
    record.update(status='ready', result=result)
    record['progress'] = {'done': record['progress']['total'],
                          'total': record['progress']['total']}
    store.set(compute_key, json.dumps(record, allow_nan=False), ex=TASK_TTL, xx=True)
    return record


def finish_rejected(job_id, detail):
    compute_key = _key(job_id, 'compute')
    raw = store.get(compute_key)
    if raw is None:
        return None
    record = json.loads(raw)
    if record['status'] in ('ready', 'rejected'):
        return record
    record.update(status='rejected', detail=detail)
    store.set(compute_key, json.dumps(record), ex=TASK_TTL, xx=True)
    return record


def view_record(job_id):
    return get_compute(job_id)
