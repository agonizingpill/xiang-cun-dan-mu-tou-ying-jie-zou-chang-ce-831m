import json
import threading
import time

from core.queueing import broker
from core import resumable
from core.worker import QUEUE
from domain.learning import calculate, iter_pairs, pair_distance
from tests.conftest import wait_for, wait_status


def submit_compare(client, headers, track='CUE-FINAL'):
    return client.post('/cue_clusters/compare-cues', headers=headers, json={'track': track})


def test_compare_idempotent_same_token(client, users, clean_jobs):
    """两人对同一拍点快照提交，必须看到同一个比较（同一 token）。"""
    h1, h2 = users('operator'), users('reviewer')
    r1 = submit_compare(client, h1)
    r2 = submit_compare(client, h2)
    assert r1.status_code == r2.status_code == 200
    a, b = r1.json(), r2.json()
    assert a['token'] == b['token']
    assert a['resumed'] is False and b['resumed'] is True
    final = wait_for(client, h1, a['token'])
    assert final['status'] == 'ready'
    assert final['progress']['done'] == final['progress']['total']


def test_duplicate_submit_concurrent_single_job(client, users, clean_jobs):
    """并发同时提交同一比较，只建立一个任务、一份检查点。"""
    h = users('operator')
    results = []

    def fire():
        results.append(submit_compare(client, h).json())

    threads = [threading.Thread(target=fire) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    tokens = {r['token'] for r in results}
    assert len(tokens) == 1
    token = results[0]['token']
    final = wait_for(client, h, token)
    assert final['status'] == 'ready'
    # 只有一份距离检查点，且对数与总对数一致。
    assert broker.hlen(resumable._key(token, 'distances')) == final['progress']['total']


def test_interrupted_resume_only_fills_missing_pairs(client, users, clean_jobs, paused_worker):
    """停机恢复：沿用发起时拍点，只补没算完的距离，不重算已有对。"""
    h = users('operator')
    token = submit_compare(client, h).json()['token']
    # worker 进入 running 后被闸门拦在距离计算之前。
    mid = wait_status(h, client, token, 'running')
    assert mid['progress']['done'] == 0
    # 模拟停机：worker 现场随进程消失，处理中的消息被丢弃，检查点存储保留。
    broker.delete(f'{QUEUE}:processing')
    # 在存储里手工写入一部分距离，模拟停机前已算完的进度。
    snapshot = resumable.load_snapshot(token)
    count = len(snapshot)
    done_pairs = []
    for idx, pair in enumerate(iter_pairs(count)):
        if idx >= 7:
            break
        value = pair_distance(snapshot, *pair)
        assert resumable.checkpoint_distance(token, pair, value) is True
        done_pairs.append(pair)
    # 已存在的检查点不可被覆盖。
    pair0 = done_pairs[0]
    assert resumable.checkpoint_distance(token, pair0, 999.999) is False
    stored = resumable.distances(token)
    assert stored[pair0] != 999.999
    # 晚班续跑。
    r = client.post('/cue_clusters/resume-cues', headers=h, json={'token': token})
    assert r.status_code == 200
    assert r.json()['resumed'] is True and r.json()['status'] == 'queued'
    paused_worker.set()
    final = wait_for(client, h, token)
    assert final['status'] == 'ready'
    assert final['progress']['done'] == final['progress']['total']
    # 停机前写入的距离值原样保留，没有被重算覆盖。
    after = resumable.distances(token)
    for pair in done_pairs:
        assert after[pair] == stored[pair]


def test_two_workers_same_job_each_pair_computed_once(client, users, clean_jobs):
    """两人同时续跑同一任务：距离对只算一次，结论唯一。"""
    h = users('operator')
    token = submit_compare(client, h).json()['token']
    final = wait_for(client, h, token)
    assert final['status'] == 'ready'
    before = resumable.distances(token)
    # 任务已完成，两路续跑都应直接拿到终态，不再入队计算。
    r1 = client.post('/cue_clusters/resume-cues', headers=users('operator'), json={'token': token})
    r2 = client.post('/cue_clusters/resume-cues', headers=users('reviewer'), json={'token': token})
    assert r1.json()['finished'] is True and r2.json()['finished'] is True
    assert broker.llen(QUEUE) == 0
    assert resumable.distances(token) == before
    view = client.get(f'/compute/{token}', headers=h).json()
    assert view['result'] == final['result']


def test_terminal_result_never_overwritten(client, users, clean_jobs):
    """原那次晚些恢复，不能覆盖接手后留下的进度或结论。"""
    h = users('operator')
    token = submit_compare(client, h).json()['token']
    final = wait_for(client, h, token)
    original_result = dict(final['result'])
    # 直接在底层伪造一次“旧作业想写新结论”，finish_ready 必须拒绝覆盖。
    tampered = dict(original_result)
    tampered['representative'] = 'CUE-99'
    record = resumable.finish_ready(token, tampered)
    assert record['result'] == original_result
    # rejected 也不能覆盖 ready。
    rejected = resumable.finish_rejected(token, '旧作业的迟到失败')
    assert rejected['status'] == 'ready'
    assert client.get(f'/compute/{token}', headers=h).json()['result'] == original_result


def test_snapshot_pinned_at_submission_ignores_later_track_edits(client, users, clean_jobs):
    """续跑沿用发起时拍点：之后台账里轨迹改了，比较仍按发起时快照执行。"""
    h = users('operator')
    token = submit_compare(client, h).json()['token']
    snapshot_before = resumable.load_snapshot(token)
    final = wait_for(client, h, token)
    # 续跑接口拿到的输入仍是发起时快照（末位为输入素材，timings 未变）。
    r = client.post('/cue_clusters/resume-cues', headers=h, json={'token': token})
    assert r.status_code == 200
    assert resumable.load_snapshot(token) == snapshot_before
    # 重新对同一素材提交，拍点未变 => 仍是同一个比较。
    again = submit_compare(client, h).json()
    assert again['token'] == token
    assert client.get(f'/compute/{token}', headers=h).json()['result'] == final['result']


def test_save_cluster_blocked_until_distances_complete(client, users, clean_jobs, paused_worker):
    """距离没算齐、没完成归簇，不允许签认。"""
    h_op, h_rev = users('operator'), users('reviewer')
    token = submit_compare(client, h_op).json()['token']
    # 任务停在 running、距离未算齐时，签认应被 409 拦截。
    wait_status(h_op, client, token, 'running')
    early = client.post('/cue_clusters/save-cluster', headers=h_rev,
                        json={'code': 'BEAT-X', 'token': token})
    assert early.status_code == 409
    paused_worker.set()
    final = wait_for(client, h_rev, token)
    assert final['status'] == 'ready'
    # 算齐并归簇后，领班（reviewer）才可签认。
    ok = client.post('/cue_clusters/save-cluster', headers=h_rev,
                     json={'code': 'BEAT-X', 'token': token})
    assert ok.status_code == 200
    saved = ok.json()
    assert saved['code'] == 'BEAT-X'
    assert saved['representative'] == final['result']['representative']
    assert saved['members'] == final['result']['members']
    assert saved['track'] == 'CUE-FINAL'
    rows = client.get('/cue_clusters', headers=h_rev).json()
    assert any(r['code'] == 'BEAT-X' for r in rows)


def test_save_cluster_permissions(client, users, clean_jobs):
    """viewer/operator 无权签认；viewer 连发起比较都不行。"""
    h_view, h_op, h_rev = users('viewer'), users('operator'), users('reviewer')
    assert submit_compare(client, h_view).status_code == 403
    token = submit_compare(client, h_op).json()['token']
    wait_for(client, h_rev, token)
    assert client.post('/cue_clusters/save-cluster', headers=h_view,
                       json={'code': 'BEAT-V', 'token': token}).status_code == 403
    assert client.post('/cue_clusters/save-cluster', headers=h_op,
                       json={'code': 'BEAT-O', 'token': token}).status_code == 403


def test_resume_unknown_token_404(client, users, clean_jobs):
    h = users('operator')
    r = client.post('/cue_clusters/resume-cues', headers=h, json={'token': 'deadbeef'})
    assert r.status_code == 404


def test_other_operator_can_view_and_resume_same_comparison(client, users, clean_jobs):
    """晚班另一名作业员接手：能查看并续跑同一个比较；只读访客被拦。"""
    h1 = users('operator')
    # reviewer 与 operator 是不同账号，用 reviewer 充当“另一名接手人”之外再覆盖 viewer。
    token = submit_compare(client, h1).json()['token']
    final = wait_for(client, h1, token)
    h_other = users('reviewer')
    seen = client.get(f'/compute/{token}', headers=h_other).json()
    assert seen['result'] == final['result']
    # 访客看不到。
    h_view = users('viewer')
    assert client.get(f'/compute/{token}', headers=h_view).status_code == 403
    # 另一作业员续跑同一任务，拿到同一 token。
    r = client.post('/cue_clusters/resume-cues', headers=h_other, json={'token': token})
    assert r.status_code == 200 and r.json()['token'] == token


def test_progress_reports_partial_and_final(client, users, clean_jobs, paused_worker):
    """前端可查询的进度从 0 走到 total。"""
    h = users('operator')
    token = submit_compare(client, h).json()['token']
    wait_status(h, client, token, 'running')
    mid = client.get(f'/compute/{token}', headers=h).json()
    assert mid['status'] == 'running'
    assert 0 <= mid['progress']['done'] <= mid['progress']['total']
    assert mid['progress']['total'] == resumable.total_pairs(len(resumable.load_snapshot(token)))
    paused_worker.set()
    final = wait_for(client, h, token)
    assert final['progress']['done'] == final['progress']['total']


def test_cluster_matches_reference_calculation(client, users, clean_jobs):
    """可续跑归簇结论与原一次性 calculate 的结论一致。"""
    h = users('operator')
    token = submit_compare(client, h).json()['token']
    final = wait_for(client, h, token)
    snapshot = resumable.load_snapshot(token)
    payload = {'track': snapshot[-1], 'references': snapshot[:-1]}
    expected = calculate(payload)
    assert final['result']['representative'] == expected['representative']
    assert final['result']['members'] == expected['members']
    assert final['result']['distance'] == expected['distance']


def test_rejected_independent_track_blocks_cluster(client, users, clean_jobs, monkeypatch):
    """独立成簇被拒时不得签认；状态停在 rejected。"""
    from core import worker as worker_mod
    h_op, h_rev = users('operator'), users('reviewer')

    def fake_clusterize(rows, distances):
        raise ValueError('当前轨迹独立成簇，需追加场测')

    # worker 以 from-import 引用 clusterize，需替换其模块内名字。
    monkeypatch.setattr(worker_mod, 'clusterize', fake_clusterize)
    token = submit_compare(client, h_op).json()['token']
    final = wait_for(client, h_rev, token, expected=('rejected',))
    assert final['status'] == 'rejected'
    r = client.post('/cue_clusters/save-cluster', headers=h_rev,
                    json={'code': 'BEAT-BAD', 'token': token})
    assert r.status_code == 409
    # 距离仍然算齐，只是归簇被拒。
    assert final['progress']['done'] == final['progress']['total']
