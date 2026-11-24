import os
import sys
import threading
from pathlib import Path

# 必须在导入应用模块前配置环境。
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DB_PATH = Path('/tmp/cue_test.db')
if DB_PATH.exists():
    DB_PATH.unlink()
os.environ.setdefault('DATABASE_URL', f'sqlite:///{DB_PATH}')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:16379/15')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('SEED_DEMO', 'false')

import json  # noqa: E402

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from core import worker as worker_mod  # noqa: E402
from core.db import metadata, engine, transaction  # noqa: E402
from core.queueing import broker  # noqa: E402
from core.app import app  # noqa: E402
from domain.venues_model import venues  # noqa: E402
from domain.cue_tracks_model import cue_tracks  # noqa: E402
from domain.learning import calculate  # noqa: E402

metadata.create_all(engine)
broker.flushdb()

# 中断模拟闸门：置位时 worker 在接手可续跑作业进入 running 后阻塞。
GATE = threading.Event()
GATE.set()
worker_mod.gate = GATE


def _seed_tracks():
    from sqlalchemy import select
    with transaction() as conn:
        if conn.scalar(select(venues.c.code).where(venues.c.code == 'HALL-A')) is not None:
            return
        conn.execute(venues.insert(), [
            {'code': 'HALL-A', 'protocol': 'PULSE4', 'channels': 3, 'ready': True},
        ])
        rows = []
        # 10 条已锁定参考片段，拍点节奏分为两类 + 输入素材。
        for k in range(10):
            base = 0.6 + 0.04 * k
            rows.append({'code': f'CUE-{k:02d}', 'venue': 'HALL-A',
                         'timings': [round(base + 0.65 * n, 3) for n in range(6)],
                         'shift': round(0.02 * k, 2), 'locked': True})
        rows.append({'code': 'CUE-FINAL', 'venue': 'HALL-A',
                     'timings': [0.65, 1.3, 1.95, 2.6, 3.25, 3.9],
                     'shift': 0.0, 'locked': True})
        conn.execute(cue_tracks.insert(), rows)

_seed_tracks()

_stop = threading.Event()


def _worker_loop():
    # 复刻 worker.serve 的循环，但允许测试结束时退出并支持中断闸门。
    while not _stop.is_set():
        item = broker.blmove(worker_mod.QUEUE, f'{worker_mod.QUEUE}:processing',
                             timeout=1, src='LEFT', dest='RIGHT')
        if item is None:
            continue
        job = json.loads(item)
        try:
            if job.get('kind') == 'resumable_compare':
                worker_mod.run_resumable(job)
            else:
                status = {'owner': job['owner'], 'input': job['input']}
                try:
                    status.update(status='ready', result=calculate(job['input']))
                except ValueError as exc:
                    status.update(status='rejected', detail=str(exc))
                broker.set(f'compute:{job["token"]}', json.dumps(status), ex=86400)
        finally:
            broker.lrem(f'{worker_mod.QUEUE}:processing', 1, item)


threading.Thread(target=_worker_loop, daemon=True, name='test-worker').start()


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def users(client):
    def login(role):
        resp = client.post('/auth/login',
                           json={'email': f'{role}@demo.test', 'password': 'DemoC0de!2026'})
        assert resp.status_code == 200
        return resp.json()
    tokens = {role: login(role)['token'] for role in ('operator', 'reviewer', 'viewer')}

    def headers(role):
        return {'Authorization': f'Bearer {tokens[role]}'}
    return headers


@pytest.fixture
def clean_jobs():
    # 每个测试前后清掉可续跑任务键，保留台账数据。
    GATE.set()
    for key in broker.scan_iter('cmp:*'):
        broker.delete(key)
    broker.delete(worker_mod.QUEUE, f'{worker_mod.QUEUE}:processing')
    yield
    GATE.set()
    for key in broker.scan_iter('cmp:*'):
        broker.delete(key)


@pytest.fixture
def paused_worker():
    GATE.clear()
    yield GATE
    GATE.set()


def wait_for(client, headers, token, expected=('ready', 'rejected'), tries=100):
    import time
    data = None
    for _ in range(tries):
        resp = client.get(f'/compute/{token}', headers=headers)
        data = resp.json()
        if data['status'] in expected:
            return data
        time.sleep(0.05)
    raise AssertionError(f'任务未进入终态: {data}')


def wait_status(headers, client, token, status, tries=100):
    import time
    for _ in range(tries):
        data = client.get(f'/compute/{token}', headers=headers).json()
        if data['status'] == status:
            return data
        time.sleep(0.05)
    raise AssertionError(f'任务未进入 {status}: {data}')
