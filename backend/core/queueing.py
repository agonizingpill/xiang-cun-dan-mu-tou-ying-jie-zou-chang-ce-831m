import json
from uuid import uuid4

from fastapi import HTTPException
from redis import Redis

from core.config import REDIS_URL

broker = Redis.from_url(REDIS_URL, decode_responses=True)
QUEUE = 'compute_requests'

def submit(user, payload):
    token = uuid4().hex
    item = {'token': token, 'owner': user['sub'], 'input': payload}
    with broker.pipeline() as pipe:
        pipe.set(f'compute:{token}', json.dumps({'owner': user['sub'], 'status': 'queued'}), ex=86400)
        pipe.rpush(QUEUE, json.dumps(item))
        pipe.execute()
    return {'token': token, 'status': 'queued'}

def _load(token):
    raw = broker.get(f'compute:{token}')
    if raw is not None:
        return json.loads(raw), False
    from core import resumable
    record = resumable.view_record(token)
    if record is not None:
        return record, True
    raise HTTPException(404, detail='计算任务不存在或已过期')

def view(user, token):
    record, is_resumable = _load(token)
    if is_resumable:
        # 可续跑比较要支持晚班接手：作业员与复核员都能看到同一个比较，只读访客不可见。
        if user['role'] not in ('operator', 'reviewer'):
            raise HTTPException(403, detail='仅作业员与复核员可查看该比较')
    elif record['owner'] != user['sub'] and user['role'] != 'reviewer':
        raise HTTPException(403, detail='仅提交人和复核员可查看计算')
    return {k: v for k, v in record.items() if k != 'owner'}

def result(user, token):
    data = view(user, token)
    if data['status'] != 'ready':
        raise HTTPException(409, detail='等待计算完成后再提交')
    return data
