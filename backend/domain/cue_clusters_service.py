from core import resumable
from core.queueing import result
from core.records import get, insert, need, scan
from domain.venues_model import venues
from domain.cue_tracks_model import cue_tracks
from domain.cue_clusters_model import cue_clusters
from domain.cue_routing_model import cue_routing


def compare_cues(conn, user, data):
    q=get(conn,cue_tracks,data.track)
    ref=[x for x in scan(conn,cue_tracks) if x['locked'] and x['code']!=data.track]
    need(q['locked'] and 8<=len(ref)<=200,'需要已确认素材及足够参考片段')
    # 提交即固化发起时拍点快照；同一比较重复提交/续跑都返回同一个任务号。
    return resumable.submit_resumable(user,{'track':q,'references':ref})


def resume_cues(conn, user, data):
    # 续跑不重新查库、不刷新拍点：沿用发起时快照，只补没算完的距离。
    return resumable.resume(user, data.token)


def save_cluster(conn, user, data):
    run=result(user,data.token)
    record = resumable.view_record(data.token)
    stats=run['result']
    if record is not None:
        # 签认闸门：必须距离算齐且已完成归簇，半成品不得交领班签认。
        progress = record.get('progress') or {}
        need(progress.get('done') == progress.get('total') and progress.get('total', 0) > 0,
             '距离尚未算齐，不能签认节奏簇')
        need(record.get('status') == 'ready', '归簇尚未完成，不能签认节奏簇')
        need({'representative','distance','members'} <= set(stats.keys()),
             '归簇结论不完整，不能签认节奏簇')
        track_code = record['track']
    else:
        # 兼容旧版一次性计算任务。
        track_code = run['input']['track']['code']
    return insert(conn,cue_clusters,code=data.code,track=track_code,
      representative=stats['representative'],distance=stats['distance'],members=stats['members'],token=data.token)
