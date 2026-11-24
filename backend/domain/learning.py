import numpy as np
from sklearn.cluster import AgglomerativeClustering

def dtw(a,b):
    dist=np.full((len(a)+1,len(b)+1),np.inf)
    dist[0,0]=0
    for i,x in enumerate(a,1):
        for j,y in enumerate(b,1):
            dist[i,j]=abs(x-y)+min(dist[i-1,j],dist[i,j-1],dist[i-1,j-1])
    return float(dist[-1,-1]/(len(a)+len(b)))

def calculate(data):
    """一次性计算（保留给旧路径与单元测试）。"""
    rows=data['references']+[data['track']]
    count=len(rows)
    distances=np.zeros((count,count))
    for i in range(count):
        for j in range(i):
            distances[i,j]=distances[j,i]=dtw(rows[i]['timings'],rows[j]['timings'])
    return clusterize(rows, distances)

def pair_distance(rows, i, j):
    """单个轨迹对的 DTW 距离，供可续跑任务逐对检查点化。"""
    return dtw(rows[i]['timings'], rows[j]['timings'])

def iter_pairs(count):
    """固定的距离对遍历次序，恢复时按同一顺序补缺。"""
    for i in range(count):
        for j in range(i):
            yield (i, j)

def clusterize(rows, distances):
    """距离矩阵算齐后做凝聚层次归簇，输出簇样板与簇内成员。"""
    count=len(rows)
    labels=AgglomerativeClustering(n_clusters=3,metric='precomputed',linkage='average').fit_predict(distances)
    members=[i for i in range(count-1) if labels[i]==labels[-1]]
    if not members:
        raise ValueError('当前轨迹独立成簇，需追加场测')
    representative=min(members,key=lambda i:sum(distances[i,j] for j in members))
    return {'representative':rows[representative]['code'],'distance':round(float(distances[-1,representative]),4),
            'members':[rows[i]['code'] for i in members],'reference_count':count-1}

def build_matrix(count, checked):
    """由检查点还原完整距离矩阵；缺任何一对都返回 None，拒绝半成品归簇。"""
    matrix=np.zeros((count,count))
    for (i,j),value in checked.items():
        matrix[i,j]=matrix[j,i]=value
    for i,j in iter_pairs(count):
        if (i,j) not in checked:
            return None
    return matrix
