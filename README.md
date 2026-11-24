# 乡村弹幕投影节奏场测

江幕夜间艺术组在不同乡镇投影点复用节奏，先比较真实拍点序列，再核查计量和切换分路。

## 现场工作与联系人

模拟客户为江幕夜间艺术组。灯光 cue 序列、场地试投偏差、节奏簇、对场 cue 合用；跨场地电表协议不同拒绝合用，记录分路提示。

- 场地配电和投影线 `venues`，字段为 `protocol`（计量协议）、`channels`（可用分路）、`ready`（已签场地）。模型约束位于 `backend/domain/venues_model.py`，规则位于 `venues_service.py`。
- 试投节奏轨迹 `cue_tracks`，字段为 `venue`（场地）、`timings`（拍点序列）、`shift`（总偏移秒）、`locked`（模板已见证）。模型约束位于 `backend/domain/cue_tracks_model.py`，规则位于 `cue_tracks_service.py`。
- 对场节奏簇 `cue_clusters`，字段为 `track`（输入素材）、`representative`（簇样板）、`distance`（DTW 距离）、`members`（簇内轨迹）、`token`（计算凭据）。模型约束位于 `backend/domain/cue_clusters_model.py`，规则位于 `cue_clusters_service.py`。
- 交场分路回单 `cue_routing`，字段为 `cluster`（节奏簇）、`destination`（目标场地）、`route`（物理分路）、`status`（交场状态）。模型约束位于 `backend/domain/cue_routing_model.py`，规则位于 `cue_routing_service.py`。

团队为模拟的8 人夜间艺术团队，负责村站联动试投与线路确认。

现场用一张纸记拍点、另一张纸记分路。相近节奏难手工比对，跨场计量口径容易串用，目标场已占分路经常要现场重分配。

已确认的试投片段进入 DTW 比较，联动岗领取合用回单或独立分路提示。

### 一天的业务操作

1. 16:20 对 HALL-A 输入六个实测拍点。
2. 16:40 经放映领班见证锁定。
3. 17:30 比对 15 条素材。
4. 18:20 签认 BEAT-24 节奏簇。
5. 19:00 HALL-B 协议区别触发分路提醒。
6. 19:30 HALL-C 通过 MTR-030，签发 C-1 合用回单。

## 启动

```sh
cp .env.example .env
docker compose up -d --build --wait
docker compose exec -T backend python -m scripts.seed_demo
```

打开 `http://127.0.0.1:18329`。FastAPI OpenAPI 在 `http://127.0.0.1:19329/docs`。
Compose 运行 Nginx、Python FastAPI、独立 Redis 消费进程、SQL 数据库和 Redis，SQL 与 Redis 使用命名卷。
本机开发时，在 Python 3.11 环境安装 `backend/requirements.txt` 后从 backend 目录运行 `uvicorn core.app:app`。须提供 `DATABASE_URL`、`REDIS_URL`、`JWT_SECRET`，`SEED_DEMO` 控制演示初始化。

| 演示账号 | 权限 |
|---|---|
| operator@demo.test | 提交领域记录和计算请求 |
| reviewer@demo.test | 提交记录并执行有业务约束的复核操作 |
| viewer@demo.test | 浏览业务台账 |

三个本地演示账号均使用 `DemoC0de!2026`。角色在 `backend/core/roles.py` 和 `frontend/src/store.js` 中声明。计算状态在 worker、queueing、API 和同一前端 store 中使用。

## 页面与接口

| 业务任务 | FastAPI 路由 | 页面 |
|---|---|---|
| 场地配电和投影线 | `/venues` /venues/sign-venue | `frontend/src/pages/venues.js` |
| 试投节奏轨迹 | `/cue_tracks` /cue_tracks/record-track，/cue_tracks/lock-track | `frontend/src/pages/cue_tracks.js` |
| 对场节奏簇 | `/cue_clusters` /cue_clusters/compare-cues，/cue_clusters/resume-cues，/cue_clusters/save-cluster | `frontend/src/pages/cue_clusters.js` |
| 交场分路回单 | `/cue_routing` /cue_routing/share-cue，/cue_routing/record-separate | `frontend/src/pages/cue_routing.js` |

### 中断续跑与同一比较

试投算到一半停机时，晚班不用从头等：

- 发起比较即把**当时的拍点快照**固化到 Redis，任务号由快照内容（轨迹编号、拍点、偏移）哈希派生。同一比较重复提交或两人同时提交，都返回同一个任务号，续跑永远沿用发起时拍点，不重新查库。
- 每个轨迹对的 DTW 距离以 `HSETNX` 逐对写入检查点（`cmp:<token>:distances`）。中断恢复后只补检查点里缺失的距离对，已算完的对不重算、不覆盖；两个 worker 同时续跑同一任务时，每对距离也只算一次。
- 状态只允许 `queued/running -> ready/rejected` 单向流转。原那次晚些恢复不能覆盖接手后留下的进度或结论，终态任务续跑直接返回结论、不再入队。
- `/cue_clusters/save-cluster` 是领班签认闸门：距离算齐（`progress.done == progress.total`）且完成归簇（`status=ready` 且结论字段完整）才允许写入台账，半成品返回 409。
- 实现位于 `backend/core/resumable.py`，逐对距离与归簇拆分在 `backend/domain/learning.py`，worker 分派在 `backend/core/worker.py`。

SQL 实体与约束在 `backend/domain` 下维护。前端使用原生 ES modules。核心算子为 DTW + 凝聚层次聚类。以绝对时间差为局部代价，DTW 归一化后作为层次聚类的距离矩阵，簇内总距离最短的轨迹为代表。

拍点与场地端为模拟记录。应用产出路线回单，无线路开关控制。

[MIT](LICENSE)
