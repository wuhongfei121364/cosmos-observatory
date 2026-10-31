# 宇宙万物观测站 · Cosmos Observatory

> **所选题目：题目二 —— 宇宙万物观测站**

一个单机系统：持续接收对象发生器推送的“宇宙对象”，可靠、去重、校验后落盘，并通过网页界面把这个**不断变化的世界**组织成可浏览、可搜索、可视化的形式。


## 功能一览

| 区域 | 内容 |
| --- | --- |
| 顶部指标 | 对象总数、接收批次、近 10 秒速率、已去重批次、拒收坏批次、分类树修订号、类型数、平均提交耗时 |
| 分类树 | 发生器下发的动态分类树（支持 create / rename / move），每个节点显示“含子类”的对象数，点击即按子树筛选 |
| 实时速率 | 近 5 分钟每秒入库对象数折线 |
| 对象浏览 | 按名称 / ID 搜索、按坐标系筛选、按分类子树筛选，游标分页（`before_seq`），点击查看完整 JSON 与分类路径 |
| 星图 | 按坐标系（银河 / 恒星系 / 地表）绘制最近 8000 个对象的位置散点，按顶层类别着色，点击光点看详情 |
| 属性探索 | 数据格式未预先给定：对选中分类子树抽样推断属性键、值类型分布、数值范围、常见值（可以直接看到 `readiness_score` 等字段的类型漂移） |
| 变化日志 | 分类树的重命名 / 迁移历史、被拒收批次及原因 |

## 目录结构

```
cosmos-observatory/
├── app/
│   ├── server.py          # HTTP：/ingest 接收 + /api/* 查询 + 静态页面（仅标准库）
│   ├── store.py           # SQLite 存储；单写线程 group commit；幂等去重；增量统计
│   ├── validate.py        # 消息/对象校验（整批原子接受或拒绝）
│   └── static/index.html  # 单页前端（原生 JS + Canvas，无构建步骤）
├── scripts/
│   ├── start_run.sh                 # 通过发生器控制 API 启动一轮
│   ├── fetch_receipt_and_verify.sh  # 下载运行记录并核对入库数据
│   └── verify_receipt.py            # 以发生器 receipt 为真值的数据正确性校验
├── Dockerfile
├── docker-compose.yml     # observatory（4 vCPU/4 GiB）+ 发生器（1 vCPU/256 MiB，profile: gen）
└── requirements.txt       # 空：只依赖 Python 3.8+ 标准库
```

## 快速开始（Docker，验收方式）

```sh
# 0. 载入发题包里的发生器镜像（只需一次）
docker load -i /path/to/宇宙万物观测站/flightgen-image.tar

# 1. 构建并启动观测站 + 发生器
docker compose --profile gen up -d --build

# 2. 打开界面
open http://127.0.0.1:8000

# 3. 启动一轮数据（参数：test_id 速率 时长秒 场景）
scripts/start_run.sh run-001 2000 120 evolving
#    运行中调速 / 查看状态（见《发生器操作说明》）
curl -fsS http://127.0.0.1:9001/rate -H 'Content-Type: application/json' -d '{"objects_per_second":0}'
curl -fsS http://127.0.0.1:9001/status

# 4. 结束后核对数据正确性
scripts/fetch_receipt_and_verify.sh run-001
```

compose 内部网络中发生器通过 `http://observatory:8000/ingest` 投递。若按《发生器操作说明》用 `docker run` 单独启动发生器，则：

```sh
TARGET=http://host.docker.internal:8000/ingest scripts/start_run.sh run-002 2000 120
```

数据持久化在 `./data/observatory.db`（挂载卷），容器重启后数据与分类树完整保留。

## 本地运行（不用 Docker）

```sh
cd app && DATA_DIR=../data PORT=8000 python3 server.py   # Python 3.8+，无需 pip install
```

## 数据探索结论（数据格式未预先提供）

通过抓包与 `flightgen preview/inspect` 探索得出：

- 所有消息 POST 到同一地址，`kind` 区分两类：
  - `types`：分类树修订，`revision` 递增，`changes[]` 中 `op ∈ {create, rename, move}`；
  - `objects`：对象批次，带 `batch_id`、依赖的 `tree_revision`、`objects[]`。
- 对象公共字段：`id / sequence / type_id / name / timestamp_ms / position / attributes`。
  `position` 可为 `null`，或带 `frame_kind`（`galactic` ly / `stellar_system` / `local_surface` m）的三维坐标；
  `attributes` 因类型而异（恒星光谱、飞机载客、设备读数、文档引用……），且存在类型漂移。
- 镜像默认参数注入三类故障：`--duplicate-rate 0.02`（已确认批次再投递一次）、`--incomplete-rate 0.01`（缺字段）、`--invalid-rate 0.01`（如 `attributes` 是数组、`sequence` 非法）。
  运行记录（receipt）里有 `fault_rejected_bitmap`，即期望接收端**拒收**这些故障探针。
- “部分发送依赖前序确认”：对象批次引用的分类树修订必须先被确认。

## 设计与关键取舍

### 写入路径：2xx = 已可靠保存

1. 处理线程解析 JSON、**整批校验**（任何一个对象非法 → 整批 `400`，不写入任何内容，并记录拒收原因）。
2. 若批次依赖的 `tree_revision` 还未入库 → `409`，让发生器重试（保证因果顺序）。
3. 通过校验的消息进入有界队列；**单写线程**把队列里积压的所有消息合并成**一个事务**写入并 `COMMIT`（`synchronous=FULL`，真正 fsync），之后才唤醒各请求返回 `200`。
   这是 group commit：fsync 次数与请求数解耦，压力越大单次提交合并得越多。
4. 幂等：`batches(test_id, batch_id)` 主键 `INSERT OR IGNORE`，重复投递直接返回 `200` 但不重复入库；对象 `(test_id, id)` 主键兜底。分类树修订按 `revision` 去重、检测跳号。
5. 背压：队列满时返回 `503`，由发生器重试，而不是让内存无限增长。
6. 统计（每类型计数、每坐标系计数、每秒入库数、批次计数）在**同一事务**中增量更新，界面查询不需要扫描全表。

### 为什么选 Python 标准库 + SQLite

- 120 分钟内可交付、可解释、零依赖：镜像构建不需要联网安装任何包，验收环境最稳。
- SQLite WAL：单写多读，读查询（界面）不阻塞写入；单文件，便于持久化卷与备份。
- 代价：Python 的 JSON 解析与 GIL 是吞吐上限（见下方实测）；若要更高，可把接收层换成 Go/Rust，或对 `attributes` 原文直接存储而不重新序列化。

### 查询路径

- 所有列表查询走索引 `(test_id, seq)` / `(test_id, type_id, seq)` / `(test_id, frame_kind, seq)`，按 `seq` 倒序游标分页，不使用 `OFFSET`。
- 分类子树筛选：内存中维护分类树，展开为 `type_id IN (...)`。
- 分类树会 rename / move：对象只存 `type_id`，名称与层级始终用最新树解释，所以改名/迁移后无需改写任何对象。

## 实际测试结果

测试环境：Linux arm64、4 vCPU、约 4 GiB 内存的虚拟机（与验收用的 Docker Desktop arm64 容器同架构；不同机器绝对数值会有差异），发生器二进制取自发题包镜像。

| 测试 | 参数 | 结果 |
| --- | --- | --- |
| **Docker Desktop（Mac，验收环境）端到端** | `docker compose --profile gen up`，evolving，2000 obj/s bursty，60 s | 717 批 / 71,700 对象全部确认并入库；17 个重复投递被去重；10/10 故障探针被拒收；分类树修订 36 = 36；0 个 4xx/5xx、0 重试；平均延迟 46 ms |
| 正确性（含故障注入） | evolving，1000 obj/s，20 s，dup 2% / incomplete 1% / invalid 1% | 393 批全部确认入库；7 个重复批次被去重；5 个故障探针全部拒收（发生器侧 `fault_rejected` 5/5）；分类树修订 40 = 40 |
| 极限吞吐 | stress，尽力发送（rate 0），16 workers，batch 100，30 s | **≈ 12,300 对象/秒（≈ 28 MB/s 原始负载）**，30 秒入库 369,800 个对象；0 个 4xx/5xx、0 超时；平均请求延迟 119 ms，平均每次提交 22.6 ms；62/62 个故障探针被拒收 |
| 崩溃恢复 | 运行第 8 秒 `kill -9` 服务进程，1 秒后重启 | 发生器出现 23 次网络错误、20 次重试；**所有已确认的 380 批全部在库**、无半截批次、无重复；3 批在宕机期间耗尽重试未确认，也未入库 |
| 查询 | 单批次 37 万对象、库内共 45 万对象 | 概览 3 ms、分类树 2 ms、列表（含子树筛选/名称搜索）1–6 ms、详情 1 ms；星图 8000 点 65 ms；属性推断（抽样 500）124 ms |

每一轮都用 `scripts/verify_receipt.py` 以发生器 receipt 为真值核对，检查项：

1. 所有发生器已确认（ack）的批次都已入库；
2. 库中没有任何未发送过的批次；
3. 故障探针从未入库，且发生器侧看到全部被拒收；
4. 对象数 = 已入库批次大小之和（没有半截批次）；
5. `已确认对象数 ≤ 入库对象数 ≤ 已发送对象数`（运行到期时被中断的在途请求可能已入库但未被确认，这是允许的）；
6. 分类树修订号与发生器确认的一致。

## 已知限制

- 名称搜索使用 `LIKE '%q%'`，命中多时很快（按 seq 倒序遇到 limit 就停），极少命中时需要扫描较多行；更大规模应加 FTS5 索引（会牺牲部分写入吞吐）。
- 吞吐上限受 Python 单进程 JSON 解析与 GIL 限制，4 vCPU 没有吃满；可通过多进程接收 + 单写进程，或换编译型语言进一步提升。
- 星图为最近 N 个对象的二维投影（x/y），未做空间索引与全量聚合渲染；不同 `frame_id`（星系/恒星系/地表）未做层级坐标换算。
- 属性探索为抽样推断（最近 500 个对象），不是全量 schema。
- 未做数据保留/归档策略；实测约 2.7 KB/对象（含原文与索引），20 GiB 约可容纳 700 万对象；更大规模需压缩原文或冷热分层。
- `synchronous=FULL` 优先保证“2xx 即落盘”；如可接受掉电丢最近事务，可改为 `NORMAL` 以换取更高吞吐。
