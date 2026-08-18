# osspilot-tenant-worker

OssPilot 租户后台（Go）。同一镜像两个 command：`scheduler` 入队，`worker` 消费。连**租户库**和 Redis 库 0，用 asynq。

不管生命周期（那是 [osspilot-ops-worker](https://github.com/cyxc1124/osspilot-ops-worker)）。

表结构与任务 JSON 跟租户 API 仓对齐；本仓是拷贝，之后会漂移。改 payload 或列要两边一起改。

## 进程

| command | 做什么 |
| --- | --- |
| `scheduler` | Redis 心跳互见；墙钟到点按 `bucket_id` 分片入细任务；同伴掉线则本槽立刻补漏。不消费队列、不扫 S3 |
| `worker` | 只消费细任务（+ 统计）和租户 API 入的批量/单桶清单。不 `Register`、启动不入队 |

心跳：`SET osspilot:sched:tenant:{HOSTNAME} 1 EX 15`，5s 续一次。认领：`SET NX osspilot:claim:{slot}:{type}:{id}`，TTL 为本槽剩余时间，任务跑完也不删。

`stats:requests` 不分片，全员抢一条。worker 并发读 `ASYNQ_CONCURRENCY`，默认 4。

## 任务

| 任务 | 来源 | 周期 | 做什么 |
| --- | --- | --- | --- |
| `objects:inventory_bucket` | scheduler（按桶）；租户 API | 15 分钟 / 入队即跑 | 只扫一个桶 |
| `objects:trash_bucket` | scheduler（按桶） | 整点 | 该桶过期 `.trash/` |
| `objects:versions_bucket` | scheduler（按桶） | 每 6 小时整点 | 该桶过期 `.versions/` |
| `objects:multipart_bucket` | scheduler（按桶） | 每 6 小时整点 | 该桶过期分片 |
| `stats:requests` | scheduler（全员抢一条） | 整点 | 审计聚合成请求/流量快照 |
| `objects:batch_delete` / `batch_copy` / `batch_move` | 租户 API | 入队即跑 | 批量 |
| `objects:inventory` / `trash` / `versions` / `multipart` | 旧队列残留 | — | 全量扫；新调度不再入 |

S3 凭证先看环境变量，再被租户库 `platform_settings` 覆盖。未设 S3 时进程仍听队列，清单 / 清理 / 批量跳过；`stats:requests` 照跑。

`GET /healthz` 默认 `:8080`（`HTTP_ADDR`）。

日志走 stdout（`log/slog`）。`LOG_LEVEL=debug|info|warn|error`（默认 info），`LOG_FORMAT=text|json`（默认 text）。每个任务有 `task start`，失败打 `task fail`。

## 本地

不跑迁移（迁移只在 API 仓）：

```bash
export DATABASE_URL=postgres://osspilot:osspilot@127.0.0.1:5432/osspilot_tenant?sslmode=disable
export REDIS_URL=redis://127.0.0.1:6379/0
go test ./...
go run ./cmd/scheduler
go run ./cmd/worker
```

未设 `REDIS_URL` / `DATABASE_URL` 时退出。

## 许可

AGPL-3.0-only

## 镜像

`Dockerfile` 编 `/app/worker` 和 `/app/scheduler`。入口默认 `command` 为 `worker`。
