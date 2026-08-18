# osspilot-tenant-worker

OssPilot 租户后台 worker（Go）。连**租户库**和 Redis，用 asynq。消费 [osspilot-tenant-api](https://github.com/cyxc1124/osspilot-tenant-api) 入队的任务，并自己定时扫清单 / 回收站 / 版本 / 分片 / 请求统计。

不管生命周期（那是 [osspilot-ops-worker](https://github.com/cyxc1124/osspilot-ops-worker)）。

表结构与任务 JSON 跟租户 API 仓对齐；本仓是拷贝，之后会漂移。改 payload 或列要两边一起改。

## 任务

| 任务 | 来源 | 周期 | 做什么 |
| --- | --- | --- | --- |
| `objects:inventory` | 本进程调度；启动时再入一次 | 15 分钟 | 扫全部 active 桶，刷新 `object_records`，写 `inventoried_at` |
| `objects:inventory_bucket` | 租户 API（运营登记/导入、授权变更、生命周期删完也会打内部接口入队） | 入队即跑 | 只扫一个桶 |
| `objects:trash` | 本进程调度 | 1 小时 | 按 `platform_settings` 清过期 `.trash/`（开关关或天数 &lt; 1 则跳过） |
| `objects:versions` | 本进程调度 | 6 小时 | 按设置清过期 `.versions/` |
| `objects:multipart` | 本进程调度 | 6 小时 | 按设置 abort 过期分片并改上传记录 |
| `stats:requests` | 本进程调度；启动时再入一次 | 1 小时 | 把审计聚合成请求/流量快照（不打 S3） |
| `objects:batch_delete` | 租户 API | 入队即跑 | 批量删除 |
| `objects:batch_copy` | 租户 API | 入队即跑 | 批量复制 |
| `objects:batch_move` | 租户 API | 入队即跑 | 批量移动 |

S3 凭证先看环境变量，再被租户库 `platform_settings` 覆盖。未设 S3 时进程仍听队列，清单 / 清理 / 批量跳过；`stats:requests` 照跑。

`GET /healthz` 默认 `:8080`（`HTTP_ADDR`）。

日志走 stdout（`log/slog`）。`LOG_LEVEL=debug|info|warn|error`（默认 info），`LOG_FORMAT=text|json`（默认 text）。每个任务有 `task start`，失败打 `task fail`。

## 本地

不跑迁移（迁移只在 API 仓）：

```bash
export DATABASE_URL=postgres://osspilot:osspilot@127.0.0.1:5432/osspilot_tenant?sslmode=disable
export REDIS_URL=redis://127.0.0.1:6379/0
go test ./...
go run ./cmd/worker
```

未设 `REDIS_URL` / `DATABASE_URL` 时退出。

## 许可

AGPL-3.0-only

## 镜像

`Dockerfile` 只编 worker。入口默认 `command` 为 `worker`。
