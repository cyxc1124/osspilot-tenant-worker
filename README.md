# osspilot-tenant-worker

OssPilot 租户后台 worker（Go）。消费 [osspilot-tenant-api](https://github.com/cyxc1124/osspilot-tenant-api) 入队的 asynq 任务，并定时扫清单 / 回收站 / 版本 / 分片 / 请求统计。

表结构与任务 JSON 跟租户 API 仓对齐；本仓是拷贝，之后会漂移。改 payload 或列要两边一起改。

## 本地

不跑迁移（迁移只在 API 仓）：

```bash
export DATABASE_URL=postgres://osspilot:osspilot@127.0.0.1:5432/osspilot_tenant?sslmode=disable
export REDIS_URL=redis://127.0.0.1:6379/0
go test ./...
go run ./cmd/worker
```

未设 `REDIS_URL` / `DATABASE_URL` 时退出。未设 S3 时进程仍监听，清单/清理/批量任务跳过。

## 许可

AGPL-3.0-only

## 镜像

`Dockerfile` 只编 worker。入口默认 `command` 为 `worker`。
