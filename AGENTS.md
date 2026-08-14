# osspilot-tenant-worker

租户后台 worker。对照 OssPilot `legacy/`（v0.6.0）按切片用 Go 重写，不抄 Python。

表结构与 asynq 任务 JSON 跟 [osspilot-tenant-api](https://github.com/cyxc1124/osspilot-tenant-api) 对齐。改批量删除 payload 或 `object_records` 列，要同时改 API 仓和本仓，漂移了就对不齐入队。

## 提交

`<type>: <中文说明>`，可加范围：`feat(worker): ...`。必须保留 type 前缀，不要只写中文。

- `feat` 新功能
- `fix` 修缺陷
- `docs` 文档
- `style` 格式（不影响行为）
- `refactor` 重构
- `perf` 性能
- `test` 测试
- `build` 构建与依赖
- `ci` CI / 工作流
- `chore` 脚手架、杂项
- `revert` 回滚

```
chore: 初始化租户 worker 仓
```

## 实现

- Go 1.26，`pgx/v5`，asynq。不用 Gin / GORM。不跑 migrate。
- 一片一变，过 CI 再合。不要顺手做邻片。
- 暂时不写功能测试；只留能挡住编译的最小检查。
- 入口 `command` 默认 `worker`。
