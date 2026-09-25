# go-backup-restore

增量备份链的安全恢复与保留清理服务库。纯 Go 标准库实现，无外部依赖。

开发环境：Go 1.23.0。

```sh
go test ./...        # 运行全部自动化测试
go test -race ./...  # 带竞态检测
```

## 能力概览

| 能力 | 入口 | 说明 |
| --- | --- | --- |
| 快照登记 | `RegisterSnapshot` / `CompleteSnapshot` / `FailSnapshot` | 不可变摘要与父指针，完成状态单向迁移 |
| 恢复创建 | `CreateRestore` | 创建时冻结整条链并取得目标环境唯一租约 |
| 租约接管 | `TakeoverRestore` / `CancelRestore` | 栅栏令牌（fencing token）废弃旧租约 |
| 步骤回执 | `ReportStep` / `RetryStep` | 按序推进，回执校验租约、栅栏与执行版本 |
| 保留清理 | `SetRetentionPolicy` / `RunRetention` | 单事务一致决策，逐条记录保留/删除原因 |
| 链查询 | `GetChain` / `ListSnapshots` / `GetJob` / `ListCleanupRecords` / `ListOutbox` | 关系、状态、通知均可查询与持久化 |

## 核心模型与不变量

### 快照与备份链

- 快照分 `full`（完整，链根）与 `incremental`（增量）。
- 快照记录在登记时确定**不可变摘要**（digest）与**父指针**，此后不可修改；
  完成状态只允许单向迁移：`pending -> completed` 或 `pending -> failed`，
  失败/完成之间不可互转（接口幂等允许重复确认同一状态）。
- 增量快照只能挂在**同一数据集、已完成**的父快照之后；登记时沿父链回溯到
  完整快照并校验：
  - 整条关系**无环**（环上任何快照都无法恢复或继续挂载）；
  - 链上每一环都属于同一数据集；
  - 链必须能回溯到完整快照，且不存在失败/未完成环节。
- **失败快照不能用于恢复，也不能作为新增量的父。**

### 恢复任务与租约

- `CreateRestore` 在**同一个事务**内：
  1. 校验目标快照链（已完成、无环、同数据集）；
  2. 把从目标快照回溯到完整快照的链**冻结**（FrozenSnapshot，含当时摘要）；
  3. 取得目标环境的**恢复租约**。同一目标环境同一时刻至多一个有效恢复，
     并发创建由事务串行化保证只有一个成功（其余返回 `ErrLeaseConflict`）。
- 恢复步骤沿冻结链有序推进：每个步骤带单调的**执行版本**（execution
  version），回执必须同时匹配 **租约 ID、栅栏令牌、步骤序号与执行版本**。
  - 乱序回执 → `ErrStepOrder`；版本不符 → `ErrVersionMismatch`。
  - 步骤失败后不能被同版本回执“翻案”，必须 `RetryStep` 重试；重试会抬升
    执行版本，使旧执行的迟到成功回执失效。
  - 任务进入终态（成功/被接管/取消）后拒绝一切回执（`ErrJobFinished`）。
- **租约接管**：`TakeoverRestore` 将旧任务置为 `superseded`，删除旧租约并
  发放栅栏令牌更大的新租约。旧持有者的迟到回执（即使伪造新任务 ID）都会因
  租约 ID 或栅栏令牌不匹配被拒绝（`ErrStaleLease`），无法干扰接管者。
- 全部步骤成功时任务完成，在同一事务里向 **outbox 写出恰好一条通知**
  （`restore_succeeded`）。通知只追加一次，可用 `MarkOutboxDelivered`
  标记投递；任务完成同时释放环境租约。

### 保留清理

- `SetRetentionPolicy(dataset, keepLatest)` 配置“保留最近 N 个已完成快照”，
  被选中的头部快照的**全部祖先自动保留**（后代存活时祖先不能删）。
- `RunRetention` 的评估与删除在**单事务**内完成，因此与快照创建、恢复创建
  并发时，决策始终基于一致快照。以下内容一律不删除：
  - 仍在运行的恢复任务**冻结链**上的快照（`active-restore-chain`）；
  - 保留策略头部及其祖先（`retention-policy`）；
  - 未完成快照自身（`snapshot-incomplete`）；
  - 未完成子快照沿祖先链引用的所有快照（`incomplete-child-reference`）。
- 可删除的只有：失败快照（`failed-snapshot`，记录失败原因）与策略过期的已
  完成快照（`expired-by-policy`）。未配置策略时采取安全默认，全部保留
  （`no-retention-policy`）。
- 每个被评估快照都追加一条 `CleanupRecord`（决定 + 原因 + 细节），只追加、
  不修改，可通过 `ListCleanupRecords` 审计。删除是软删除（打标记），活动查询
  与恢复链默认不再可见。

## 持久化

`Store` 是唯一的状态边界，`Update` 提供可串行化的读-改-写事务（回调失败
整体回滚），`View` 提供只读访问：

- `NewMemoryStore()`：内存实现，适合测试与嵌入；
- `NewFileStore(path)`：状态以 JSON 原子落盘（写临时文件后 rename），
  进程重启后快照关系、恢复任务/租约、步骤执行版本、outbox、清理记录
  完整恢复。

替换为真实数据库时只需实现 `Store` 接口（单连接串行事务或
SELECT ... FOR UPDATE / SERIALIZABLE 隔离级别）。

## 错误分类

所有公开 API 的错误都可用 `errors.Is` 分类：

| 哨兵错误 | 含义 / 调用方处理建议 |
| --- | --- |
| `ErrInvalidInput` | 参数缺失或非法（调用方 bug） |
| `ErrNotFound` | 快照/任务/消息不存在或已删除 |
| `ErrInvalidTransition` | 状态机非法迁移（如完成后再置失败） |
| `ErrSnapshotNotCompleted` | 父或恢复目标仍在 pending |
| `ErrSnapshotFailed` | 父或恢复目标已失败 |
| `ErrChainCycle` | 父链存在环 |
| `ErrCrossDataset` | 增量快照跨数据集挂载 |
| `ErrDigestConflict` | 以不同摘要重复确认同一快照 |
| `ErrLeaseConflict` | 目标环境已有有效恢复 |
| `ErrStaleLease` | 回执租约/栅栏令牌过期（旧持有者迟到回执） |
| `ErrVersionMismatch` | 步骤执行版本不匹配（重试后旧回执） |
| `ErrStepOrder` | 回执的步骤不是当前待执行步骤 |
| `ErrJobFinished` | 任务已是终态，不再接受回执 |

## 使用示例

```go
store, _ := backuprestore.NewFileStore("data/state.json")
svc := backuprestore.NewService(store)
ctx := context.Background()

// 1. 建立链：full -> inc1
full, _ := svc.RegisterSnapshot(ctx, backuprestore.RegisterSnapshotInput{
    DatasetID: "orders", Kind: backuprestore.KindFull, Digest: "sha256:aaa",
})
svc.CompleteSnapshot(ctx, full.ID, "sha256:aaa")

inc1, _ := svc.RegisterSnapshot(ctx, backuprestore.RegisterSnapshotInput{
    DatasetID: "orders", Kind: backuprestore.KindIncremental,
    ParentID: full.ID, Digest: "sha256:bbb",
})
svc.CompleteSnapshot(ctx, inc1.ID, "sha256:bbb")

// 2. 创建恢复（冻结链 + 取租约），按步骤回执
job, _ := svc.CreateRestore(ctx, "env-prod", inc1.ID)
for _, step := range job.Steps {
    // worker 执行恢复动作，然后回执（必须带租约与执行版本）
    err := svc.ReportStep(ctx, backuprestore.StepReceipt{
        JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
        StepIndex: step.Index, ExecutionVersion: step.ExecutionVersion,
        Success: true,
    })
    // 失败时：svc.RetryStep(...) 抬升版本后用新版本重执
    _ = err
}

// 3. worker 失联时由协调者接管（旧租约立即失效）
newJob, _ := svc.TakeoverRestore(ctx, "env-prod", inc1.ID)
_ = newJob

// 4. 保留清理（最近 7 个已完成快照及其祖先，其余带原因删除）
svc.SetRetentionPolicy(ctx, "orders", 7)
report, _ := svc.RunRetention(ctx, "orders")
for _, d := range report.Decisions {
    log.Printf("%s -> %s (%s)", d.SnapshotID, d.Decision, d.Reason)
}
```

## 测试覆盖

`service_test.go` 覆盖：

- 快照登记：输入校验、跨数据集拒绝、pending/failed 父拒绝、状态机迁移、
  摘要一致性；注入环存储验证无环校验真实生效；
- 恢复：链冻结顺序、每环境唯一租约（32 并发仅 1 成功）、步骤乱序/版本不符
  拒绝、失败重试抬升版本、终态拒收回执、outbox 恰好一条、租约释放；
- 接管：旧任务 `superseded`、栅栏令牌单调、旧租约迟到回执与伪造新任务 ID
  的回执均被拒绝；
- 保留清理：最近 N 个及共享祖先保护、有效恢复链保护、未完成子快照祖先保护、
  失败快照回收、无策略安全默认、审计记录只追加、清理与恢复创建 20 轮并发后
  “运行中恢复引用的快照必存活”不变量校验；
- 持久化：FileStore 重启后关系/任务/outbox 完整恢复；失败事务不落盘不脏内存。
