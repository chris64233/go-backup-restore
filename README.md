# go-backup-restore

增量备份链的安全恢复与保留清理服务（Go 1.23，零第三方依赖）。

## 能力概览

| 能力 | 入口 | 说明 |
| --- | --- | --- |
| 快照登记 | `RegisterSnapshot` / `MarkSnapshotCompleted` / `MarkSnapshotFailed` | 不可变摘要 + 父快照 + 完成状态 |
| 恢复创建 | `CreateRestore` | 冻结回溯链并获取目标环境租约 |
| 步骤推进 | `DispatchNextStep` / `AckStep` | 沿链有序执行，允许重试，回执匹配租约与执行版本 |
| 租约接管 | `TakeoverLease` | epoch 单调递增，旧租约回执一律失效 |
| 任务完成 | `CompleteRestore` | 写出且只写出一次 outbox 通知，重复调用幂等 |
| 保留清理 | `RunRetention` | 一致快照上决策，逐条记录原因并落库 |
| 备份链查询 | `GetBackupChain` / `ListDatasetSnapshots` / `GetRestoreTask` / `ListLeaseEvents` / `ListRetentionRuns` / `ListOutboxEvents` | 只读视图 |

## 核心规则

### 1. 快照关系

- 快照记录 `Digest`（内容摘要，全库唯一）与 `ParentID`，一经登记不可变；
  状态只允许 `pending → completed | failed` 一次迁移，终态不可再变。
- 完整快照（`full`）不得声明父快照；增量快照（`incremental`）只能挂在
  **同一数据集、已完成** 的父快照之后。新节点必为叶子、父边只指向过去，
  关系天然无环；登记与链查询时仍做祖先遍历防御（断链/跨数据集/环均报 `conflict`）。
- 失败或进行中的快照不能作为恢复目标。

### 2. 恢复与租约（fencing）

- `CreateRestore` 在**单个事务**内完成三件事：校验目标快照已完成、回溯并
  **冻结** 从目标到完整快照的链、获取目标环境的恢复租约。
  同一目标环境同时只允许一个有效恢复（`pending`/`running`）。
- 步骤沿冻结链**有序**派发；失败步骤可重试，每次派发递增该步骤的
  `ExecutionVersion`。回执必须同时匹配 **租约 ID + epoch** 与
  **步骤执行版本**，否则被拒绝且状态不变。
- `TakeoverLease` 换发租约 ID 并递增 epoch，把在途步骤重置为可重试；
  旧租约持有者的迟到回执因 epoch 不匹配被挡下（`lease` 错误），
  无法干扰接管者。租约的获取/接管/释放全部记入审计日志。
- `CompleteRestore` 要求全部步骤成功；完成时写出**唯一一条** outbox 通知
  （存储层 `outbox_by_task` 唯一约束兜底），重复完成幂等返回同一事件。

### 3. 保留清理

- `RunRetention` 在**单个可序列化事务**内从一致快照作出全部决定，
  与快照登记、恢复创建并发时不会读到中间状态。
- 以下内容一律保留（原因写入决策记录）：
  - 仍被**有效恢复链**冻结的快照（`active_restore`）；
  - 命中**保留策略**的最近 N 个已完成快照及其全部祖先
    （`retention_policy` / `ancestor_of_retained_snapshot`）；
  - **未完成（pending）子快照**自身及其祖先链
    （`snapshot_in_progress` / `ancestor_of_incomplete_snapshot`）。
- 其余快照删除并记录原因：过期完成快照 `expired_and_unreferenced`，
  失败快照 `failed_and_unreferenced`。每次运行落库一条 `RetentionRun`。

### 4. 持久化与错误分类

- 存储接口 `Store`（`Update`/`View`）保证事务串行、视图一致：
  - `NewMemoryStore()`：进程内实现，适合测试；
  - `NewFileStore(path)`：每次提交原子落盘（临时文件 + rename + fsync），
    重启后关系、状态、outbox、租约审计、保留记录全部恢复，ID 序列不回退。
- 错误统一为 `*Error`，用 `CodeOf(err)` 分类：
  `invalid_argument` / `not_found` / `already_exists` / `conflict` /
  `lease` / `unavailable`。

## 典型流程

```go
store, _ := backuprestore.NewFileStore("state.json")
svc := backuprestore.NewService(store)
ctx := context.Background()

// 1. 登记并完成快照链：full -> inc1 -> inc2
full, _ := svc.RegisterSnapshot(ctx, backuprestore.RegisterSnapshotInput{
    DatasetID: "ds", Kind: backuprestore.KindFull, Digest: "sha256:..."})
svc.MarkSnapshotCompleted(ctx, full.ID)
inc, _ := svc.RegisterSnapshot(ctx, backuprestore.RegisterSnapshotInput{
    DatasetID: "ds", Kind: backuprestore.KindIncremental, ParentID: full.ID, Digest: "sha256:..."})
svc.MarkSnapshotCompleted(ctx, inc.ID)

// 2. 创建恢复：冻结链 + 租约
task, _ := svc.CreateRestore(ctx, backuprestore.CreateRestoreInput{
    TargetSnapshotID: inc.ID, TargetEnvironment: "prod-a", Holder: "worker-1"})

// 3. 循环派发/回执，直到没有可派发步骤
for {
    d, _ := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
    if d.Step == nil {
        break // 全部完成或在途
    }
    // ... 执行实际数据拷贝 ...
    svc.AckStep(ctx, backuprestore.AckStepInput{
        TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
        StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
        Success: true})
}

// 4. 完成（写出唯一 outbox 通知）；执行者失联时由他人 TakeoverLease 接管
svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)

// 5. 保留清理：保留每数据集最近 3 个已完成快照及其依赖
svc.RunRetention(ctx, []backuprestore.RetentionRule{
    {DatasetID: "ds", KeepLatestCompleted: 3}})
```

## 测试

```sh
go test ./...          # 全部测试
go test -race ./...    # 含并发竞态检测
```

测试覆盖：登记规则（父快照状态/跨数据集/摘要唯一/终态迁移）、链查询、
恢复创建（冻结链、环境唯一租约、不可恢复快照）、步骤有序派发与重试、
回执的租约/版本匹配、租约接管 fencing、outbox 恰好一次、保留清理的
保护规则与原因记录、并发下“同一环境仅一个有效恢复”与“清理不删被引用快照”、
FileStore 重启恢复与事务回滚。

## 代码结构

```
domain.go   领域模型：快照、冻结链、恢复任务、步骤、租约事件、outbox、保留决策
errors.go   错误分类（ErrorCode）与 *Error
store.go    事务式 Store 接口、内存实现、原子落盘的 FileStore
service.go  业务规则：登记/恢复/回执/接管/保留/查询
*_test.go   自动化测试（含 -race 并发用例）
```
