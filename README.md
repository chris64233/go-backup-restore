# go-backup-restore

增量备份链的安全恢复与保留清理服务（Go 1.23，零第三方依赖）。

## 能力概览

| 能力 | 入口 | 说明 |
| --- | --- | --- |
| 快照登记 | `RegisterSnapshot` / `MarkSnapshotCompleted` / `MarkSnapshotFailed` | 不可变摘要 + 父快照 + 完成状态 |
| 恢复创建 | `CreateRestore` | 冻结执行计划（应用压缩替代）并获取目标环境租约 |
| 时间点恢复 | `CreatePointInTimeRestore` / `GetPointInTimeSelection` | 只给目标时间：选出不晚于该时间的最新已完成快照，解析完整/增量/压缩替代并冻结计划 |
| 步骤推进 | `DispatchNextStep` / `AckStep` | 沿计划有序执行，允许重试，回执匹配租约与执行版本 |
| 任务取消 | `CancelRestore` | 与执行并发只留下一个终态；取消后仍可查询计划与失败位置 |
| 进度查询 | `GetRestoreProgress` | 实际使用的备份链（含压缩溯源）、已完成步骤与失败位置 |
| 租约接管 | `TakeoverLease` | epoch 单调递增，旧租约回执一律失效 |
| 任务完成 | `CompleteRestore` | 写出且只写出一次 outbox 通知，重复调用幂等 |
| 链压缩 | `CreateCompaction` / `DispatchCompactionStep` / `AckCompactionStep` / `TakeoverCompactionLease` / `PublishCompaction` | 一段已完成链合成为新的完整快照，摘要校验通过后原子发布 |
| 保留清理 | `RunRetention` | 一致快照上决策，逐条记录原因并落库 |
| 备份链查询 | `GetBackupChain` / `GetEffectiveChain` / `ListDatasetSnapshots` / `GetRestoreTask` / `ListLeaseEvents` / `ListRetentionRuns` / `ListOutboxEvents` | 只读视图 |
| 压缩查询 | `GetCompactionTask` / `ListCompactionTasks` / `GetCompactionProgress` / `ListReplacements` / `ExplainSnapshotReferences` | 进度、替代关系与引用阻断原因 |

## 核心规则

### 1. 快照关系

- 快照记录 `Digest`（内容摘要，全库唯一）与 `ParentID`，一经登记不可变；
  状态只允许 `pending → completed | failed` 一次迁移，终态不可再变。
- 完整快照（`full`）不得声明父快照；增量快照（`incremental`）只能挂在
  **同一数据集、已完成** 的父快照之后。新节点必为叶子、父边只指向过去，
  关系天然无环；登记与链查询时仍做祖先遍历防御（断链/跨数据集/环均报 `conflict`）。
- 失败或进行中的快照不能作为恢复目标。

### 2. 恢复与租约（fencing）

- `CreateRestore` 在**单个事务**内完成三件事：校验目标快照已完成、
  解析并**冻结**执行计划、获取目标环境的恢复租约。
  同一目标环境同时只允许一个有效恢复（`pending`/`running`）。
- **执行计划不是“选最近一个文件”**：从目标快照沿原始父边回溯到完整快照，
  再应用已发布的压缩替代——一段原链被其压缩产物（新的完整快照）整段替换，
  支持链式压缩。冻结的每个计划节点都带溯源：`Source`（`original` /
  `compaction`）、产出它的 `CompactionTaskID`、以及被它折叠的全部原链快照
  （`ReplacedSnapshots`，链式压缩时为递归闭包）。
- **时间点恢复** `CreatePointInTimeRestore`：用户只给 `DatasetID` + `TargetTime`。
  - 系统在一致快照上选出**完成时间不晚于目标时间**的最新已完成快照
    （完成时间相同取 ID 最大者；`pending`/`failed` 不可见；没有满足条件的
    快照返回 `not_found`，绝不退而选择最近一个文件）。
  - **压缩产物不参与时间点候选**：它是对旧链的物理重写，发布时间可能晚于
    其后继增量，直接按时间选它会静默丢掉压缩段之后的增量。“用不用压缩产物”
    只在计划折叠阶段决定——前缀折叠为产物、后继增量照常保留。
  - 计划折叠遵守 **as-of 边界**：只有 `PublishedAt ≤ TargetTime` 的替代关系
    才参与这份历史计划；目标时间之后才发布的压缩不能改写过去。
  - 可用 `GetPointInTimeSelection` 只读预览会选中哪个快照，不冻结、不建任务。
- **计划冻结后不可更换**：此后并发链压缩发布或保留清理都不影响这份计划；
  计划引用的每个快照（计划节点 + 被折叠的原链）在任务结束前一律受
  `active_restore` 保护，保留清理不得回收。
- 步骤沿冻结计划**有序**派发；失败步骤可重试，每次派发递增该步骤的
  `ExecutionVersion`。回执必须同时匹配 **租约 ID + epoch** 与
  **步骤执行版本**，否则被拒绝且状态不变。
- **中断恢复**：任务、计划、每个步骤状态（含执行版本、尝试次数、失败详情）
  随每个事务原子落盘。进程重启后新执行方 `TakeoverLease` 接管：已成功的步骤
  保持成功、绝不重复应用，崩溃时在途（`running`）的步骤重置为 `failed` 并
  提升执行版本，派发从第一个未成功步骤继续；旧持有者的迟到回执因
  epoch/版本不匹配被挡下。
- **取消** `CancelRestore`：取消与派发/回执/完成跑在同一个可序列化存储上，
  提交后任务进入唯一终态 `canceled` 并释放租约；此后任何执行动作都被拒绝，
  已成功的任务不能再取消，重复取消幂等。取消不写完成 outbox，步骤状态原样保留。
- `GetRestoreProgress` 查询**实际使用的备份链**（含压缩任务来源与折叠的原链）、
  步骤计数、已完成步骤数与 `FirstFailed` 失败位置——取消或中断后据此定位与续跑。
- `TakeoverLease` 换发租约 ID 并递增 epoch，把在途步骤重置为可重试；
  旧租约持有者的迟到回执因 epoch 不匹配被挡下（`lease` 错误），
  无法干扰接管者。租约的获取/接管/释放全部记入审计日志。
- `CompleteRestore` 要求全部步骤成功；完成时写出**唯一一条** outbox 通知
  （存储层 `outbox_by_task` 唯一约束兜底），重复完成幂等返回同一事件。

### 3. 链压缩（增量链 -> 新完整快照）

- `CreateCompaction` 在**单个事务**内校验并冻结：起点（必须为完整快照）、
  终点、链中每个快照的摘要、以及当前**数据集版本**（每次登记/发布递增）。
  链必须连续（起点是终点沿父边的祖先）且全部已完成。
- **任务号幂等**：`IdempotencyKey` 相同的重复创建返回同一任务；
  键相同但链不同报 `conflict`，绝不悄悄复用。
- 执行与恢复任务同一套 fencing：步骤沿冻结链有序派发、失败可重试，
  回执必须匹配 **租约 ID + epoch** 与 **步骤执行版本**；
  `TakeoverCompactionLease` 换发租约后，旧持有者的回执一律失效。
- `PublishCompaction` 在**单个事务**内原子发布：
  内容摘要必须等于创建时由冻结链推导的 `ExpectedDigest`，
  不通过则任务保持可重试、**读者继续看到原链**；
  通过则一次性写入新的已完成完整快照 + 每个原快照的**替代关系**
  （`Replacement`）+ 任务终态。新快照在提交前不存在，
  读者不可能观察到半成品；重复发布幂等返回同一结果。
- **同一链的并发压缩最多一个生效**：发布时校验冻结链上没有任何节点
  已被其他压缩替代（存储层 `replacements` 唯一约束兜底），后到者报 `conflict`。
- 发布后**原链不立即删除**：`RunRetention` 只在快照不再被
  有效恢复任务（`active_restore`）、有效压缩任务（`active_compaction`）、
  保留策略或其他增量子快照引用时才逐个回收，
  被替代节点的回收原因记为 `replaced_by_compaction`。
  恢复创建与回收都运行在可序列化事务上：先冻结则清理让路，
  先回收则恢复创建得到明确的 `not_found`，绝不会得到断裂链。
- 查询：`GetCompactionProgress`（步骤进度）、`ListReplacements`（原快照 ->
  新完整快照映射）、`GetEffectiveChain`（应用替代后的新链视图，
  支持链式压缩）、`GetBackupChain`（保持原始回溯的旧链视图）、
  `ExplainSnapshotReferences`（引用阻断原因）。

### 4. 保留清理

- `RunRetention` 在**单个可序列化事务**内从一致快照作出全部决定，
  与快照登记、恢复创建、压缩发布并发时不会读到中间状态。
- 以下内容一律保留（原因写入决策记录）：
  - 仍被**有效恢复链**冻结的快照（`active_restore`）；
  - 仍被**有效压缩任务**冻结的原链快照（`active_compaction`）；
  - 命中**保留策略**的最近 N 个已完成快照及其全部祖先
    （`retention_policy` / `ancestor_of_retained_snapshot`）；
  - **未完成（pending）子快照**自身及其祖先链
    （`snapshot_in_progress` / `ancestor_of_incomplete_snapshot`）。
- 其余快照删除并记录原因：被压缩替代且不再被引用的原链节点
  `replaced_by_compaction`，过期完成快照 `expired_and_unreferenced`，
  失败快照 `failed_and_unreferenced`。每次运行落库一条 `RetentionRun`。

### 5. 持久化与错误分类

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

// 4b. 按时间点恢复：用户只给目标时间，系统选快照并冻结含压缩替代的计划
//     （压缩产物不参与候选，且只折叠目标时间之前已发布的替代）
pit, _ := svc.CreatePointInTimeRestore(ctx, backuprestore.CreatePointInTimeRestoreInput{
    DatasetID: "ds", TargetTime: time.Now().Add(-time.Hour),
    TargetEnvironment: "prod-b", Holder: "worker-1"})
// pit.SelectedSnapshotID 是实际选中的快照；pit.Chain 是冻结执行计划，
// 节点 Source=compaction 时带 CompactionTaskID 与 ReplacedSnapshots 溯源。
for {
    d, _ := svc.DispatchNextStep(ctx, pit.ID, pit.LeaseID, pit.LeaseEpoch)
    if d.Step == nil {
        break
    }
    // ... 执行该步骤；失败可重派，重启后从第一个未成功步骤继续 ...
    svc.AckStep(ctx, backuprestore.AckStepInput{
        TaskID: pit.ID, LeaseID: pit.LeaseID, Epoch: pit.LeaseEpoch,
        StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
        Success: true})
}
// 随时可查实际使用的备份链与失败位置；需要中止时 CancelRestore（终态唯一）
prog, _ := svc.GetRestoreProgress(ctx, pit.ID) // prog.Plan / prog.FirstFailed
// svc.CancelRestore(ctx, pit.ID, "ops", "no longer needed")
svc.CompleteRestore(ctx, pit.ID, pit.LeaseID, pit.LeaseEpoch)

// 5. 链压缩：full..inc -> 新的完整快照（任务号幂等）
ct, _ := svc.CreateCompaction(ctx, backuprestore.CreateCompactionInput{
    FromSnapshotID: full.ID, ToSnapshotID: inc.ID,
    Holder: "worker-1", IdempotencyKey: "compact-2026-09"})
for {
    d, _ := svc.DispatchCompactionStep(ctx, ct.ID, ct.LeaseID, ct.LeaseEpoch)
    if d.Step == nil {
        break
    }
    // ... 合并该快照的数据 ...
    svc.AckCompactionStep(ctx, backuprestore.AckStepInput{
        TaskID: ct.ID, LeaseID: ct.LeaseID, Epoch: ct.LeaseEpoch,
        StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
        Success: true})
}
// 内容摘要校验通过后原子发布；同一链的并发压缩最多一个生效
pub, _ := svc.PublishCompaction(ctx, backuprestore.PublishCompactionInput{
    TaskID: ct.ID, LeaseID: ct.LeaseID, Epoch: ct.LeaseEpoch,
    ContentDigest: ct.ExpectedDigest})
_ = pub.NewSnapshot // 新的完整快照；原链由保留清理在无引用时逐个回收

// 6. 保留清理：保留每数据集最近 3 个已完成快照及其依赖
svc.RunRetention(ctx, []backuprestore.RetentionRule{
    {DatasetID: "ds", KeepLatestCompleted: 3}})
```

## 测试

```sh
go test ./...          # 全部测试
go test -race ./...    # 含并发竞态检测
```

测试覆盖：登记规则（父快照状态/跨数据集/摘要唯一/终态迁移）、链查询、
恢复创建（冻结计划、环境唯一租约、不可恢复快照）、步骤有序派发与重试、
回执的租约/版本匹配、租约接管 fencing、outbox 恰好一次、
时间点选择（边界含等号、忽略 pending/failed、无满足快照返回 not_found、
压缩产物不顶替更新增量）、时间点计划与压缩链兼容（前缀折叠为压缩产物、
as-of 忽略目标时间之后发布的替代、链式压缩的递归折叠闭包）、
冻结计划不被并发压缩更换且计划引用数据在任务结束前不被回收、
取消的唯一终态（取消后派发/回执/接管/完成全被拒绝、重复取消幂等、不写 outbox）、
取消与完成/执行并发只留下一个终态、进度查询的实际链与失败位置、
中断恢复（FileStore 重启后接管续跑、已成功增量不重复应用、崩溃在途尝试失效）、
压缩创建（冻结链摘要与数据集版本、链连续性与完成态校验、任务号幂等）、
压缩步骤重试与接管 fencing、发布的摘要校验与原子性（发布前读者只见原链）、
同一链并发压缩唯一生效、发布幂等重放、发布后原链的引用阻断与逐个回收、
恢复与回收竞争不产生断裂链、链式压缩的新旧链查询、
保留清理的保护规则与原因记录、并发下“同一环境仅一个有效恢复”与
“清理不删被引用快照”、FileStore 重启恢复与事务回滚。

## 代码结构

```
domain.go          领域模型：快照、冻结计划（含压缩溯源）、恢复/压缩任务、步骤、
                   租约事件、outbox、保留决策、替代关系、恢复进度
errors.go          错误分类（ErrorCode）与 *Error
store.go           事务式 Store 接口、内存实现、原子落盘的 FileStore
service.go         业务规则：时间点选择/计划折叠/登记/恢复/取消/压缩/发布/接管/保留/查询
*_test.go          自动化测试（含 -race 并发用例）
```
