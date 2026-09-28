# go-backup-restore

增量备份链的安全恢复与保留清理服务（Go 1.23，零第三方依赖）。

## 能力概览

| 能力 | 入口 | 说明 |
| --- | --- | --- |
| 快照登记 | `RegisterSnapshot` / `MarkSnapshotCompleted` / `MarkSnapshotFailed` | 不可变摘要 + 父快照 + 完成状态 |
| 恢复创建 | `CreateRestore` | 指定快照，或给出 `DatasetID + TargetTime` 按时间点创建；冻结逻辑链与物理执行链并获取目标环境租约 |
| 步骤推进 | `DispatchNextStep` / `AckStep` | 沿冻结计划有序执行，允许重试，回执匹配租约与执行版本 |
| 取消恢复 | `CancelRestore` | 取消与执行并发只留一个终态；换发租约 fencing 在途执行者 |
| 中断查询 | `GetFailureLocation` / `GetRestoreTask` | 查询实际使用的备份链（逻辑链/物理链）与失败步骤位置 |
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
- **取消** `CancelRestore` 在单个事务内把任务置为终态 `cancelled`，同时
  换发租约（epoch+1、租约 ID 更换），在途步骤回到 failed。取消与执行并发时
  全局只会留下一个状态：取消先落库则执行者后续的派发/回执/完成全部被
  fencing（`lease`/`conflict`）；执行先落库（任务已 `succeeded`）则取消
  返回 `conflict`。取消的任务不写完成通知，取消后同一环境可新建恢复。

### 3. 按时间点恢复（PITR）

调用方只给 `DatasetID + TargetTime`，系统在 `CreateRestore` 的**单个事务**
内完成解析并冻结一份**可重复执行**的恢复计划，而不是“挑最近的一个文件”：

- **时间点选择**：在数据集的已完成快照中选 `CreatedAt <= TargetTime` 的
  最近一个作为**逻辑目标**（同一时刻以更大 ID 决胜）；`pending`/`failed`
  快照与压缩产物不构成可选时间点。窗口内没有任何已完成快照返回 `not_found`。
- **解析压缩替代**：先回溯逻辑目标的原始父边得到**逻辑链**
  （`Task.LogicalChain`），再在其上应用已发布的压缩替代，得到**物理执行链**
  （`Task.Chain`，步骤即按它生成）。折叠规则与 `GetEffectiveChain` 一致：
  压缩任务的冻结段必须与当前链连续逐 ID 精确匹配，支持链式压缩；
  物理节点用 `OriginSnapshotID`（覆盖到的逻辑链末端）与
  `ViaCompactionTaskID`（来源压缩任务）标注，`GetRestoreTask` 可直接回答
  “实际用了哪条备份链”。
- **兼容已压缩/已回收的链**：保留清理回收快照时只把记录移入归档表
  （常规查询不可见，时间戳/摘要/父边等不可变元数据保留），替代关系本身
  永不删除。因此即使原链实体已全部回收，段尾时间点仍能从“归档 + 替代关系”
  复原计划并落到替代完整快照执行；目标时间落在已回收压缩段的**中间**
  （替代完整快照只能表达段尾状态）则明确报 `conflict`（超出可恢复窗口），
  绝不生成断裂或语义错误的计划。
- **计划冻结**：逻辑链、物理链、目标时间、目标快照在创建事务内一并落库。
  之后并发的链压缩发布**不能更换**这份计划；`RunRetention` 通过
  `active_restore` 保护物理链上的每个快照，直到任务进入终态（成功或取消）。
  压缩产物在内容上等价于被压缩段（发布时 `ExpectedDigest` 校验），
  所以计划只在创建时解析一次，之后的新压缩对它无影响。
- **中断恢复**：每步的派发/回执状态、尝试次数、执行版本与
  `FailureStepIndex/FailureDetail` 都随事务持久化。进程重启后
  `GetRestoreTask` / `GetFailureLocation` 可查到失败位置；已 `succeeded`
  的步骤不会被重复派发（`DispatchNextStep` 只取第一个未成功步骤）。
  进程在步骤执行中崩溃（步骤停在 `running`、结果未知）时，由监督者
  `TakeoverLease` 接管：epoch+1 fencing 旧进程的迟到回执，在途步骤回到
  failed 并从安全位置重放。

### 4. 链压缩（增量链 -> 新完整快照）

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

### 5. 保留清理

- `RunRetention` 在**单个可序列化事务**内从一致快照作出全部决定，
  与快照登记、按时间点恢复创建、压缩发布并发时不会读到中间状态。
- 以下内容一律保留（原因写入决策记录）：
  - 仍被**有效恢复链**冻结的物理快照（`active_restore`，含时间点恢复
    计划折叠出的替代完整快照）；
  - 仍被**有效压缩任务**冻结的原链快照（`active_compaction`）；
  - 命中**保留策略**的最近 N 个已完成快照及其全部祖先
    （`retention_policy` / `ancestor_of_retained_snapshot`）；
  - **未完成（pending）子快照**自身及其祖先链
    （`snapshot_in_progress` / `ancestor_of_incomplete_snapshot`）。
- 其余快照的物理实体回收并记录原因：被压缩替代且不再被引用的原链节点
  `replaced_by_compaction`，过期完成快照 `expired_and_unreferenced`，
  失败快照 `failed_and_unreferenced`。每次运行落库一条 `RetentionRun`。
  被回收快照的不可变记录（ID/父边/摘要/时间戳）移入归档表，供之后的
  按时间点恢复复原计划使用；常规快照查询看不到归档记录。

### 6. 持久化与错误分类

- 存储接口 `Store`（`Update`/`View`）保证事务串行、视图一致：
  - `NewMemoryStore()`：进程内实现，适合测试；
  - `NewFileStore(path)`：每次提交原子落盘（临时文件 + rename + fsync），
    重启后关系、状态（含恢复计划/步骤/失败位置/取消状态）、outbox、
    租约审计、保留记录与归档快照全部恢复，ID 序列不回退。
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

// 7. 按时间点恢复（与上面的快照恢复二选一）：只给目标时间，
//    系统冻结逻辑链与物理执行链，兼容已发布压缩与已回收原链。
task, _ = svc.CreateRestore(ctx, backuprestore.CreateRestoreInput{
    DatasetID: "ds", TargetTime: time.Date(2026, 9, 25, 2, 0, 0, 0, time.UTC),
    TargetEnvironment: "dr-env", Holder: "worker-1"})
// task.LogicalChain：选择时刻的原始链 full -> inc -> ...
// task.Chain：实际执行链（已把匹配的压缩段折叠为替代完整快照）
for {
    d, _ := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
    if d.Step == nil {
        break
    }
    // ... 应用该物理步骤；失败可重试，重启后从失败位置继续，不重复应用 ...
    svc.AckStep(ctx, backuprestore.AckStepInput{
        TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
        StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
        Success: true})
}
svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)

// 需要中止时取消（与执行并发只留一个状态）；之后可查询实际链与失败位置。
// svc.CancelRestore(ctx, backuprestore.CancelRestoreInput{
//     TaskID: task.ID, Reason: "rollback drill cancelled", By: "operator"})
// loc, _ := svc.GetFailureLocation(ctx, task.ID)
// rt, _ := svc.GetRestoreTask(ctx, task.ID) // rt.Chain / rt.LogicalChain
```

## 测试

```sh
go test ./...          # 全部测试
go test -race ./...    # 含并发竞态检测
```

测试覆盖：登记规则（父快照状态/跨数据集/摘要唯一/终态迁移）、链查询、
恢复创建（冻结链、环境唯一租约、不可恢复快照）、步骤有序派发与重试、
回执的租约/版本匹配、租约接管 fencing、outbox 恰好一次、
**按时间点恢复**（时间点选择边界、pending/压缩产物不参选、压缩段折叠、
链式压缩、回收后从归档+替代关系复原计划、压缩段中间时间点报冲突、
计划冻结后并发压缩不能更换且物理链受 `active_restore` 保护、
与保留清理并发不产生断裂计划、
取消 fencing 在途执行者、取消与完成并发 24 轮只留一个终态且通知互斥、
失败位置记录/清除/接管后可见、FileStore 重启后跳过已完成步骤继续、
崩溃于 running 步骤后接管重放且旧回执失效）、
压缩创建（冻结链摘要与数据集版本、链连续性与完成态校验、任务号幂等）、
压缩步骤重试与接管 fencing、发布的摘要校验与原子性（发布前读者只见原链）、
同一链并发压缩唯一生效、发布幂等重放、发布后原链的引用阻断与逐个回收、
恢复与回收竞争不产生断裂链、链式压缩的新旧链查询、
保留清理的保护规则与原因记录、并发下“同一环境仅一个有效恢复”与
“清理不删被引用快照”、FileStore 重启恢复与事务回滚。

## 代码结构

```
domain.go          领域模型：快照、冻结链（含来源标注）、恢复/压缩任务、
                   步骤、失败位置、取消状态、租约事件、outbox、替代关系
errors.go          错误分类（ErrorCode）与 *Error
store.go           事务式 Store 接口、内存实现、原子落盘的 FileStore、
                   回收快照归档表（时间点计划解析专用）
service.go         业务规则：登记/快照恢复/按时间点恢复/取消/失败位置/
                   压缩/发布/接管/保留/查询
*_test.go          自动化测试（含 -race 并发用例）：pitr_test.go 覆盖
                   时间点选择、压缩冲突、计划冻结、取消竞争与中断恢复
```
