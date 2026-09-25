package backuprestore

import (
	"context"
	"sort"
	"time"
)

// Service 在事务式 Store 之上提供备份恢复领域操作。
// 所有涉及多对象的判定都在单个可序列化事务内完成，
// 因此快照登记、恢复创建与保留清理并发时彼此不会读到中间状态。
type Service struct {
	store Store
	now   func() time.Time
}

// NewService 创建服务。store 通常为 NewFileStore（持久化）或 NewMemoryStore（测试）。
func NewService(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

// WithClock 用给定时钟替换默认的 time.Now，主要用于测试。
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

func (s *Service) timeNow() time.Time { return s.now().UTC() }

// ---------------------------------------------------------------------------
// 快照登记
// ---------------------------------------------------------------------------

// RegisterSnapshotInput 是登记快照的入参。增量快照必须提供 ParentID。
type RegisterSnapshotInput struct {
	DatasetID string
	Kind      SnapshotKind
	ParentID  string
	Digest    string // 不可变内容摘要，全库唯一
}

// RegisterSnapshot 登记一个 pending 快照。
// 完整快照不能有父快照；增量快照只能挂在同一数据集已完成的父快照之后。
// 新登记的节点必为叶子，父边只指向过去的已完成节点，关系天然无环；
// 这里仍做一次祖先遍历以确保链最终落在完整快照上。
func (s *Service) RegisterSnapshot(ctx context.Context, in RegisterSnapshotInput) (*Snapshot, error) {
	if in.DatasetID == "" {
		return nil, classified(ErrCodeInvalidArgument, "dataset id is required")
	}
	if in.Digest == "" {
		return nil, classified(ErrCodeInvalidArgument, "digest is required")
	}
	switch in.Kind {
	case KindFull:
		if in.ParentID != "" {
			return nil, classified(ErrCodeInvalidArgument, "full snapshot must not declare a parent")
		}
	case KindIncremental:
		if in.ParentID == "" {
			return nil, classified(ErrCodeInvalidArgument, "incremental snapshot requires a parent")
		}
	default:
		return nil, classified(ErrCodeInvalidArgument, "unknown snapshot kind %q", in.Kind)
	}

	var out *Snapshot
	err := s.store.Update(func(tx *Tx) error {
		for _, ex := range tx.ListSnapshots() {
			if ex.Digest == in.Digest {
				return classified(ErrCodeAlreadyExists, "snapshot with digest %s already exists: %s", in.Digest, ex.ID)
			}
		}
		if in.Kind == KindIncremental {
			parent, ok := tx.GetSnapshot(in.ParentID)
			if !ok {
				return classified(ErrCodeNotFound, "parent snapshot %s not found", in.ParentID)
			}
			if parent.DatasetID != in.DatasetID {
				return classified(ErrCodeInvalidArgument, "parent snapshot %s belongs to dataset %s, want %s",
					parent.ID, parent.DatasetID, in.DatasetID)
			}
			if parent.Status != StatusCompleted {
				return classified(ErrCodeConflict, "parent snapshot %s is %s, only completed ancestors can be extended",
					parent.ID, parent.Status)
			}
			// 防御性校验：祖先链存在、无环且以完整快照收尾。
			chain, err := walkChain(tx, parent.ID)
			if err != nil {
				return err
			}
			if root := chain[0]; root.Kind != KindFull {
				return classified(ErrCodeConflict, "ancestry of %s does not start at a full snapshot", parent.ID)
			}
		}
		snap := Snapshot{
			ID:        tx.NewID("snap"),
			DatasetID: in.DatasetID,
			Kind:      in.Kind,
			ParentID:  in.ParentID,
			Digest:    in.Digest,
			Status:    StatusPending,
			CreatedAt: s.timeNow(),
		}
		tx.PutSnapshot(snap)
		out = cloneSnapshot(&snap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MarkSnapshotCompleted 将 pending 快照标记为已完成。终态只能迁移一次。
func (s *Service) MarkSnapshotCompleted(ctx context.Context, snapshotID string) (*Snapshot, error) {
	return s.transitionSnapshot(snapshotID, StatusCompleted, "")
}

// MarkSnapshotFailed 将 pending 快照标记为失败并记录原因。失败快照不能用于恢复。
func (s *Service) MarkSnapshotFailed(ctx context.Context, snapshotID, reason string) (*Snapshot, error) {
	if reason == "" {
		return nil, classified(ErrCodeInvalidArgument, "failure reason is required")
	}
	return s.transitionSnapshot(snapshotID, StatusFailed, reason)
}

func (s *Service) transitionSnapshot(id string, target SnapshotStatus, reason string) (*Snapshot, error) {
	var out *Snapshot
	err := s.store.Update(func(tx *Tx) error {
		snap, ok := tx.GetSnapshot(id)
		if !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", id)
		}
		if snap.Status != StatusPending {
			return classified(ErrCodeConflict, "snapshot %s is already %s", id, snap.Status)
		}
		snap.Status = target
		if target == StatusFailed {
			snap.FailureReason = reason
		}
		now := s.timeNow()
		snap.CompletedAt = &now
		tx.PutSnapshot(*snap)
		out = cloneSnapshot(snap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetSnapshot 查询单个快照。
func (s *Service) GetSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	var out *Snapshot
	err := s.store.View(func(tx *Tx) error {
		snap, ok := tx.GetSnapshot(id)
		if !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", id)
		}
		out = cloneSnapshot(snap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListDatasetSnapshots 按创建顺序列出数据集的全部快照。
func (s *Service) ListDatasetSnapshots(ctx context.Context, datasetID string) ([]*Snapshot, error) {
	var out []*Snapshot
	err := s.store.View(func(tx *Tx) error {
		for _, snap := range tx.ListSnapshotsByDataset(datasetID) {
			out = append(out, cloneSnapshot(snap))
		}
		return nil
	})
	return out, err
}

// walkChain 从 target 沿父边回溯，返回完整快照 -> 目标快照的有序切片。
// 父边缺失、跨数据集或出现环都属于数据完整性冲突。
func walkChain(tx *Tx, targetID string) ([]*Snapshot, error) {
	rev := make([]*Snapshot, 0)
	visited := map[string]bool{}
	curID := targetID
	datasetID := ""
	for {
		if visited[curID] {
			return nil, classified(ErrCodeConflict, "cycle detected in snapshot ancestry at %s", curID)
		}
		visited[curID] = true
		cur, ok := tx.GetSnapshot(curID)
		if !ok {
			return nil, classified(ErrCodeConflict, "snapshot chain is broken: %s is missing", curID)
		}
		if datasetID == "" {
			datasetID = cur.DatasetID
		} else if cur.DatasetID != datasetID {
			return nil, classified(ErrCodeConflict, "snapshot %s belongs to another dataset", cur.ID)
		}
		rev = append(rev, cur)
		if cur.Kind == KindFull {
			if cur.ParentID != "" {
				return nil, classified(ErrCodeConflict, "full snapshot %s must not have a parent", cur.ID)
			}
			break
		}
		if cur.ParentID == "" {
			return nil, classified(ErrCodeConflict, "incremental snapshot %s has no parent", cur.ID)
		}
		curID = cur.ParentID
	}
	out := make([]*Snapshot, len(rev))
	for i, sn := range rev {
		out[len(rev)-1-i] = sn
	}
	return out, nil
}

// GetBackupChain 返回目标快照回溯到完整快照的链（完整快照在前），用于备份链查询。
func (s *Service) GetBackupChain(ctx context.Context, targetSnapshotID string) ([]*Snapshot, error) {
	var out []*Snapshot
	err := s.store.View(func(tx *Tx) error {
		if _, ok := tx.GetSnapshot(targetSnapshotID); !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", targetSnapshotID)
		}
		chain, err := walkChain(tx, targetSnapshotID)
		if err != nil {
			return err
		}
		for _, sn := range chain {
			out = append(out, cloneSnapshot(sn))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 恢复创建：冻结链 + 租约
// ---------------------------------------------------------------------------

// CreateRestoreInput 创建恢复任务的入参。
type CreateRestoreInput struct {
	TargetSnapshotID  string
	TargetEnvironment string
	Holder            string // 申请租约的执行方标识
}

// CreateRestore 在单个事务内：
//  1. 校验目标快照已完成（失败/进行中均不可恢复），并回溯构造到完整快照的链；
//  2. 冻结链（此后保留清理不得删除链上任何快照）；
//  3. 取得目标环境的恢复租约（同一环境同时只允许一个有效恢复）。
func (s *Service) CreateRestore(ctx context.Context, in CreateRestoreInput) (*RestoreTask, error) {
	if in.TargetSnapshotID == "" || in.TargetEnvironment == "" || in.Holder == "" {
		return nil, classified(ErrCodeInvalidArgument, "target snapshot id, target environment and holder are required")
	}
	var out *RestoreTask
	err := s.store.Update(func(tx *Tx) error {
		target, ok := tx.GetSnapshot(in.TargetSnapshotID)
		if !ok {
			return classified(ErrCodeNotFound, "target snapshot %s not found", in.TargetSnapshotID)
		}
		switch target.Status {
		case StatusFailed:
			return classified(ErrCodeConflict, "cannot restore from failed snapshot %s", target.ID)
		case StatusPending:
			return classified(ErrCodeConflict, "snapshot %s is not completed yet", target.ID)
		}
		chainSnap, err := walkChain(tx, target.ID)
		if err != nil {
			return err
		}
		for _, sn := range chainSnap {
			if sn.Status != StatusCompleted {
				return classified(ErrCodeConflict, "snapshot %s on chain is %s, only completed chains can be restored",
					sn.ID, sn.Status)
			}
		}
		if existing, ok := tx.FindActiveTaskByEnv(in.TargetEnvironment); ok {
			return classified(ErrCodeConflict, "target environment %s already has active restore %s (lease %s epoch %d)",
				in.TargetEnvironment, existing.ID, existing.LeaseID, existing.LeaseEpoch)
		}

		now := s.timeNow()
		frozen := make([]FrozenSnapshot, len(chainSnap))
		steps := make([]RestoreStep, len(chainSnap))
		for i, sn := range chainSnap {
			frozen[i] = FrozenSnapshot{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Kind: sn.Kind}
			steps[i] = RestoreStep{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Status: StepPending}
		}
		leaseID := tx.NewID("lease")
		task := RestoreTask{
			ID:                tx.NewID("task"),
			DatasetID:         target.DatasetID,
			TargetEnvironment: in.TargetEnvironment,
			TargetSnapshotID:  target.ID,
			Chain:             frozen,
			Steps:             steps,
			Status:            TaskPending,
			LeaseID:           leaseID,
			LeaseEpoch:        1,
			LeaseHolder:       in.Holder,
			CreatedAt:         now,
		}
		tx.PutTask(task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: leaseID, Epoch: 1,
			Holder: in.Holder, Action: LeaseAcquired,
		})
		out = cloneTask(&task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetRestoreTask 查询恢复任务（含冻结链、步骤与当前租约）。
func (s *Service) GetRestoreTask(ctx context.Context, taskID string) (*RestoreTask, error) {
	var out *RestoreTask
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		out = cloneTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListRestoreTasks 按创建顺序列出全部恢复任务。
func (s *Service) ListRestoreTasks(ctx context.Context) ([]*RestoreTask, error) {
	var out []*RestoreTask
	err := s.store.View(func(tx *Tx) error {
		for _, t := range tx.ListTasks() {
			out = append(out, cloneTask(t))
		}
		return nil
	})
	return out, err
}

// requireLease 在事务内校验任务有效且租约 ID + epoch 完全匹配。
// 旧租约持有者（含被接管后仍持有旧 epoch 的执行者）一律被挡下。
func requireLease(task *RestoreTask, leaseID string, epoch int64) error {
	if !task.Active() {
		return classified(ErrCodeConflict, "restore task %s is %s", task.ID, task.Status)
	}
	if task.LeaseID != leaseID || task.LeaseEpoch != epoch {
		return classified(ErrCodeLease, "lease mismatch for task %s: current=%s epoch=%d, got=%s epoch=%d",
			task.ID, task.LeaseID, task.LeaseEpoch, leaseID, epoch)
	}
	return nil
}

// Dispatch 是一次步骤派发的结果。Step 为 nil 表示当前没有可派发的步骤
// （上一步仍在执行，或全部完成，应调用 CompleteRestore）。
type Dispatch struct {
	Task *RestoreTask
	Step *RestoreStep
}

// DispatchNextStep 沿冻结链有序派发下一个步骤：只派发第一个尚未成功的步骤，
// 且它当前不能处于 running。每次派发递增该步骤的 ExecutionVersion，
// 回执必须携带此版本，重复/迟到的旧回执会被拒绝。
func (s *Service) DispatchNextStep(ctx context.Context, taskID, leaseID string, epoch int64) (*Dispatch, error) {
	res := &Dispatch{}
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		if err := requireLease(task, leaseID, epoch); err != nil {
			return err
		}
		now := s.timeNow()
		for i := range task.Steps {
			step := &task.Steps[i]
			if step.Status == StepSucceeded {
				continue
			}
			if step.Status == StepRunning {
				// 已有在途尝试；等它回执，或先接管租约再重新派发。
				res.Task = cloneTask(task)
				return nil
			}
			// 有序推进：走到这里说明前面的步骤全部成功。
			step.Status = StepRunning
			step.ExecutionVersion++
			step.Attempts++
			if step.StartedAt == nil {
				t := now
				step.StartedAt = &t
			}
			t2 := now
			step.UpdatedAt = &t2
			if task.Status == TaskPending {
				task.Status = TaskRunning
				t3 := now
				task.StartedAt = &t3
			}
			tx.PutTask(*task)
			res.Task = cloneTask(task)
			cp := *step
			res.Step = &cp
			return nil
		}
		// 全部步骤成功，等待 CompleteRestore。
		res.Task = cloneTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// AckStepInput 是步骤回执。ExecutionVersion 必须等于派发时拿到的版本。
type AckStepInput struct {
	TaskID           string
	LeaseID          string
	Epoch            int64
	StepIndex        int
	ExecutionVersion int32
	Success          bool
	Detail           string
}

// AckStep 处理一个步骤回执：
//   - 租约 ID/epoch 不匹配（旧租约的迟到回执、接管后的旧执行者）→ ErrCodeLease，状态不动；
//   - 步骤执行版本不匹配（重试后旧尝试的迟到/重复回执）→ ErrCodeConflict，状态不动；
//   - 成功则步骤完成，失败则回到 failed 等待按序重试。
func (s *Service) AckStep(ctx context.Context, in AckStepInput) (*RestoreStep, error) {
	var out *RestoreStep
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(in.TaskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", in.TaskID)
		}
		if err := requireLease(task, in.LeaseID, in.Epoch); err != nil {
			return err
		}
		if in.StepIndex < 0 || in.StepIndex >= len(task.Steps) {
			return classified(ErrCodeInvalidArgument, "step index %d out of range (chain length %d)",
				in.StepIndex, len(task.Steps))
		}
		step := &task.Steps[in.StepIndex]
		if step.ExecutionVersion != in.ExecutionVersion {
			return classified(ErrCodeConflict,
				"stale receipt for task %s step %d: current execution version %d, receipt %d",
				task.ID, step.Index, step.ExecutionVersion, in.ExecutionVersion)
		}
		if step.Status != StepRunning {
			return classified(ErrCodeConflict, "task %s step %d is not running (status=%s)",
				task.ID, step.Index, step.Status)
		}
		now := s.timeNow()
		if in.Success {
			if in.StepIndex > 0 && task.Steps[in.StepIndex-1].Status != StepSucceeded {
				return classified(ErrCodeConflict, "cannot succeed step %d before predecessor", in.StepIndex)
			}
			step.Status = StepSucceeded
		} else {
			step.Status = StepFailed
		}
		step.LastDetail = in.Detail
		t := now
		step.UpdatedAt = &t
		tx.PutTask(*task)
		cp := *step
		out = &cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CompleteRestoreResult 返回终态任务与其唯一的 outbox 通知。
type CompleteRestoreResult struct {
	Task  *RestoreTask
	Event *OutboxEvent
}

// CompleteRestore 在所有步骤成功后结束任务并写出且只写出一次完成通知。
// 对已经成功的任务重复调用是幂等的：返回既有任务与既有通知，不再写 outbox。
func (s *Service) CompleteRestore(ctx context.Context, taskID, leaseID string, epoch int64) (*CompleteRestoreResult, error) {
	res := &CompleteRestoreResult{}
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		if !task.Active() {
			// 幂等：任务已完成，复用已有通知，绝不产生第二条。
			event, _ := tx.GetOutboxByTask(task.ID)
			res.Task = cloneTask(task)
			if event != nil {
				cp := *event
				res.Event = &cp
			}
			return nil
		}
		if err := requireLease(task, leaseID, epoch); err != nil {
			return err
		}
		for _, step := range task.Steps {
			if step.Status != StepSucceeded {
				return classified(ErrCodeConflict, "task %s step %d is %s, cannot complete",
					task.ID, step.Index, step.Status)
			}
		}
		now := s.timeNow()
		task.Status = TaskSucceeded
		task.CompletedAt = &now
		tx.PutTask(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: task.LeaseHolder, Action: LeaseReleased, Reason: "restore completed",
		})
		event := OutboxEvent{
			ID: tx.NewID("evt"), TaskID: task.ID, Type: OutboxRestoreCompleted,
			DatasetID: task.DatasetID, TargetEnvironment: task.TargetEnvironment,
			TargetSnapshotID: task.TargetSnapshotID, CreatedAt: now,
		}
		if err := tx.AddOutboxEvent(event); err != nil {
			return err
		}
		res.Task = cloneTask(task)
		res.Event = &event
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// TakeoverLease 接管一个有效恢复任务的租约：epoch 单调递增并换发租约 ID，
// 旧持有者的任何后续回执都会因 epoch 不匹配被拒绝。接管时仍在 running 的步骤
// 被重置为 failed 并提升执行版本，由新持有者重新派发执行。
func (s *Service) TakeoverLease(ctx context.Context, taskID, newHolder, reason string) (*RestoreTask, error) {
	if newHolder == "" {
		return nil, classified(ErrCodeInvalidArgument, "new holder is required")
	}
	var out *RestoreTask
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		if !task.Active() {
			return classified(ErrCodeConflict, "cannot take over lease of %s task %s", task.Status, task.ID)
		}
		now := s.timeNow()
		task.LeaseID = tx.NewID("lease")
		task.LeaseEpoch = task.LeaseEpoch + 1
		task.LeaseHolder = newHolder
		for i := range task.Steps {
			step := &task.Steps[i]
			if step.Status == StepRunning {
				step.Status = StepFailed
				step.ExecutionVersion++
				step.LastDetail = "reset by lease takeover"
				t := now
				step.UpdatedAt = &t
			}
		}
		tx.PutTask(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: newHolder, Action: LeaseTakenOver,
			Reason: reason,
		})
		out = cloneTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListLeaseEvents 返回租约获取/接管/释放的审计记录；taskID 为空时返回全部。
func (s *Service) ListLeaseEvents(ctx context.Context, taskID string) ([]LeaseEvent, error) {
	var out []LeaseEvent
	err := s.store.View(func(tx *Tx) error {
		for _, e := range tx.ListLeaseEvents() {
			if taskID == "" || e.TaskID == taskID {
				out = append(out, e)
			}
		}
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// 保留清理
// ---------------------------------------------------------------------------

// RunRetention 在单个事务的一致快照上作出保留/删除决定：
//   - 仍被有效恢复链冻结的快照，保留；
//   - 命中保留策略的最近已完成快照及其全部祖先（增量链可用性），保留；
//   - 未完成（pending）子快照本身及其祖先，保留；
//   - 其余快照删除，失败快照与过期已完成快照分别记录原因。
//
// 每个决定（含原因）与实际删除在同一事务落库为一条 RetentionRun。
func (s *Service) RunRetention(ctx context.Context, rules []RetentionRule) (*RetentionRun, error) {
	if len(rules) == 0 {
		return nil, classified(ErrCodeInvalidArgument, "at least one retention rule is required")
	}
	covered := map[string]RetentionRule{}
	for _, r := range rules {
		if r.DatasetID == "" {
			return nil, classified(ErrCodeInvalidArgument, "retention rule requires a dataset id")
		}
		if r.KeepLatestCompleted < 0 {
			return nil, classified(ErrCodeInvalidArgument, "keep latest completed must be >= 0 for %s", r.DatasetID)
		}
		covered[r.DatasetID] = r
	}

	var out *RetentionRun
	err := s.store.Update(func(tx *Tx) error {
		protected := map[string]map[string]bool{} // snapshotID -> reason 集合
		addReason := func(id, reason string) {
			if protected[id] == nil {
				protected[id] = map[string]bool{}
			}
			protected[id][reason] = true
		}
		// 沿父边向上标记祖先（带环保护）。
		markAncestors := func(startID, reason string) {
			curID := startID
			seen := map[string]bool{}
			for curID != "" {
				if seen[curID] {
					return
				}
				seen[curID] = true
				cur, ok := tx.GetSnapshot(curID)
				if !ok {
					return
				}
				addReason(cur.ID, reason)
				if cur.Kind == KindFull {
					return
				}
				curID = cur.ParentID
			}
		}

		// (1) 有效恢复链冻结的内容一律保留。
		for _, task := range tx.ListTasks() {
			if !task.Active() {
				continue
			}
			for _, f := range task.Chain {
				addReason(f.SnapshotID, ReasonActiveRestore)
			}
		}

		// (2) 保留策略：每个数据集最近 N 个已完成快照，外加它们的祖先链。
		for datasetID, rule := range covered {
			var completed []*Snapshot
			for _, sn := range tx.ListSnapshotsByDataset(datasetID) {
				if sn.Status == StatusCompleted {
					completed = append(completed, sn)
				}
			}
			if rule.KeepLatestCompleted < len(completed) {
				completed = completed[len(completed)-rule.KeepLatestCompleted:]
			}
			for _, sn := range completed {
				addReason(sn.ID, ReasonRetentionPolicy)
				markAncestors(sn.ParentID, ReasonAncestorRetained)
			}
		}

		// (3) 未完成（pending）子快照引用的内容：自身 + 祖先链保留。
		for _, sn := range tx.ListSnapshots() {
			if sn.Status != StatusPending {
				continue
			}
			addReason(sn.ID, ReasonSnapshotInProgress)
			markAncestors(sn.ParentID, ReasonAncestorIncomplete)
		}

		// 汇总决定：仅评估被规则覆盖的数据集。
		run := &RetentionRun{ID: tx.NewID("run"), DecidedAt: s.timeNow(), Rules: append([]RetentionRule(nil), rules...)}
		for _, sn := range tx.ListSnapshots() {
			if _, ok := covered[sn.DatasetID]; !ok {
				continue
			}
			if reasons, keep := protected[sn.ID]; keep {
				rs := make([]string, 0, len(reasons))
				for r := range reasons {
					rs = append(rs, r)
				}
				sort.Strings(rs)
				run.Decisions = append(run.Decisions, RetentionDecision{
					SnapshotID: sn.ID, DatasetID: sn.DatasetID,
					Action: RetentionRetained, Reasons: rs,
				})
				continue
			}
			reason := ReasonExpiredUnreferenced
			if sn.Status == StatusFailed {
				reason = ReasonFailedUnreferenced
			}
			run.Decisions = append(run.Decisions, RetentionDecision{
				SnapshotID: sn.ID, DatasetID: sn.DatasetID,
				Action: RetentionDeleted, Reasons: []string{reason},
			})
			tx.DeleteSnapshot(sn.ID)
		}
		tx.AddRetentionRun(*run)
		out = cloneRetentionRun(run)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListRetentionRuns 列出历次保留清理的决定记录。
func (s *Service) ListRetentionRuns(ctx context.Context) ([]*RetentionRun, error) {
	var out []*RetentionRun
	err := s.store.View(func(tx *Tx) error {
		for _, r := range tx.ListRetentionRuns() {
			out = append(out, cloneRetentionRun(r))
		}
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

// ListOutboxEvents 按创建顺序列出通知事件（投递轮询用）。
func (s *Service) ListOutboxEvents(ctx context.Context, includeDelivered bool) ([]*OutboxEvent, error) {
	var out []*OutboxEvent
	err := s.store.View(func(tx *Tx) error {
		for _, e := range tx.ListOutbox() {
			if !includeDelivered && e.DeliveredAt != nil {
				continue
			}
			cp := *e
			out = append(out, &cp)
		}
		return nil
	})
	return out, err
}

// MarkOutboxDelivered 将事件标记为已投递（at-least-once 投递的幂等记账）。
func (s *Service) MarkOutboxDelivered(ctx context.Context, eventID string) error {
	return s.store.Update(func(tx *Tx) error {
		return tx.MarkOutboxDelivered(eventID, s.timeNow())
	})
}
