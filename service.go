package backuprestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
		tx.BumpDatasetVersion(in.DatasetID)
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

// GetEffectiveChain 返回目标快照的“新链”视图：在原始回溯链上应用已发布的
// 压缩替代（可链式：被替代的完整快照自身也可能被更新的压缩替代）。
// 未发布任何相关压缩时与 GetBackupChain 一致。时间点恢复冻结计划使用同一套解析。
func (s *Service) GetEffectiveChain(ctx context.Context, targetSnapshotID string) ([]*Snapshot, error) {
	var out []*Snapshot
	err := s.store.View(func(tx *Tx) error {
		nodes, err := resolveEffectivePlan(tx, targetSnapshotID, s.timeNow())
		if err != nil {
			return err
		}
		for _, n := range nodes {
			out = append(out, cloneSnapshot(n.snapshot))
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
//  1. 校验目标快照已完成（失败/进行中均不可恢复），并回溯构造到完整快照的执行计划
//     （原始链上应用已发布的压缩替代，压缩产物节点带溯源）；
//  2. 冻结计划：计划引用的每个快照（含压缩产物折叠的原链节点）此后保留清理不得删除；
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
		now := s.timeNow()
		frozen, steps, err := buildFrozenPlan(tx, target, now)
		if err != nil {
			return err
		}
		if existing, ok := tx.FindActiveTaskByEnv(in.TargetEnvironment); ok {
			return classified(ErrCodeConflict, "target environment %s already has active restore %s (lease %s epoch %d)",
				in.TargetEnvironment, existing.ID, existing.LeaseID, existing.LeaseEpoch)
		}

		leaseID := tx.NewID("lease")
		task := RestoreTask{
			ID:                 tx.NewID("task"),
			DatasetID:          target.DatasetID,
			TargetEnvironment:  in.TargetEnvironment,
			Mode:               RestoreModeSnapshot,
			TargetSnapshotID:   target.ID,
			TargetTime:         *target.CompletedAt,
			SelectedSnapshotID: target.ID,
			FrozenAt:           now,
			Chain:              frozen,
			Steps:              steps,
			Status:             TaskPending,
			LeaseID:            leaseID,
			LeaseEpoch:         1,
			LeaseHolder:        in.Holder,
			CreatedAt:          now,
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

// CreatePointInTimeRestoreInput 是按时间点创建恢复的入参。用户只给目标时间，
// 不给快照 ID：系统负责选出该时间点可见的最新已完成快照。
type CreatePointInTimeRestoreInput struct {
	DatasetID         string
	TargetTime        time.Time
	TargetEnvironment string
	Holder            string
}

// PointInTimeSelection 说明时间点恢复实际选中了什么，便于调用方核对。
type PointInTimeSelection struct {
	SelectedSnapshotID string
	TargetTime         time.Time
	CompletedAt        time.Time
}

// CreatePointInTimeRestore 在单个事务内：
//  1. 在数据集的一致快照上选出“完成时间不晚于目标时间”的最新已完成快照
//     （不选择完成于目标时间之后的快照，也绝不退而选择最近一个文件）；
//  2. 从该快照回溯原始链并应用已发布的压缩替代，形成带来源溯源的执行计划；
//  3. 冻结计划并取得目标环境租约。
//
// 冻结后即使有并发链压缩发布或保留清理，这份计划也不会被更换：
// 计划引用的快照（含被折叠的原链）在任务结束前一律受 active_restore 保护不得回收。
func (s *Service) CreatePointInTimeRestore(ctx context.Context, in CreatePointInTimeRestoreInput) (*RestoreTask, error) {
	if in.DatasetID == "" || in.TargetEnvironment == "" || in.Holder == "" {
		return nil, classified(ErrCodeInvalidArgument, "dataset id, target environment and holder are required")
	}
	if in.TargetTime.IsZero() {
		return nil, classified(ErrCodeInvalidArgument, "target time is required")
	}
	targetTime := in.TargetTime.UTC()
	var out *RestoreTask
	err := s.store.Update(func(tx *Tx) error {
		target, err := selectPointInTimeSnapshot(tx, in.DatasetID, targetTime)
		if err != nil {
			return err
		}
		now := s.timeNow()
		frozen, steps, err := buildFrozenPlan(tx, target, targetTime)
		if err != nil {
			return err
		}
		if existing, ok := tx.FindActiveTaskByEnv(in.TargetEnvironment); ok {
			return classified(ErrCodeConflict, "target environment %s already has active restore %s (lease %s epoch %d)",
				in.TargetEnvironment, existing.ID, existing.LeaseID, existing.LeaseEpoch)
		}

		leaseID := tx.NewID("lease")
		task := RestoreTask{
			ID:                 tx.NewID("task"),
			DatasetID:          in.DatasetID,
			TargetEnvironment:  in.TargetEnvironment,
			Mode:               RestoreModePointInTime,
			TargetSnapshotID:   target.ID, // 实际选中的快照即执行目标
			TargetTime:         targetTime,
			SelectedSnapshotID: target.ID,
			FrozenAt:           now,
			Chain:              frozen,
			Steps:              steps,
			Status:             TaskPending,
			LeaseID:            leaseID,
			LeaseEpoch:         1,
			LeaseHolder:        in.Holder,
			CreatedAt:          now,
		}
		tx.PutTask(task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: leaseID, Epoch: 1,
			Holder: in.Holder, Action: LeaseAcquired,
			Reason: fmt.Sprintf("point-in-time restore selected %s completed at %s",
				target.ID, target.CompletedAt.Format(time.RFC3339Nano)),
		})
		out = cloneTask(&task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetPointInTimeSelection 以只读方式预览某目标时间会选中哪个快照（不冻结、不建任务）。
// 不存在不晚于该时间的已完成快照时返回 not_found。
func (s *Service) GetPointInTimeSelection(ctx context.Context, datasetID string, target time.Time) (*PointInTimeSelection, error) {
	if datasetID == "" {
		return nil, classified(ErrCodeInvalidArgument, "dataset id is required")
	}
	if target.IsZero() {
		return nil, classified(ErrCodeInvalidArgument, "target time is required")
	}
	var out *PointInTimeSelection
	err := s.store.View(func(tx *Tx) error {
		sn, err := selectPointInTimeSnapshot(tx, datasetID, target.UTC())
		if err != nil {
			return err
		}
		out = &PointInTimeSelection{
			SelectedSnapshotID: sn.ID,
			TargetTime:         target.UTC(),
			CompletedAt:        *sn.CompletedAt,
		}
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
			if task.Status == TaskSucceeded {
				// 幂等：任务已完成，复用已有通知，绝不产生第二条。
				event, _ := tx.GetOutboxByTask(task.ID)
				res.Task = cloneTask(task)
				if event != nil {
					cp := *event
					res.Event = &cp
				}
				return nil
			}
			// 已取消：完成与取消竞争中取消胜出，拒绝完成而不是伪装成幂等成功。
			return classified(ErrCodeConflict, "restore task %s is %s and cannot be completed", task.ID, task.Status)
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

// CancelRestore 取消一个尚未终态的恢复任务。取消与步骤派发/回执/完成并发时，
// 所有这些操作都在同一个可序列化存储上排队：一旦取消提交，任务进入唯一终态
// canceled，租约释放，此后任何派发、回执、接管、完成都因任务不再 active 而被拒绝。
//
// 已成功的任务不能再取消（返回 conflict，完成与取消竞争中完成胜出）；
// 对已取消的任务重复取消是幂等的，返回当前任务。步骤状态原样保留，
// 因此取消后仍能查到实际使用的备份链与停在哪一步。
func (s *Service) CancelRestore(ctx context.Context, taskID, requestedBy, reason string) (*RestoreTask, error) {
	if requestedBy == "" {
		return nil, classified(ErrCodeInvalidArgument, "requested by is required")
	}
	var out *RestoreTask
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		if task.Status == TaskCanceled {
			out = cloneTask(task) // 幂等：取消只有一个终态
			return nil
		}
		if !task.Active() {
			return classified(ErrCodeConflict, "cannot cancel %s restore task %s", task.Status, task.ID)
		}
		now := s.timeNow()
		task.Status = TaskCanceled
		task.CanceledAt = &now
		task.CancelReason = reason
		tx.PutTask(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: requestedBy, Action: LeaseReleased,
			Reason: "restore canceled: " + reason,
		})
		out = cloneTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetRestoreProgress 查询恢复任务的执行进度：实际使用的备份链（含每个节点是原始
// 备份还是压缩产物、压缩任务来源、被折叠的原链快照）、步骤计数与失败位置。
// 取消或失败暂停后，这里给出的 FirstFailed 就是恢复将继续的安全位置。
func (s *Service) GetRestoreProgress(ctx context.Context, taskID string) (*RestoreProgress, error) {
	var out *RestoreProgress
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		p := &RestoreProgress{
			TaskID:             task.ID,
			Status:             task.Status,
			DatasetID:          task.DatasetID,
			Mode:               task.Mode,
			TargetTime:         task.TargetTime,
			SelectedSnapshotID: task.SelectedSnapshotID,
			Total:              len(task.Steps),
		}
		p.Plan = make([]PlanNodeView, len(task.Chain))
		for i, f := range task.Chain {
			p.Plan[i] = PlanNodeView{
				Index:             f.Index,
				SnapshotID:        f.SnapshotID,
				Digest:            f.Digest,
				Kind:              f.Kind,
				Source:            f.Source,
				CompactionTaskID:  f.CompactionTaskID,
				ReplacedSnapshots: append([]string(nil), f.ReplacedSnapshots...),
			}
		}
		for _, st := range task.Steps {
			switch st.Status {
			case StepPending:
				p.Pending++
			case StepRunning:
				p.Running++
			case StepSucceeded:
				p.Succeeded++
				p.CompletedSteps++
			case StepFailed:
				p.Failed++
			}
			if st.Status != StepSucceeded {
				v := StepFailureView{
					Index: st.Index, SnapshotID: st.SnapshotID, Status: st.Status,
					Attempts: st.Attempts, LastDetail: st.LastDetail,
				}
				p.Failures = append(p.Failures, v)
				if p.FirstFailed == nil {
					cp := v
					p.FirstFailed = &cp
				}
			}
		}
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
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
//   - 仍被有效压缩任务冻结的原链快照，保留；
//   - 命中保留策略的最近已完成快照及其全部祖先（增量链可用性），保留；
//   - 未完成（pending）子快照本身及其祖先，保留；
//   - 其余快照删除：被压缩替代的记录 replaced_by_compaction，
//     失败快照与过期已完成快照分别记录原因。
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

		// (1) 有效恢复计划冻结的内容一律保留：计划节点本身（含压缩产物），
		// 以及压缩产物节点折叠掉的原链快照——它们也被计划引用，任务结束前不得回收。
		for _, task := range tx.ListTasks() {
			if !task.Active() {
				continue
			}
			for _, f := range task.Chain {
				addReason(f.SnapshotID, ReasonActiveRestore)
				for _, replacedID := range f.ReplacedSnapshots {
					addReason(replacedID, ReasonActiveRestore)
				}
			}
		}

		// (1b) 有效压缩任务冻结的链同样保留：压缩执行期间原链不得被回收。
		for _, c := range tx.ListCompactions() {
			if !c.Active() {
				continue
			}
			for _, f := range c.Chain {
				addReason(f.SnapshotID, ReasonActiveCompaction)
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
			} else if _, replaced := tx.GetReplacement(sn.ID); replaced {
				// 已被压缩结果替代、且不再被任何恢复/策略/子快照引用的原链节点。
				reason = ReasonReplacedByCompaction
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
// 链压缩：一段已完成快照链 -> 新的完整快照
// ---------------------------------------------------------------------------

// CreateCompactionInput 创建压缩任务的入参。IdempotencyKey 是任务号：
// 相同键重复创建返回同一任务，键相同但链不同则报 conflict。
type CreateCompactionInput struct {
	FromSnapshotID string // 起点，必须为完整快照（链根）
	ToSnapshotID   string // 终点
	Holder         string // 申请执行租约的执行方标识
	IdempotencyKey string
}

// compactionExpectedDigest 由冻结链推导期望的内容摘要。
// 执行方合并出的新完整快照必须带有该摘要才允许发布。
func compactionExpectedDigest(datasetID string, chain []FrozenSnapshot) string {
	h := sha256.New()
	fmt.Fprintf(h, "compaction/v1\ndataset=%s\n", datasetID)
	for _, f := range chain {
		fmt.Fprintf(h, "%d %s %s %s\n", f.Index, f.SnapshotID, f.Kind, f.Digest)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// walkSegment 从 to 沿父边回溯到 from（含两端），返回 from -> to 的有序切片。
// from 不是 to 的祖先（链不连续）时报 invalid_argument。
func walkSegment(tx *Tx, fromID, toID string) ([]*Snapshot, error) {
	rev := make([]*Snapshot, 0)
	visited := map[string]bool{}
	curID := toID
	for {
		if visited[curID] {
			return nil, classified(ErrCodeConflict, "cycle detected in snapshot ancestry at %s", curID)
		}
		visited[curID] = true
		cur, ok := tx.GetSnapshot(curID)
		if !ok {
			return nil, classified(ErrCodeNotFound, "snapshot %s not found", curID)
		}
		rev = append(rev, cur)
		if cur.ID == fromID {
			break
		}
		if cur.Kind == KindFull {
			return nil, classified(ErrCodeInvalidArgument,
				"snapshot %s is not an ancestor of %s (reached chain root %s)", fromID, toID, cur.ID)
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

// CreateCompaction 在单个事务内：
//  1. 幂等校验：任务号已存在时直接返回既有任务（链不一致则报 conflict）；
//  2. 校验起点为完整快照、起点是终点沿父边的祖先、链上快照全部已完成且同数据集；
//  3. 冻结起点、终点、链中每个快照的摘要与当前数据集版本；
//  4. 取得执行租约（epoch 从 1 开始，语义与恢复租约一致）。
func (s *Service) CreateCompaction(ctx context.Context, in CreateCompactionInput) (*CompactionTask, error) {
	if in.FromSnapshotID == "" || in.ToSnapshotID == "" || in.Holder == "" || in.IdempotencyKey == "" {
		return nil, classified(ErrCodeInvalidArgument,
			"from/to snapshot id, holder and idempotency key are required")
	}
	var out *CompactionTask
	err := s.store.Update(func(tx *Tx) error {
		if existing, ok := tx.FindCompactionByKey(in.IdempotencyKey); ok {
			if existing.FromSnapshotID != in.FromSnapshotID || existing.ToSnapshotID != in.ToSnapshotID {
				return classified(ErrCodeConflict,
					"idempotency key %s already used by compaction %s with different chain (%s -> %s)",
					in.IdempotencyKey, existing.ID, existing.FromSnapshotID, existing.ToSnapshotID)
			}
			out = cloneCompactionTask(existing)
			return nil
		}
		from, ok := tx.GetSnapshot(in.FromSnapshotID)
		if !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", in.FromSnapshotID)
		}
		if from.Kind != KindFull {
			return classified(ErrCodeInvalidArgument,
				"compaction must start at a full snapshot, %s is %s", from.ID, from.Kind)
		}
		seg, err := walkSegment(tx, in.FromSnapshotID, in.ToSnapshotID)
		if err != nil {
			return err
		}
		if len(seg) < 2 {
			return classified(ErrCodeInvalidArgument,
				"compaction requires a chain of at least 2 snapshots")
		}
		for _, sn := range seg {
			if sn.DatasetID != from.DatasetID {
				return classified(ErrCodeConflict, "snapshot %s belongs to another dataset", sn.ID)
			}
			if sn.Status != StatusCompleted {
				return classified(ErrCodeConflict,
					"snapshot %s on chain is %s, only completed chains can be compacted", sn.ID, sn.Status)
			}
		}

		now := s.timeNow()
		frozen := make([]FrozenSnapshot, len(seg))
		steps := make([]RestoreStep, len(seg))
		for i, sn := range seg {
			frozen[i] = FrozenSnapshot{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Kind: sn.Kind}
			steps[i] = RestoreStep{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Status: StepPending}
		}
		leaseID := tx.NewID("lease")
		task := CompactionTask{
			ID:             tx.NewID("comp"),
			IdempotencyKey: in.IdempotencyKey,
			DatasetID:      from.DatasetID,
			FromSnapshotID: from.ID,
			ToSnapshotID:   in.ToSnapshotID,
			Chain:          frozen,
			DatasetVersion: tx.DatasetVersion(from.DatasetID),
			Steps:          steps,
			Status:         TaskPending,
			LeaseID:        leaseID,
			LeaseEpoch:     1,
			LeaseHolder:    in.Holder,
			CreatedAt:      now,
		}
		task.ExpectedDigest = compactionExpectedDigest(task.DatasetID, frozen)
		tx.PutCompaction(task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: leaseID, Epoch: 1,
			Holder: in.Holder, Action: LeaseAcquired,
		})
		out = cloneCompactionTask(&task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetCompactionTask 查询压缩任务（含冻结链、步骤与当前租约）。
func (s *Service) GetCompactionTask(ctx context.Context, taskID string) (*CompactionTask, error) {
	var out *CompactionTask
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		out = cloneCompactionTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListCompactionTasks 按创建顺序列出全部压缩任务。
func (s *Service) ListCompactionTasks(ctx context.Context) ([]*CompactionTask, error) {
	var out []*CompactionTask
	err := s.store.View(func(tx *Tx) error {
		for _, t := range tx.ListCompactions() {
			out = append(out, cloneCompactionTask(t))
		}
		return nil
	})
	return out, err
}

// CompactionProgress 是压缩任务的进度视图。
type CompactionProgress struct {
	TaskID        string
	Status        TaskStatus
	Total         int
	Pending       int
	Running       int
	Succeeded     int
	Failed        int
	NewSnapshotID string // 发布成功后为新完整快照 ID
}

// GetCompactionProgress 汇总任务的步骤进度。
func (s *Service) GetCompactionProgress(ctx context.Context, taskID string) (*CompactionProgress, error) {
	var out *CompactionProgress
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		p := &CompactionProgress{
			TaskID: task.ID, Status: task.Status,
			Total: len(task.Steps), NewSnapshotID: task.NewSnapshotID,
		}
		for _, st := range task.Steps {
			switch st.Status {
			case StepPending:
				p.Pending++
			case StepRunning:
				p.Running++
			case StepSucceeded:
				p.Succeeded++
			case StepFailed:
				p.Failed++
			}
		}
		out = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// requireCompactionLease 校验压缩任务有效且租约 ID + epoch 完全匹配。
func requireCompactionLease(task *CompactionTask, leaseID string, epoch int64) error {
	if !task.Active() {
		return classified(ErrCodeConflict, "compaction task %s is %s", task.ID, task.Status)
	}
	if task.LeaseID != leaseID || task.LeaseEpoch != epoch {
		return classified(ErrCodeLease, "lease mismatch for compaction %s: current=%s epoch=%d, got=%s epoch=%d",
			task.ID, task.LeaseID, task.LeaseEpoch, leaseID, epoch)
	}
	return nil
}

// CompactionDispatch 是一次压缩步骤派发的结果。Step 为 nil 表示没有可派发步骤。
type CompactionDispatch struct {
	Task *CompactionTask
	Step *RestoreStep
}

// DispatchCompactionStep 沿冻结链有序派发下一个合并步骤，语义与恢复步骤一致：
// 每次派发递增 ExecutionVersion，回执必须携带该版本。
func (s *Service) DispatchCompactionStep(ctx context.Context, taskID, leaseID string, epoch int64) (*CompactionDispatch, error) {
	res := &CompactionDispatch{}
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		if err := requireCompactionLease(task, leaseID, epoch); err != nil {
			return err
		}
		now := s.timeNow()
		for i := range task.Steps {
			step := &task.Steps[i]
			if step.Status == StepSucceeded {
				continue
			}
			if step.Status == StepRunning {
				res.Task = cloneCompactionTask(task)
				return nil
			}
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
			tx.PutCompaction(*task)
			res.Task = cloneCompactionTask(task)
			cp := *step
			res.Step = &cp
			return nil
		}
		res.Task = cloneCompactionTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// AckCompactionStep 处理压缩步骤回执：租约或执行版本不匹配一律拒绝且状态不变；
// 失败的步骤回到 failed 等待按序重试。
func (s *Service) AckCompactionStep(ctx context.Context, in AckStepInput) (*RestoreStep, error) {
	var out *RestoreStep
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetCompaction(in.TaskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", in.TaskID)
		}
		if err := requireCompactionLease(task, in.LeaseID, in.Epoch); err != nil {
			return err
		}
		if in.StepIndex < 0 || in.StepIndex >= len(task.Steps) {
			return classified(ErrCodeInvalidArgument, "step index %d out of range (chain length %d)",
				in.StepIndex, len(task.Steps))
		}
		step := &task.Steps[in.StepIndex]
		if step.ExecutionVersion != in.ExecutionVersion {
			return classified(ErrCodeConflict,
				"stale receipt for compaction %s step %d: current execution version %d, receipt %d",
				task.ID, step.Index, step.ExecutionVersion, in.ExecutionVersion)
		}
		if step.Status != StepRunning {
			return classified(ErrCodeConflict, "compaction %s step %d is not running (status=%s)",
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
		tx.PutCompaction(*task)
		cp := *step
		out = &cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TakeoverCompactionLease 接管有效压缩任务的执行租约：epoch 单调递增并换发租约 ID，
// 在途步骤重置为可重试；旧租约持有者的回执因 epoch 不匹配被拒绝。
func (s *Service) TakeoverCompactionLease(ctx context.Context, taskID, newHolder, reason string) (*CompactionTask, error) {
	if newHolder == "" {
		return nil, classified(ErrCodeInvalidArgument, "new holder is required")
	}
	var out *CompactionTask
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		if !task.Active() {
			return classified(ErrCodeConflict, "cannot take over lease of %s compaction %s", task.Status, task.ID)
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
		tx.PutCompaction(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: newHolder, Action: LeaseTakenOver,
			Reason: reason,
		})
		out = cloneCompactionTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PublishCompactionInput 发布压缩结果的入参。
// ContentDigest 是执行方对合并产物计算的内容摘要，必须等于任务冻结的 ExpectedDigest。
type PublishCompactionInput struct {
	TaskID        string
	LeaseID       string
	Epoch         int64
	ContentDigest string
}

// PublishCompactionResult 返回终态任务、新完整快照与全部替代关系。
type PublishCompactionResult struct {
	Task         *CompactionTask
	NewSnapshot  *Snapshot
	Replacements []Replacement
}

// PublishCompaction 在单个事务内原子发布压缩结果：
//  1. 校验租约与全部步骤成功；
//  2. 校验内容摘要与冻结的期望摘要一致，不通过则任务保持可重试、读者仍看到原链；
//  3. 校验冻结链上没有任何快照已被其他压缩替代（同一链最多一个有效结果）；
//  4. 一次性写入：新的已完成完整快照 + 每个原快照的替代关系 + 任务终态。
//
// 新快照在此事务提交前不存在，读者不可能观察到半成品；对已成功任务重复调用幂等。
func (s *Service) PublishCompaction(ctx context.Context, in PublishCompactionInput) (*PublishCompactionResult, error) {
	if in.ContentDigest == "" {
		return nil, classified(ErrCodeInvalidArgument, "content digest is required")
	}
	res := &PublishCompactionResult{}
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetCompaction(in.TaskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", in.TaskID)
		}
		if !task.Active() {
			// 幂等：已发布的任务返回既有结果，不产生第二个快照。
			snap, ok := tx.GetSnapshot(task.NewSnapshotID)
			if !ok {
				return classified(ErrCodeConflict, "compaction %s is %s but result snapshot %s is missing",
					task.ID, task.Status, task.NewSnapshotID)
			}
			res.Task = cloneCompactionTask(task)
			res.NewSnapshot = cloneSnapshot(snap)
			for _, f := range task.Chain {
				if r, ok := tx.GetReplacement(f.SnapshotID); ok {
					res.Replacements = append(res.Replacements, *r)
				}
			}
			return nil
		}
		if err := requireCompactionLease(task, in.LeaseID, in.Epoch); err != nil {
			return err
		}
		for _, step := range task.Steps {
			if step.Status != StepSucceeded {
				return classified(ErrCodeConflict, "compaction %s step %d is %s, cannot publish",
					task.ID, step.Index, step.Status)
			}
		}
		if in.ContentDigest != task.ExpectedDigest {
			return classified(ErrCodeConflict,
				"content digest mismatch for compaction %s: expected %s, got %s",
				task.ID, task.ExpectedDigest, in.ContentDigest)
		}
		for _, ex := range tx.ListSnapshots() {
			if ex.Digest == in.ContentDigest {
				return classified(ErrCodeAlreadyExists, "snapshot with digest %s already exists: %s",
					in.ContentDigest, ex.ID)
			}
		}
		// 同一链的并发压缩：任何节点已被替代则本任务失败，赢家只有一个。
		for _, f := range task.Chain {
			snap, ok := tx.GetSnapshot(f.SnapshotID)
			if !ok {
				return classified(ErrCodeConflict, "chain snapshot %s no longer exists", f.SnapshotID)
			}
			if snap.Digest != f.Digest {
				return classified(ErrCodeConflict, "chain snapshot %s digest changed since freeze", f.SnapshotID)
			}
			if r, replaced := tx.GetReplacement(f.SnapshotID); replaced {
				return classified(ErrCodeConflict,
					"chain snapshot %s already replaced by compaction %s", f.SnapshotID, r.CompactionTaskID)
			}
		}

		now := s.timeNow()
		newSnap := Snapshot{
			ID:          tx.NewID("snap"),
			DatasetID:   task.DatasetID,
			Kind:        KindFull,
			Digest:      in.ContentDigest,
			Status:      StatusCompleted,
			CreatedAt:   now,
			CompletedAt: &now,
		}
		tx.PutSnapshot(newSnap)
		tx.BumpDatasetVersion(task.DatasetID)
		for _, f := range task.Chain {
			rep := Replacement{
				OriginalSnapshotID: f.SnapshotID,
				NewSnapshotID:      newSnap.ID,
				CompactionTaskID:   task.ID,
				DatasetID:          task.DatasetID,
				PublishedAt:        now,
			}
			if err := tx.PutReplacement(rep); err != nil {
				return err
			}
			res.Replacements = append(res.Replacements, rep)
		}
		task.Status = TaskSucceeded
		task.NewSnapshotID = newSnap.ID
		task.CompletedAt = &now
		tx.PutCompaction(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: task.LeaseHolder, Action: LeaseReleased, Reason: "compaction published",
		})
		res.Task = cloneCompactionTask(task)
		res.NewSnapshot = cloneSnapshot(&newSnap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ListReplacements 列出替代关系（原快照 -> 新完整快照）；datasetID 为空时返回全部。
func (s *Service) ListReplacements(ctx context.Context, datasetID string) ([]Replacement, error) {
	var out []Replacement
	err := s.store.View(func(tx *Tx) error {
		for _, r := range tx.ListReplacements() {
			if datasetID == "" || r.DatasetID == datasetID {
				out = append(out, *r)
			}
		}
		return nil
	})
	return out, err
}

// nodeOrigin 记录执行计划节点相对原始备份链的来历。
type nodeOrigin struct {
	// compactionTaskID 非空时表示该节点是这个压缩任务合成出的产物。
	compactionTaskID string
	// replaced 是被该节点折叠掉的全部原链快照（链式压缩时含传递闭包）。
	replaced []string
}

// resolvedPlanNode 是应用压缩替代后的一个执行计划节点。
type resolvedPlanNode struct {
	snapshot         *Snapshot
	compactionTaskID string
	replaced         []string
}

// resolveEffectivePlan 从目标快照沿原始父边回溯得到原始链，然后反复应用
// asOf 之前（含）已发布的压缩替代（可链式：被替代的压缩产物自身也可能再被压缩），
// 返回有序执行计划（完整快照在前，目标快照在末尾）。
//
// asOf 是时间点边界：只有 PublishedAt 不晚于 asOf 的替代关系才能参与折叠。
// 时间点恢复传入用户目标时间——目标时刻之后才发布的压缩不属于那份历史，
// 不得改变冻结计划；直接快照恢复传入冻结时刻（当时已发布的替代全部可见）。
//
// 每次折叠都缩短链，保证终止；压缩产物节点记录产出它的压缩任务以及
// 被折叠的全部原链快照，使计划在“链已被压缩甚至原链已被回收”的情况下
// 仍然可解释、可复现，而不是简单选择最近一个文件。
func resolveEffectivePlan(tx *Tx, targetID string, asOf time.Time) ([]resolvedPlanNode, error) {
	if _, ok := tx.GetSnapshot(targetID); !ok {
		return nil, classified(ErrCodeNotFound, "snapshot %s not found", targetID)
	}
	chain, err := walkChain(tx, targetID)
	if err != nil {
		return nil, err
	}
	// 每个已发布压缩产物 -> 产出它的压缩任务，用于递归求折叠闭包。
	resultTask := map[string]*CompactionTask{}
	for _, c := range tx.ListCompactions() {
		if c.NewSnapshotID != "" && c.Status == TaskSucceeded {
			resultTask[c.NewSnapshotID] = c
		}
	}
	// originClosure 返回某节点若是压缩产物时所折叠的全部原链快照（递归传递闭包）。
	memo := map[string][]string{}
	var originClosure func(id string) []string
	originClosure = func(id string) []string {
		if v, ok := memo[id]; ok {
			return v
		}
		c, isResult := resultTask[id]
		if !isResult {
			memo[id] = nil
			return nil
		}
		acc := make([]string, 0)
		for _, f := range c.Chain {
			acc = append(acc, f.SnapshotID)
			acc = append(acc, originClosure(f.SnapshotID)...)
		}
		acc = dedupeStrings(acc)
		memo[id] = acc
		return acc
	}
	// 已折叠产物的溯源：压缩产物 ID -> 来历。
	origins := map[string]nodeOrigin{}
	for {
		applied := false
		for i, sn := range chain {
			rep, ok := tx.GetReplacement(sn.ID)
			if !ok {
				continue
			}
			if rep.PublishedAt.After(asOf) {
				continue // 目标时间之后才发布的替代不参与这份历史计划
			}
			task, ok := tx.GetCompaction(rep.CompactionTaskID)
			if !ok {
				continue
			}
			seg := task.Chain
			if i+len(seg) > len(chain) {
				continue
			}
			match := true
			for j, f := range seg {
				if chain[i+j].ID != f.SnapshotID {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			newSnap, ok := tx.GetSnapshot(rep.NewSnapshotID)
			if !ok {
				continue
			}
			// 记录本节点折叠的原链；压缩段内若含更早的压缩产物则递归并入其闭包。
			origins[newSnap.ID] = nodeOrigin{
				compactionTaskID: task.ID,
				replaced:         append([]string(nil), originClosure(newSnap.ID)...),
			}
			next := make([]*Snapshot, 0, len(chain)-len(seg)+1)
			next = append(next, chain[:i]...)
			next = append(next, newSnap)
			next = append(next, chain[i+len(seg):]...)
			chain = next
			applied = true
			break
		}
		if !applied {
			break
		}
	}
	out := make([]resolvedPlanNode, len(chain))
	for i, sn := range chain {
		out[i] = resolvedPlanNode{snapshot: sn}
		if m, ok := origins[sn.ID]; ok {
			out[i].compactionTaskID = m.compactionTaskID
			out[i].replaced = append([]string(nil), m.replaced...)
		}
	}
	return out, nil
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	out := in[:0:0]
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

// buildFrozenPlan 把目标快照解析为可冻结的执行计划：在 asOf 时间边界内
// 应用压缩替代后的有序节点，每个节点带摘要与溯源。
// 链上任一节点不是 completed 都是冲突（不允许恢复半成品）。
func buildFrozenPlan(tx *Tx, target *Snapshot, asOf time.Time) ([]FrozenSnapshot, []RestoreStep, error) {
	nodes, err := resolveEffectivePlan(tx, target.ID, asOf)
	if err != nil {
		return nil, nil, err
	}
	frozen := make([]FrozenSnapshot, len(nodes))
	steps := make([]RestoreStep, len(nodes))
	for i, n := range nodes {
		sn := n.snapshot
		if sn.Status != StatusCompleted {
			return nil, nil, classified(ErrCodeConflict,
				"snapshot %s on restore plan is %s, only completed chains can be restored", sn.ID, sn.Status)
		}
		source := FrozenSourceOriginal
		if n.compactionTaskID != "" {
			source = FrozenSourceCompaction
		}
		frozen[i] = FrozenSnapshot{
			Index:             i,
			SnapshotID:        sn.ID,
			Digest:            sn.Digest,
			Kind:              sn.Kind,
			Source:            source,
			CompactionTaskID:  n.compactionTaskID,
			ReplacedSnapshots: append([]string(nil), n.replaced...),
		}
		steps[i] = RestoreStep{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Status: StepPending}
	}
	return frozen, steps, nil
}

// selectPointInTimeSnapshot 在数据集内选出完成时间不晚于 target 的最新已完成快照。
// 完成时间相同时取 ID 最大者（ID 单调递增，即登记/完成最晚者），保证选择确定性。
//
// 压缩合成出的完整快照不参与候选：它是对一段旧链的物理重写，不代表新的数据版本，
// 其发布时间可能晚于其后继增量——选它会静默丢掉压缩段之后的增量。
// “是否用压缩产物执行”属于执行计划层面的折叠（buildFrozenPlan），不属于时间点选择。
// 没有任何满足条件的快照时返回 not_found——调用方不得退而选择“最近一个文件”。
func selectPointInTimeSnapshot(tx *Tx, datasetID string, target time.Time) (*Snapshot, error) {
	compactionResults := map[string]bool{}
	for _, c := range tx.ListCompactions() {
		if c.NewSnapshotID != "" {
			compactionResults[c.NewSnapshotID] = true
		}
	}
	var best *Snapshot
	for _, sn := range tx.ListSnapshotsByDataset(datasetID) {
		if sn.Status != StatusCompleted || sn.CompletedAt == nil {
			continue
		}
		if compactionResults[sn.ID] {
			continue
		}
		if sn.CompletedAt.After(target) {
			continue // 完成于目标时间之后的快照不可见
		}
		if best == nil ||
			sn.CompletedAt.After(*best.CompletedAt) ||
			(sn.CompletedAt.Equal(*best.CompletedAt) && sn.ID > best.ID) {
			best = sn
		}
	}
	if best == nil {
		return nil, classified(ErrCodeNotFound,
			"no completed snapshot in dataset %s at or before %s", datasetID, target.Format(time.RFC3339Nano))
	}
	return best, nil
}

// ReferenceBlock 是一条阻止快照被回收的引用。
type ReferenceBlock struct {
	Kind   string // restore_task | compaction_task | child_snapshot
	ID     string // 引用方 ID（任务 ID 或子快照 ID）
	Detail string
}

// SnapshotReferences 解释一个快照当前被谁引用、以及（若已发布压缩）被谁替代。
type SnapshotReferences struct {
	SnapshotID string
	Blocks     []ReferenceBlock
	ReplacedBy *Replacement
}

// ExplainSnapshotReferences 汇总快照的引用阻断原因，用于排查“为什么还没被清理”。
func (s *Service) ExplainSnapshotReferences(ctx context.Context, snapshotID string) (*SnapshotReferences, error) {
	var out *SnapshotReferences
	err := s.store.View(func(tx *Tx) error {
		snap, ok := tx.GetSnapshot(snapshotID)
		if !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", snapshotID)
		}
		res := &SnapshotReferences{SnapshotID: snapshotID}
		for _, task := range tx.ListTasks() {
			if !task.Active() {
				continue
			}
		blocked:
			for _, f := range task.Chain {
				if f.SnapshotID == snapshotID {
					res.Blocks = append(res.Blocks, ReferenceBlock{
						Kind: "restore_task", ID: task.ID,
						Detail: fmt.Sprintf("frozen by active restore to %s", task.TargetEnvironment),
					})
					break blocked
				}
				// 被压缩产物节点折叠的原链快照同样受冻结计划引用。
				for _, replacedID := range f.ReplacedSnapshots {
					if replacedID == snapshotID {
						res.Blocks = append(res.Blocks, ReferenceBlock{
							Kind: "restore_task", ID: task.ID,
							Detail: fmt.Sprintf("folded into plan node %s by active restore to %s",
								f.SnapshotID, task.TargetEnvironment),
						})
						break blocked
					}
				}
			}
		}
		for _, c := range tx.ListCompactions() {
			if !c.Active() {
				continue
			}
			for _, f := range c.Chain {
				if f.SnapshotID == snapshotID {
					res.Blocks = append(res.Blocks, ReferenceBlock{
						Kind: "compaction_task", ID: c.ID,
						Detail: fmt.Sprintf("frozen by active compaction %s -> %s", c.FromSnapshotID, c.ToSnapshotID),
					})
					break
				}
			}
		}
		for _, child := range tx.ListChildren(snapshotID) {
			if child.Status == StatusFailed {
				continue
			}
			res.Blocks = append(res.Blocks, ReferenceBlock{
				Kind: "child_snapshot", ID: child.ID,
				Detail: fmt.Sprintf("referenced by %s snapshot (status=%s)", child.Kind, child.Status),
			})
		}
		if rep, ok := tx.GetReplacement(snap.ID); ok {
			cp := *rep
			res.ReplacedBy = &cp
		}
		out = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
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
