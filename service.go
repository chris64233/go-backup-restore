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

// ---------------------------------------------------------------------------
// 恢复创建：冻结链 + 租约
// ---------------------------------------------------------------------------

// CreateRestoreInput 创建恢复任务的入参，两种互斥的目标指定方式：
//   - 快照恢复：提供 TargetSnapshotID，恢复某个确定的快照本身；
//   - 按时间点恢复：提供 DatasetID 与非零 TargetTime，由系统在
//     CreatedAt <= TargetTime 的已完成快照中选择最近的一个作为逻辑目标，
//     并把压缩替代解析进物理执行计划。
type CreateRestoreInput struct {
	// 快照恢复（与 DatasetID/TargetTime 二选一）。
	TargetSnapshotID string
	// 按时间点恢复。
	DatasetID  string
	TargetTime time.Time

	TargetEnvironment string
	Holder            string // 申请租约的执行方标识
}

// CreateRestore 在单个事务内：
//  1. 确定恢复目标：显式快照，或目标时间之前最近的已完成快照；
//  2. 构造恢复计划：快照模式回溯原始父链；时间点模式在原始链上应用
//     已发布的压缩替代（原链节点即使已被保留清理回收也能折叠到替代完整快照），
//     同时冻结逻辑链与物理执行链；
//  3. 冻结计划（此后并发的链压缩不能更换计划，保留清理不得删除物理链上的快照）；
//  4. 取得目标环境的恢复租约（同一环境同时只允许一个有效恢复）。
func (s *Service) CreateRestore(ctx context.Context, in CreateRestoreInput) (*RestoreTask, error) {
	if in.TargetEnvironment == "" || in.Holder == "" {
		return nil, classified(ErrCodeInvalidArgument, "target environment and holder are required")
	}
	snapshotMode := in.TargetSnapshotID != ""
	pitMode := in.DatasetID != "" || !in.TargetTime.IsZero()
	if snapshotMode == pitMode {
		return nil, classified(ErrCodeInvalidArgument,
			"exactly one of target snapshot id or (dataset id + target time) is required")
	}
	if pitMode && in.DatasetID == "" {
		return nil, classified(ErrCodeInvalidArgument, "dataset id is required for point-in-time restore")
	}
	var out *RestoreTask
	err := s.store.Update(func(tx *Tx) error {
		var target *Snapshot
		var logical, physical []FrozenSnapshot

		if pitMode {
			// 时间点选择：在数据集全部已完成快照（含已被保留清理归档的）中，
			// 取 CreatedAt <= 目标时间的最近一个；同一时刻以更大的 ID 决胜
			//（登记顺序更晚），结果对并发确定。
			targetTime := in.TargetTime.UTC()
			for _, sn := range tx.ListSnapshotsByDatasetAny(in.DatasetID) {
				if sn.Status != StatusCompleted || sn.CreatedAt.After(targetTime) {
					continue
				}
				// 压缩产物不是新的逻辑时间点（内容等价于段尾，段尾本身仍可被选中）。
				if tx.IsCompactionProduct(sn.ID) {
					continue
				}
				if target == nil || sn.CreatedAt.After(target.CreatedAt) ||
					(sn.CreatedAt.Equal(target.CreatedAt) && sn.ID > target.ID) {
					target = sn
				}
			}
			if target == nil {
				return classified(ErrCodeNotFound,
					"no completed snapshot in dataset %s at or before %s", in.DatasetID, targetTime.Format(time.RFC3339Nano))
			}
			var err error
			logical, physical, err = resolvePointInTimePlan(tx, target, targetTime)
			if err != nil {
				return err
			}
		} else {
			var ok bool
			target, ok = tx.GetSnapshot(in.TargetSnapshotID)
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
			logical = make([]FrozenSnapshot, len(chainSnap))
			physical = make([]FrozenSnapshot, len(chainSnap))
			for i, sn := range chainSnap {
				f := FrozenSnapshot{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Kind: sn.Kind, OriginSnapshotID: sn.ID}
				logical[i] = f
				physical[i] = f
			}
		}

		if existing, ok := tx.FindActiveTaskByEnv(in.TargetEnvironment); ok {
			return classified(ErrCodeConflict, "target environment %s already has active restore %s (lease %s epoch %d)",
				in.TargetEnvironment, existing.ID, existing.LeaseID, existing.LeaseEpoch)
		}

		now := s.timeNow()
		steps := make([]RestoreStep, len(physical))
		for i, f := range physical {
			steps[i] = RestoreStep{Index: i, SnapshotID: f.SnapshotID, Digest: f.Digest, Status: StepPending}
		}
		var targetTime *time.Time
		if pitMode {
			t := in.TargetTime.UTC()
			targetTime = &t
		}
		leaseID := tx.NewID("lease")
		task := RestoreTask{
			ID:                tx.NewID("task"),
			DatasetID:         target.DatasetID,
			TargetEnvironment: in.TargetEnvironment,
			TargetSnapshotID:  target.ID,
			TargetTime:        targetTime,
			LogicalChain:      logical,
			Chain:             physical,
			Steps:             steps,
			Status:            TaskPending,
			LeaseID:           leaseID,
			LeaseEpoch:        1,
			LeaseHolder:       in.Holder,
			FailureStepIndex:  -1,
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

// resolvePointInTimePlan 从逻辑目标快照出发构造时间点恢复计划，
// 返回“根 -> 目标”顺序的：
//   - 逻辑链 logical：选择时刻看到的链（原始完整快照 + 增量）。
//     已被保留清理回收的节点仍可从归档表与压缩任务冻结视图中复原其 ID/摘要/父边；
//   - 物理执行链 physical：在逻辑链上反复应用已发布压缩的整段替代
//     （与 GetEffectiveChain 同一套“冻结段精确匹配”规则，支持链式压缩），
//     是实际要读取/应用的快照；物理节点必须仍然存在，否则计划失败而非断裂。
//
// 压缩产物在内容上等价于被压缩段（发布时 ExpectedDigest 校验），因此即使其
// 发布时间晚于目标时间，用于恢复目标时刻也是等价的；计划一旦在本事务冻结，
// 之后新的压缩发布或保留回收都不再影响它。
func resolvePointInTimePlan(tx *Tx, target *Snapshot, targetTime time.Time) (logical, physical []FrozenSnapshot, err error) {
	type node struct {
		id, digest string
		kind       SnapshotKind
	}

	// 1) 沿父边回溯构造逻辑链（root -> target），允许读取已归档（回收）记录。
	rev := make([]node, 0)
	curID := target.ID
	datasetID := target.DatasetID
	seen := map[string]bool{}
	for {
		if seen[curID] {
			return nil, nil, classified(ErrCodeConflict, "cycle detected while resolving restore plan at %s", curID)
		}
		seen[curID] = true
		cur, ok := tx.GetSnapshotAny(curID)
		if !ok {
			return nil, nil, classified(ErrCodeConflict,
				"snapshot chain is broken: %s is missing without a compaction replacement", curID)
		}
		if cur.DatasetID != datasetID {
			return nil, nil, classified(ErrCodeConflict, "snapshot %s belongs to another dataset", cur.ID)
		}
		if cur.Status != StatusCompleted {
			return nil, nil, classified(ErrCodeConflict,
				"snapshot %s on chain is %s, only completed chains can be restored", cur.ID, cur.Status)
		}
		rev = append(rev, node{cur.ID, cur.Digest, cur.Kind})
		if cur.Kind == KindFull {
			if cur.ParentID != "" {
				return nil, nil, classified(ErrCodeConflict, "full snapshot %s must not have a parent", cur.ID)
			}
			break
		}
		if cur.ParentID == "" {
			return nil, nil, classified(ErrCodeConflict, "incremental snapshot %s has no parent", cur.ID)
		}
		curID = cur.ParentID
	}
	logicalNodes := make([]node, len(rev))
	for i, n := range rev {
		logicalNodes[len(rev)-1-i] = n
	}

	// 2) 在逻辑链上反复折叠已发布压缩段，规则与 GetEffectiveChain 一致：
	//    替代任务的冻结链必须与当前链的一个连续段逐 ID 精确匹配。
	//    每折一次链严格变短，循环必然终止。
	type physNode struct {
		node
		originTip string // 该物理节点覆盖到的逻辑链末端
		viaTaskID string // 非空表示来自压缩替代
	}
	cur := make([]physNode, len(logicalNodes))
	for i, n := range logicalNodes {
		cur[i] = physNode{node: n, originTip: n.id}
	}
	for {
		applied := false
		for i := 0; i < len(cur); i++ {
			rep, replaced := tx.GetReplacement(cur[i].id)
			if !replaced {
				continue
			}
			comp, ok := tx.GetCompaction(rep.CompactionTaskID)
			if !ok {
				return nil, nil, classified(ErrCodeConflict,
					"compaction %s behind replacement of %s is missing", rep.CompactionTaskID, cur[i].id)
			}
			seg := comp.Chain
			if i+len(seg) > len(cur) {
				continue
			}
			match := true
			for j, f := range seg {
				if cur[i+j].id != f.SnapshotID {
					match = false
					break
				}
			}
			if !match {
				continue
			}
			// 替代快照必须是仍可读取的物理实体（活跃表），且为同数据集已完成完整快照。
			fullSnap, ok := tx.GetSnapshot(rep.NewSnapshotID)
			if !ok {
				return nil, nil, classified(ErrCodeConflict,
					"replacement full snapshot %s (compaction %s) has been physically reclaimed",
					rep.NewSnapshotID, comp.ID)
			}
			if fullSnap.Kind != KindFull || fullSnap.Status != StatusCompleted || fullSnap.DatasetID != datasetID {
				return nil, nil, classified(ErrCodeConflict,
					"replacement snapshot %s is not a completed full of dataset %s", fullSnap.ID, datasetID)
			}
			if fullSnap.Digest != comp.ExpectedDigest {
				return nil, nil, classified(ErrCodeConflict,
					"replacement snapshot %s digest does not match frozen compaction %s", fullSnap.ID, comp.ID)
			}
			next := make([]physNode, 0, len(cur)-len(seg)+1)
			next = append(next, cur[:i]...)
			next = append(next, physNode{
				node:      node{fullSnap.ID, fullSnap.Digest, KindFull},
				originTip: cur[i+len(seg)-1].originTip,
				viaTaskID: comp.ID,
			})
			next = append(next, cur[i+len(seg):]...)
			cur = next
			applied = true
			break
		}
		if !applied {
			break
		}
	}

	logical = make([]FrozenSnapshot, len(logicalNodes))
	for i, n := range logicalNodes {
		logical[i] = FrozenSnapshot{Index: i, SnapshotID: n.id, Digest: n.digest, Kind: n.kind, OriginSnapshotID: n.id}
	}
	physical = make([]FrozenSnapshot, len(cur))
	for i, p := range cur {
		physical[i] = FrozenSnapshot{
			Index: i, SnapshotID: p.id, Digest: p.digest, Kind: p.kind,
			OriginSnapshotID: p.originTip, ViaCompactionTaskID: p.viaTaskID,
		}
	}
	// 3) 物理链上的每个节点必须仍是可读取的实体。目标时刻若落在已回收压缩段
	//    的中间（只能由整段的替代完整快照表示段尾状态），属于超出可恢复窗口，
	//    明确报冲突而不是生成断裂/语义错误的计划。
	for _, p := range physical {
		sn, ok := tx.GetSnapshot(p.SnapshotID)
		if !ok {
			return nil, nil, classified(ErrCodeConflict,
				"physical snapshot %s for target %s has been reclaimed and no matching compaction covers the target point",
				p.SnapshotID, target.ID)
		}
		if sn.Digest != p.Digest {
			return nil, nil, classified(ErrCodeConflict,
				"physical snapshot %s digest changed since plan resolution", p.SnapshotID)
		}
	}
	return logical, physical, nil
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
// 旧租约持有者（含被接管后仍持有旧 epoch 的执行者、取消后拿着换发前租约的
// 执行者）一律先得到 lease 错误；租约正确但任务已终态则报 conflict。
func requireLease(task *RestoreTask, leaseID string, epoch int64) error {
	if task.LeaseID != leaseID || task.LeaseEpoch != epoch {
		return classified(ErrCodeLease, "lease mismatch for task %s: current=%s epoch=%d, got=%s epoch=%d",
			task.ID, task.LeaseID, task.LeaseEpoch, leaseID, epoch)
	}
	if !task.Active() {
		return classified(ErrCodeConflict, "restore task %s is %s", task.ID, task.Status)
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
			// 越过失败位置后清除失败标记；后续步骤若再失败会重新记录。
			if task.FailureStepIndex == in.StepIndex {
				task.FailureStepIndex = -1
				task.FailureDetail = ""
			}
		} else {
			step.Status = StepFailed
			task.FailureStepIndex = in.StepIndex
			task.FailureDetail = in.Detail
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

// FailureLocation 描述恢复计划的失败位置，用于中断排查。
type FailureLocation struct {
	TaskID     string
	Index      int // 失败步骤在物理执行链上的位置
	SnapshotID string
	Detail     string
}

// HasFailure 表示当前是否有失败在等重试。
func (l FailureLocation) HasFailure() bool { return l.Index >= 0 }

// GetFailureLocation 查询任务最近一次失败回执的位置；没有失败时 HasFailure 为 false。
func (s *Service) GetFailureLocation(ctx context.Context, taskID string) (*FailureLocation, error) {
	var out *FailureLocation
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetTask(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", taskID)
		}
		loc := &FailureLocation{TaskID: task.ID, Index: task.FailureStepIndex, Detail: task.FailureDetail}
		if loc.HasFailure() && loc.Index < len(task.Steps) {
			loc.SnapshotID = task.Steps[loc.Index].SnapshotID
		}
		out = loc
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
			if task.Status == TaskCancelled {
				// 取消与完成竞争：取消先生效，完成不能再翻转终态。
				return classified(ErrCodeConflict, "restore task %s is cancelled", task.ID)
			}
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
				// 接管意味着上一位持有者的在途尝试已中断：记录为失败位置，
				// 进程重启/接管后可直接查询到从哪里继续。
				if task.FailureStepIndex < 0 {
					task.FailureStepIndex = i
					task.FailureDetail = "reset by lease takeover"
				}
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

// CancelRestoreInput 取消恢复任务的入参。
type CancelRestoreInput struct {
	TaskID string
	Reason string
	By     string
}

// CancelRestore 请求取消一个仍在执行的恢复任务。取消与执行并发时只会留下
// 一个状态：本操作在单个事务内把任务置为终态 cancelled，同时换发租约
// （epoch + 1、租约 ID 更换），使正在派发/回执的执行者立刻被 fencing：
//   - 取消先于执行落库：执行者后续的 DispatchNextStep/AckStep/CompleteRestore
//     因租约不匹配或任务已终态被拒绝（lease/conflict），不会再推进任何步骤；
//   - 执行先于取消落库（任务刚好 succeeded）：取消返回 conflict，状态保持 succeeded；
//   - 重复取消以及取消已成功的任务：返回 conflict，终态不再翻转。
//
// 取消后冻结的物理链不再受保护，由保留清理按普通规则回收。
func (s *Service) CancelRestore(ctx context.Context, in CancelRestoreInput) (*RestoreTask, error) {
	if in.TaskID == "" {
		return nil, classified(ErrCodeInvalidArgument, "task id is required")
	}
	var out *RestoreTask
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(in.TaskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", in.TaskID)
		}
		if !task.Active() {
			return classified(ErrCodeConflict, "restore task %s is already %s", task.ID, task.Status)
		}
		now := s.timeNow()
		// 换发租约：epoch 单调递增，旧持有者的一切在途操作立即失效。
		task.LeaseID = tx.NewID("lease")
		task.LeaseEpoch++
		if in.By != "" {
			task.LeaseHolder = in.By
		}
		// 在途（running）步骤回到 failed，保证没有任何步骤停留在 running；
		// 任务已是终态，这些步骤不会再被派发。
		for i := range task.Steps {
			step := &task.Steps[i]
			if step.Status == StepRunning {
				step.Status = StepFailed
				step.ExecutionVersion++
				if step.LastDetail == "" {
					step.LastDetail = "reset by cancellation"
				}
				t := now
				step.UpdatedAt = &t
			}
		}
		task.Status = TaskCancelled
		task.CancelledAt = &now
		task.CancelReason = in.Reason
		task.CancelledBy = in.By
		tx.PutTask(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: task.LeaseHolder, Action: LeaseCancelled, Reason: in.Reason,
		})
		out = cloneTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListLeaseEvents 返回租约获取/接管/释放/取消的审计记录；taskID 为空时返回全部。
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

		// (1) 有效恢复链冻结的内容一律保留。
		for _, task := range tx.ListTasks() {
			if !task.Active() {
				continue
			}
			for _, f := range task.Chain {
				addReason(f.SnapshotID, ReasonActiveRestore)
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

// GetEffectiveChain 返回目标快照的“新链”视图：在原始回溯链上应用已发布的
// 压缩替代（可链式：被替代的完整快照自身也可能被更新的压缩替代）。
// 未发布任何相关压缩时与 GetBackupChain 一致。
func (s *Service) GetEffectiveChain(ctx context.Context, targetSnapshotID string) ([]*Snapshot, error) {
	var out []*Snapshot
	err := s.store.View(func(tx *Tx) error {
		if _, ok := tx.GetSnapshot(targetSnapshotID); !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", targetSnapshotID)
		}
		chain, err := walkChain(tx, targetSnapshotID)
		if err != nil {
			return err
		}
		// 反复应用替代段，直到没有可应用的压缩；每次应用都缩短链，保证终止。
		for {
			applied := false
			for i, sn := range chain {
				rep, ok := tx.GetReplacement(sn.ID)
				if !ok {
					continue
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
			for _, f := range task.Chain {
				if f.SnapshotID == snapshotID {
					res.Blocks = append(res.Blocks, ReferenceBlock{
						Kind: "restore_task", ID: task.ID,
						Detail: fmt.Sprintf("frozen by active restore to %s", task.TargetEnvironment),
					})
					break
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
