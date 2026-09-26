package backuprestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// ---------------------------------------------------------------------------
// 增量备份链压缩：把一段已完成的快照链合成为新的完整快照
// ---------------------------------------------------------------------------
//
// 生命周期：
//  1. CreateCompaction 在单个事务内校验链连续且全部完成，冻结起点、终点、
//     链上快照摘要与数据集版本，生成确定性的 ExpectedDigest 并获取执行租约；
//  2. DispatchCompactionStep / AckCompactionStep 沿冻结链有序执行，失败可重试；
//     租约被 TakeoverCompactionLease 接管后，旧租约的回执一律失效；
//  3. PublishCompaction 校验内容摘要后，在同一事务内原子创建新的完整快照、
//     记录每个原快照的替代关系并完成任务——发布前读者只看到原链，
//     同一链的并发压缩最多一个发布成功；
//  4. 发布后原链不立即删除，由 RunRetention 在无任何引用时逐个回收。

// compactionDigest 由冻结链确定性地推导新完整快照应有的内容摘要。
// 相同数据集、相同链摘要序列必然得到相同结果，发布时据此校验执行方
// 实际产出的摘要，不一致即拒绝发布。
func compactionDigest(datasetID string, chain []FrozenSnapshot) string {
	h := sha256.New()
	h.Write([]byte("compaction\x00"))
	h.Write([]byte(datasetID))
	for _, f := range chain {
		h.Write([]byte{0})
		h.Write([]byte(f.SnapshotID))
		h.Write([]byte{0})
		h.Write([]byte(f.Digest))
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// CreateCompactionInput 创建压缩任务的入参。
type CreateCompactionInput struct {
	// IdempotencyKey 是任务号：相同键的重复创建幂等返回同一任务，
	// 调用方可用 “数据集+起点+终点+尝试批次” 之类的稳定标识。
	IdempotencyKey  string
	StartSnapshotID string // 链起点，必须是完整快照
	EndSnapshotID   string // 链终点，起点必须是其祖先
	Holder          string // 申请执行租约的执行方标识
}

// CreateCompaction 在单个事务内创建压缩任务：
//  1. 校验起点为完整快照、终点可达起点、链连续且全部已完成；
//  2. 冻结起点、终点、链上每个快照的 ID 与摘要、当前数据集版本；
//  3. 计算期望内容摘要并获取执行租约（epoch 从 1 开始）。
//
// IdempotencyKey 非空时，相同键的重复调用直接返回既有任务（含已完成任务）。
func (s *Service) CreateCompaction(ctx context.Context, in CreateCompactionInput) (*CompactionTask, error) {
	if in.StartSnapshotID == "" || in.EndSnapshotID == "" || in.Holder == "" {
		return nil, classified(ErrCodeInvalidArgument, "start snapshot id, end snapshot id and holder are required")
	}
	var out *CompactionTask
	err := s.store.Update(func(tx *Tx) error {
		if in.IdempotencyKey != "" {
			if existing, ok := tx.GetCompactionByKey(in.IdempotencyKey); ok {
				out = cloneCompaction(existing)
				return nil
			}
		}
		start, ok := tx.GetSnapshot(in.StartSnapshotID)
		if !ok {
			return classified(ErrCodeNotFound, "start snapshot %s not found", in.StartSnapshotID)
		}
		if start.Kind != KindFull {
			return classified(ErrCodeInvalidArgument,
				"start snapshot %s is %s, compaction must start at a full snapshot", start.ID, start.Kind)
		}
		end, ok := tx.GetSnapshot(in.EndSnapshotID)
		if !ok {
			return classified(ErrCodeNotFound, "end snapshot %s not found", in.EndSnapshotID)
		}
		if end.DatasetID != start.DatasetID {
			return classified(ErrCodeInvalidArgument, "start and end snapshots belong to different datasets")
		}
		// 从终点沿父边回溯到起点，得到连续段；走不到起点即不在同一链上。
		segment, err := walkSegment(tx, start.ID, end.ID)
		if err != nil {
			return err
		}
		for _, sn := range segment {
			if sn.Status != StatusCompleted {
				return classified(ErrCodeConflict, "snapshot %s on chain is %s, only completed chains can be compacted",
					sn.ID, sn.Status)
			}
		}

		now := s.timeNow()
		frozen := make([]FrozenSnapshot, len(segment))
		steps := make([]CompactionStep, len(segment))
		for i, sn := range segment {
			frozen[i] = FrozenSnapshot{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Kind: sn.Kind}
			steps[i] = CompactionStep{Index: i, SnapshotID: sn.ID, Digest: sn.Digest, Status: StepPending}
		}
		leaseID := tx.NewID("lease")
		task := CompactionTask{
			ID:              tx.NewID("cmp"),
			IdempotencyKey:  in.IdempotencyKey,
			DatasetID:       start.DatasetID,
			DatasetVersion:  tx.DatasetVersionNow(start.DatasetID),
			StartSnapshotID: start.ID,
			EndSnapshotID:   end.ID,
			Chain:           frozen,
			ExpectedDigest:  compactionDigest(start.DatasetID, frozen),
			Steps:           steps,
			Status:          TaskPending,
			LeaseID:         leaseID,
			LeaseEpoch:      1,
			LeaseHolder:     in.Holder,
			CreatedAt:       now,
		}
		tx.PutCompaction(task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: leaseID, Epoch: 1,
			Holder: in.Holder, Action: LeaseAcquired,
		})
		out = cloneCompaction(&task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// walkSegment 从 end 沿父边回溯直到 start，返回 start -> end 的有序切片。
// 父边缺失、跨数据集、出现环或根本走不到 start 都属于链不连续。
func walkSegment(tx *Tx, startID, endID string) ([]*Snapshot, error) {
	rev := make([]*Snapshot, 0)
	visited := map[string]bool{}
	curID := endID
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
		if cur.ID == startID {
			break
		}
		if cur.Kind == KindFull || cur.ParentID == "" {
			return nil, classified(ErrCodeConflict,
				"snapshot %s is not on the chain from %s: reached %s without passing it",
				startID, endID, cur.ID)
		}
		curID = cur.ParentID
	}
	out := make([]*Snapshot, len(rev))
	for i, sn := range rev {
		out[len(rev)-1-i] = sn
	}
	return out, nil
}

// GetCompactionTask 查询压缩任务（含冻结链、步骤、租约与发布结果）。
func (s *Service) GetCompactionTask(ctx context.Context, taskID string) (*CompactionTask, error) {
	var out *CompactionTask
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		out = cloneCompaction(task)
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
			out = append(out, cloneCompaction(t))
		}
		return nil
	})
	return out, err
}

// GetCompactionProgress 汇总压缩进度：各状态步骤数、累计尝试次数与发布结果。
func (s *Service) GetCompactionProgress(ctx context.Context, taskID string) (*CompactionProgress, error) {
	var out *CompactionProgress
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		p := &CompactionProgress{
			TaskID: task.ID, Status: task.Status,
			TotalSteps: len(task.Steps), NewSnapshotID: task.NewSnapshotID,
		}
		for _, st := range task.Steps {
			p.Attempts += st.Attempts
			switch st.Status {
			case StepSucceeded:
				p.SucceededSteps++
			case StepRunning:
				p.RunningSteps++
			case StepFailed:
				p.FailedSteps++
			case StepPending:
				p.PendingSteps++
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

// GetCompactionChains 返回任务的新旧链：Old 为创建时冻结的原链，
// New 为发布后新完整快照所在的备份链（发布前为空切片）。
func (s *Service) GetCompactionChains(ctx context.Context, taskID string) (*CompactionChains, error) {
	out := &CompactionChains{New: []*Snapshot{}}
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		out.Old = append([]FrozenSnapshot(nil), task.Chain...)
		if task.NewSnapshotID != "" {
			chain, err := walkChain(tx, task.NewSnapshotID)
			if err != nil {
				return err
			}
			for _, sn := range chain {
				out.New = append(out.New, cloneSnapshot(sn))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetReplacement 返回原快照被压缩替代后的新完整快照 ID；未替代时 ok=false。
func (s *Service) GetReplacement(ctx context.Context, oldSnapshotID string) (string, bool, error) {
	var id string
	var found bool
	err := s.store.View(func(tx *Tx) error {
		id, found = tx.GetReplacement(oldSnapshotID)
		return nil
	})
	return id, found, err
}

// ExplainSnapshotReferences 列出快照当前的活动引用（阻断回收的原因）：
// 冻结链包含它的有效恢复任务、有效压缩任务，以及引用它的未完成子快照。
func (s *Service) ExplainSnapshotReferences(ctx context.Context, snapshotID string) (*SnapshotReferences, error) {
	out := &SnapshotReferences{SnapshotID: snapshotID}
	err := s.store.View(func(tx *Tx) error {
		if _, ok := tx.GetSnapshot(snapshotID); !ok {
			return classified(ErrCodeNotFound, "snapshot %s not found", snapshotID)
		}
		for _, task := range tx.ListTasks() {
			if !task.Active() {
				continue
			}
			for _, f := range task.Chain {
				if f.SnapshotID == snapshotID {
					out.ActiveRestoreTaskIDs = append(out.ActiveRestoreTaskIDs, task.ID)
					break
				}
			}
		}
		for _, task := range tx.ListCompactions() {
			if !task.Active() {
				continue
			}
			for _, f := range task.Chain {
				if f.SnapshotID == snapshotID {
					out.ActiveCompactionIDs = append(out.ActiveCompactionIDs, task.ID)
					break
				}
			}
		}
		for _, child := range tx.ListChildren(snapshotID) {
			if child.Status == StatusPending {
				out.PendingChildIDs = append(out.PendingChildIDs, child.ID)
			}
		}
		sort.Strings(out.ActiveRestoreTaskIDs)
		sort.Strings(out.ActiveCompactionIDs)
		sort.Strings(out.PendingChildIDs)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// requireCompactionLease 在事务内校验压缩任务有效且租约 ID + epoch 完全匹配。
func requireCompactionLease(task *CompactionTask, leaseID string, epoch int64) error {
	if !task.Active() {
		return classified(ErrCodeConflict, "compaction task %s is %s", task.ID, task.Status)
	}
	if task.LeaseID != leaseID || task.LeaseEpoch != epoch {
		return classified(ErrCodeLease, "lease mismatch for compaction task %s: current=%s epoch=%d, got=%s epoch=%d",
			task.ID, task.LeaseID, task.LeaseEpoch, leaseID, epoch)
	}
	return nil
}

// CompactionDispatch 是一次压缩步骤派发的结果。Step 为 nil 表示当前没有
// 可派发的步骤（上一步仍在执行，或全部完成，应调用 PublishCompaction）。
type CompactionDispatch struct {
	Task *CompactionTask
	Step *CompactionStep
}

// DispatchCompactionStep 沿冻结链有序派发下一个合并步骤，语义与恢复步骤
// 派发一致：只派发第一个未成功且不在途的步骤，每次派发递增 ExecutionVersion。
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
				res.Task = cloneCompaction(task)
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
			res.Task = cloneCompaction(task)
			cp := *step
			res.Step = &cp
			return nil
		}
		res.Task = cloneCompaction(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// AckCompactionStep 处理压缩步骤回执：租约 ID/epoch 或执行版本不匹配的
// 回执（旧租约的迟到回执、重试后旧尝试的回执）一律被拒绝且状态不变；
// 失败回执使步骤回到 failed 等待按序重试。
func (s *Service) AckCompactionStep(ctx context.Context, in AckStepInput) (*CompactionStep, error) {
	var out *CompactionStep
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
				"stale receipt for compaction task %s step %d: current execution version %d, receipt %d",
				task.ID, step.Index, step.ExecutionVersion, in.ExecutionVersion)
		}
		if step.Status != StepRunning {
			return classified(ErrCodeConflict, "compaction task %s step %d is not running (status=%s)",
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

// TakeoverCompactionLease 接管有效压缩任务的租约：epoch 单调递增并换发
// 租约 ID，在途步骤重置为可重试；旧租约持有者的任何回执因 epoch 不匹配
// 被挡下，无法推进新接管者的任务。
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
			return classified(ErrCodeConflict, "cannot take over lease of %s compaction task %s", task.Status, task.ID)
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
		out = cloneCompaction(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PublishCompactionResult 是发布结果：终态任务、新完整快照与唯一通知。
type PublishCompactionResult struct {
	Task        *CompactionTask
	NewSnapshot *Snapshot
	Event       *OutboxEvent
}

// PublishCompaction 在全部步骤成功后原子发布新的完整快照：
//   - digest 必须等于创建时冻结链推导出的期望摘要，否则拒绝发布；
//   - 冻结链上的快照必须仍然存在且摘要未变（执行期间被回收/篡改则拒绝）；
//   - 同一链已被其他压缩任务发布过时拒绝（并发压缩最多一个成为有效结果）；
//   - 通过校验后在同一事务内：创建已完成的新完整快照、记录每个原快照到
//     新快照的替代关系、写出唯一一条 outbox 通知并释放租约。
//
// 发布前读者只看到原链；新快照直接以 completed 状态出现，不存在半成品。
// 对已完成任务重复调用是幂等的：返回既有结果，不再写 outbox。
func (s *Service) PublishCompaction(ctx context.Context, taskID, leaseID string, epoch int64, digest string) (*PublishCompactionResult, error) {
	res := &PublishCompactionResult{}
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetCompaction(taskID)
		if !ok {
			return classified(ErrCodeNotFound, "compaction task %s not found", taskID)
		}
		if !task.Active() {
			// 幂等：任务已发布，复用既有结果，绝不产生第二条通知。
			if task.NewSnapshotID != "" {
				if snap, ok := tx.GetSnapshot(task.NewSnapshotID); ok {
					res.NewSnapshot = cloneSnapshot(snap)
				}
			}
			if event, ok := tx.GetOutboxByTask(task.ID); ok {
				cp := *event
				res.Event = &cp
			}
			res.Task = cloneCompaction(task)
			return nil
		}
		if err := requireCompactionLease(task, leaseID, epoch); err != nil {
			return err
		}
		for _, step := range task.Steps {
			if step.Status != StepSucceeded {
				return classified(ErrCodeConflict, "compaction task %s step %d is %s, cannot publish",
					task.ID, step.Index, step.Status)
			}
		}
		if digest == "" || digest != task.ExpectedDigest {
			return classified(ErrCodeConflict,
				"digest verification failed for compaction task %s: expected %s, got %q",
				task.ID, task.ExpectedDigest, digest)
		}
		// 冻结链完整性：执行期间原链不得被回收或变化。
		for _, f := range task.Chain {
			snap, ok := tx.GetSnapshot(f.SnapshotID)
			if !ok {
				return classified(ErrCodeConflict,
					"chain snapshot %s was reclaimed before publish, compaction %s cannot proceed",
					f.SnapshotID, task.ID)
			}
			if snap.Digest != f.Digest || snap.Status != StatusCompleted {
				return classified(ErrCodeConflict,
					"chain snapshot %s changed since freeze, compaction %s cannot proceed",
					f.SnapshotID, task.ID)
			}
			if _, replaced := tx.GetReplacement(f.SnapshotID); replaced {
				return classified(ErrCodeConflict,
					"chain snapshot %s was already compacted by another task, %s loses the race",
					f.SnapshotID, task.ID)
			}
		}

		now := s.timeNow()
		newSnap := Snapshot{
			ID:          tx.NewID("snap"),
			DatasetID:   task.DatasetID,
			Kind:        KindFull,
			Digest:      digest,
			Status:      StatusCompleted,
			CreatedAt:   now,
			CompletedAt: &now,
		}
		tx.PutSnapshot(newSnap)
		tx.BumpDatasetVersion(task.DatasetID)

		task.Replacements = make(map[string]string, len(task.Chain))
		for _, f := range task.Chain {
			task.Replacements[f.SnapshotID] = newSnap.ID
			tx.PutReplacement(f.SnapshotID, newSnap.ID)
		}
		task.NewSnapshotID = newSnap.ID
		task.Status = TaskSucceeded
		task.CompletedAt = &now
		tx.PutCompaction(*task)
		tx.AddLeaseEvent(LeaseEvent{
			At: now, TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			Holder: task.LeaseHolder, Action: LeaseReleased, Reason: "compaction published",
		})
		event := OutboxEvent{
			ID: tx.NewID("evt"), TaskID: task.ID, Type: OutboxCompactionCompleted,
			DatasetID: task.DatasetID, TargetSnapshotID: newSnap.ID, CreatedAt: now,
		}
		if err := tx.AddOutboxEvent(event); err != nil {
			return err
		}
		res.Task = cloneCompaction(task)
		res.NewSnapshot = cloneSnapshot(&newSnap)
		res.Event = &event
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
