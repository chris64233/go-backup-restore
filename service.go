package backuprestore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Service 提供快照登记、恢复编排与保留清理能力。
// 所有跨实体不变量（链无环、租约唯一、回执栅栏、清理一致性）
// 都在单个 Store 事务内检查并提交。
type Service struct {
	store Store
	now   func() time.Time
}

// NewService 创建服务。store 决定持久化方式（内存或文件）。
func NewService(store Store) *Service {
	return &Service{store: store, now: time.Now}
}

// SetClock 注入时间源，仅用于测试。
func (s *Service) SetClock(now func() time.Time) { s.now = now }

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

// ---------------------------------------------------------------------------
// 快照登记
// ---------------------------------------------------------------------------

// RegisterSnapshotInput 是登记快照的请求。
type RegisterSnapshotInput struct {
	DatasetID string
	Kind      SnapshotKind
	ParentID  string // 增量快照必填；完整快照必须为空
	Digest    string // 不可变内容摘要
}

// RegisterSnapshot 登记一个新快照（初始状态 pending）。
//
// 增量快照只允许挂在同一数据集的已完成祖先之后，且整条父链必须无环；
// 完整快照不允许携带父指针。同一 ID 重复登记时，只有摘要完全一致才幂等返回。
func (s *Service) RegisterSnapshot(ctx context.Context, in RegisterSnapshotInput) (*Snapshot, error) {
	if in.DatasetID == "" || in.Digest == "" {
		return nil, fmt.Errorf("%w: dataset id and digest are required", ErrInvalidInput)
	}
	var out *Snapshot
	err := s.store.Update(ctx, func(st *state) error {
		switch in.Kind {
		case KindFull:
			if in.ParentID != "" {
				return fmt.Errorf("%w: full snapshot must not have a parent", ErrInvalidInput)
			}
		case KindIncremental:
			if in.ParentID == "" {
				return fmt.Errorf("%w: incremental snapshot requires a parent", ErrInvalidInput)
			}
			parent, ok := st.Snapshots[in.ParentID]
			if !ok || parent.Deleted {
				return fmt.Errorf("%w: parent snapshot %q", ErrNotFound, in.ParentID)
			}
			if parent.DatasetID != in.DatasetID {
				return fmt.Errorf("%w: parent %q belongs to dataset %q", ErrCrossDataset, in.ParentID, parent.DatasetID)
			}
			if parent.Status == SnapshotFailed {
				return fmt.Errorf("%w: parent %q", ErrSnapshotFailed, in.ParentID)
			}
			if parent.Status != SnapshotCompleted {
				return fmt.Errorf("%w: parent %q is %s", ErrSnapshotNotCompleted, in.ParentID, parent.Status)
			}
			// 父链必须无环且能回溯到完整快照。
			if _, err := chainLocked(st, in.ParentID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unknown snapshot kind %q", ErrInvalidInput, in.Kind)
		}

		st.SnapshotSeq[in.DatasetID]++
		snap := &Snapshot{
			ID:        newID(),
			DatasetID: in.DatasetID,
			Kind:      in.Kind,
			ParentID:  in.ParentID,
			Digest:    in.Digest,
			Status:    SnapshotPending,
			Sequence:  st.SnapshotSeq[in.DatasetID],
			CreatedAt: s.now(),
		}
		st.Snapshots[snap.ID] = snap
		out = copySnapshot(snap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CompleteSnapshot 将 pending 快照标记为已完成，此后摘要与父指针完全不可变。
// 重复完成同一快照是幂等的，但完成时携带的摘要必须与登记时一致。
func (s *Service) CompleteSnapshot(ctx context.Context, id, digest string) (*Snapshot, error) {
	var out *Snapshot
	err := s.store.Update(ctx, func(st *state) error {
		snap, ok := st.Snapshots[id]
		if !ok || snap.Deleted {
			return fmt.Errorf("%w: snapshot %q", ErrNotFound, id)
		}
		if digest != "" && digest != snap.Digest {
			return fmt.Errorf("%w: snapshot %q registered with a different digest", ErrDigestConflict, id)
		}
		switch snap.Status {
		case SnapshotCompleted:
			// 幂等。
		case SnapshotPending:
			snap.Status = SnapshotCompleted
			snap.CompletedAt = s.now()
		case SnapshotFailed:
			return fmt.Errorf("%w: snapshot %q already failed", ErrInvalidTransition, id)
		}
		out = copySnapshot(snap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FailSnapshot 将 pending 快照标记为失败。失败快照不能恢复、不能作为父，
// 且会被保留清理回收。重复失败是幂等的。
func (s *Service) FailSnapshot(ctx context.Context, id, reason string) (*Snapshot, error) {
	var out *Snapshot
	err := s.store.Update(ctx, func(st *state) error {
		snap, ok := st.Snapshots[id]
		if !ok || snap.Deleted {
			return fmt.Errorf("%w: snapshot %q", ErrNotFound, id)
		}
		switch snap.Status {
		case SnapshotFailed:
			// 幂等。
		case SnapshotPending:
			snap.Status = SnapshotFailed
			snap.FailReason = reason
		case SnapshotCompleted:
			return fmt.Errorf("%w: snapshot %q already completed", ErrInvalidTransition, id)
		}
		out = copySnapshot(snap)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 恢复任务
// ---------------------------------------------------------------------------

// CreateRestore 为 targetEnv 创建到 targetSnapshotID 的恢复任务：
// 在同一事务内冻结从目标回溯到完整快照的链，并取得目标环境的恢复租约。
// 目标环境已存在有效恢复时返回 ErrLeaseConflict。
func (s *Service) CreateRestore(ctx context.Context, targetEnv, targetSnapshotID string) (*RestoreJob, error) {
	if targetEnv == "" || targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: target env and snapshot id are required", ErrInvalidInput)
	}
	var out *RestoreJob
	err := s.store.Update(ctx, func(st *state) error {
		if _, _, ok := activeLeaseLocked(st, targetEnv); ok {
			return fmt.Errorf("%w: target env %q already has an active restore", ErrLeaseConflict, targetEnv)
		}
		job, err := s.createRestoreLocked(st, targetEnv, targetSnapshotID)
		if err != nil {
			return err
		}
		out = copyJob(job)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TakeoverRestore 强制接管 targetEnv 上的恢复租约：旧任务被标记为 superseded，
// 其租约作废（栅栏令牌递增），随后为新目标快照创建恢复任务。
// 旧租约 holder 之后提交的任何回执都会以 ErrStaleLease / ErrJobFinished 被拒绝。
func (s *Service) TakeoverRestore(ctx context.Context, targetEnv, targetSnapshotID string) (*RestoreJob, error) {
	if targetEnv == "" || targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: target env and snapshot id are required", ErrInvalidInput)
	}
	var out *RestoreJob
	err := s.store.Update(ctx, func(st *state) error {
		if lease, oldJob, ok := activeLeaseLocked(st, targetEnv); ok {
			oldJob.Status = RestoreSuperseded
			oldJob.FinishedAt = s.now()
			delete(st.Leases, lease.TargetEnv)
		}
		job, err := s.createRestoreLocked(st, targetEnv, targetSnapshotID)
		if err != nil {
			return err
		}
		out = copyJob(job)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// CancelRestore 由当前租约持有者取消恢复任务并释放租约。
func (s *Service) CancelRestore(ctx context.Context, jobID, leaseID string, fencing uint64) error {
	return s.store.Update(ctx, func(st *state) error {
		job, err := s.authorizeLocked(st, jobID, leaseID, fencing)
		if err != nil {
			return err
		}
		job.Status = RestoreCanceled
		job.FinishedAt = s.now()
		delete(st.Leases, job.TargetEnv)
		return nil
	})
}

// StepReceipt 是一次步骤回执。
type StepReceipt struct {
	JobID            string
	LeaseID          string
	Fencing          uint64
	StepIndex        int
	ExecutionVersion uint64
	Success          bool
	Error            string // 失败原因（Success=false 时记录）
}

// ReportStep 处理步骤回执。回执必须匹配当前租约、栅栏令牌与步骤执行版本；
// 步骤沿冻结链严格按序推进。全部步骤成功时任务进入终态，并在同一事务内
// 写出恰好一条 outbox 通知。旧租约的迟到回执以 ErrStaleLease 拒绝，
// 不会干扰接管者。
func (s *Service) ReportStep(ctx context.Context, r StepReceipt) error {
	return s.store.Update(ctx, func(st *state) error {
		job, err := s.authorizeLocked(st, r.JobID, r.LeaseID, r.Fencing)
		if err != nil {
			return err
		}
		cur := currentStepLocked(job)
		if cur == nil {
			return fmt.Errorf("%w: job %q has no pending step", ErrJobFinished, job.ID)
		}
		if r.StepIndex != cur.Index {
			return fmt.Errorf("%w: expected step %d, got %d", ErrStepOrder, cur.Index, r.StepIndex)
		}
		if r.ExecutionVersion != cur.ExecutionVersion {
			return fmt.Errorf("%w: step %d expects version %d, got %d",
				ErrVersionMismatch, cur.Index, cur.ExecutionVersion, r.ExecutionVersion)
		}
		if cur.Status == StepFailed {
			// 同一执行版本已经记录过失败，结论不可被同版本回执翻案；
			// 必须先 RetryStep 抬升执行版本后重新执行。
			return fmt.Errorf("%w: step %d already failed at version %d, retry required",
				ErrInvalidTransition, cur.Index, cur.ExecutionVersion)
		}
		if r.Success {
			cur.Status = StepSucceeded
			cur.LastError = ""
		} else {
			cur.Status = StepFailed
			cur.LastError = r.Error
		}
		cur.UpdatedAt = s.now()

		if currentStepLocked(job) == nil {
			// 全部步骤成功：任务完成，outbox 通知只写一次。
			job.Status = RestoreSucceeded
			job.FinishedAt = s.now()
			delete(st.Leases, job.TargetEnv)
			if !outboxExistsLocked(st, job.ID) {
				payload, _ := json.Marshal(map[string]string{
					"event":              "restore_succeeded",
					"job_id":             job.ID,
					"target_env":         job.TargetEnv,
					"dataset_id":         job.DatasetID,
					"target_snapshot_id": job.TargetSnapshotID,
				})
				msgID := newID()
				st.Outbox[msgID] = &OutboxMessage{
					ID:               msgID,
					JobID:            job.ID,
					TargetEnv:        job.TargetEnv,
					DatasetID:        job.DatasetID,
					TargetSnapshotID: job.TargetSnapshotID,
					Payload:          string(payload),
					CreatedAt:        s.now(),
				}
			}
		}
		return nil
	})
}

// RetryStep 将失败步骤重新置为待执行并抬升执行版本，使旧版本的迟到回执失效。
// 只有当前租约持有者可以重试，且只有失败步骤可以重试。
func (s *Service) RetryStep(ctx context.Context, jobID, leaseID string, fencing uint64, stepIndex int) (uint64, error) {
	var version uint64
	err := s.store.Update(ctx, func(st *state) error {
		job, err := s.authorizeLocked(st, jobID, leaseID, fencing)
		if err != nil {
			return err
		}
		cur := currentStepLocked(job)
		if cur == nil {
			return fmt.Errorf("%w: job %q has no pending step", ErrJobFinished, job.ID)
		}
		if stepIndex != cur.Index {
			return fmt.Errorf("%w: expected step %d, got %d", ErrStepOrder, cur.Index, stepIndex)
		}
		if cur.Status != StepFailed {
			return fmt.Errorf("%w: step %d is %s, only failed steps can be retried",
				ErrInvalidTransition, cur.Index, cur.Status)
		}
		cur.Status = StepPending
		cur.ExecutionVersion++
		cur.UpdatedAt = s.now()
		version = cur.ExecutionVersion
		return nil
	})
	if err != nil {
		return 0, err
	}
	return version, nil
}

// ---------------------------------------------------------------------------
// 保留清理
// ---------------------------------------------------------------------------

// SetRetentionPolicy 设置数据集的保留策略（保留最近 KeepLatest 个已完成快照及其祖先）。
func (s *Service) SetRetentionPolicy(ctx context.Context, datasetID string, keepLatest int) error {
	if datasetID == "" || keepLatest < 0 {
		return fmt.Errorf("%w: dataset id required and keep_latest must be >= 0", ErrInvalidInput)
	}
	return s.store.Update(ctx, func(st *state) error {
		st.Policies[datasetID] = &RetentionPolicy{
			DatasetID:  datasetID,
			KeepLatest: keepLatest,
			UpdatedAt:  s.now(),
		}
		return nil
	})
}

// RunRetention 对数据集执行一次保留清理。
//
// 整个评估与删除发生在同一个事务里，因此与快照创建、恢复创建并发时，
// 清理决定始终基于一致快照：凡是被有效恢复链、保留策略或未完成子快照
// 引用的内容都不会被删除。每个被评估的快照都会留下一条带原因的清理记录。
func (s *Service) RunRetention(ctx context.Context, datasetID string) (*RetentionReport, error) {
	if datasetID == "" {
		return nil, fmt.Errorf("%w: dataset id is required", ErrInvalidInput)
	}
	report := &RetentionReport{DatasetID: datasetID}
	err := s.store.Update(ctx, func(st *state) error {
		now := s.now()

		// 1. 有效恢复任务冻结链上的所有快照受保护。
		protected := make(map[string]string) // snapshotID -> reason
		for _, job := range st.Jobs {
			if job.Status != RestoreRunning {
				continue
			}
			if _, ok := st.Leases[job.TargetEnv]; !ok {
				continue
			}
			for _, fs := range job.Chain {
				if _, ok := protected[fs.SnapshotID]; !ok {
					protected[fs.SnapshotID] = ReasonActiveRestore
				}
			}
		}

		snaps := datasetSnapshotsLocked(st, datasetID)

		// 2. 保留策略：最近 KeepLatest 个已完成快照及其全部祖先。
		if p, ok := st.Policies[datasetID]; ok && p.KeepLatest > 0 {
			completed := make([]*Snapshot, 0, len(snaps))
			for _, snap := range snaps {
				if snap.Status == SnapshotCompleted {
					completed = append(completed, snap)
				}
			}
			sort.Slice(completed, func(i, j int) bool {
				if completed[i].Sequence != completed[j].Sequence {
					return completed[i].Sequence > completed[j].Sequence
				}
				return completed[i].ID > completed[j].ID
			})
			for i, head := range completed {
				if i >= p.KeepLatest {
					break
				}
				for cur := head; cur != nil; {
					if _, ok := protected[cur.ID]; !ok {
						protected[cur.ID] = ReasonRetentionPolicy
					}
					if cur.ParentID == "" {
						break
					}
					cur = st.Snapshots[cur.ParentID]
				}
			}
		}

		// 3. 未完成快照自身及其全部祖先受保护（不能删除仍被引用的内容）。
		for _, snap := range snaps {
			if snap.Status != SnapshotPending {
				continue
			}
			for cur := snap; cur != nil; {
				if _, ok := protected[cur.ID]; !ok {
					if cur.ID == snap.ID {
						protected[cur.ID] = ReasonIncompleteSnapshot
					} else {
						protected[cur.ID] = ReasonIncompleteChild
					}
				}
				if cur.ParentID == "" {
					break
				}
				cur = st.Snapshots[cur.ParentID]
			}
		}

		// 4. 逐个评估并记录决定；删除只打标记，记录保留用于审计。
		_, hasPolicy := st.Policies[datasetID]
		for _, snap := range snaps {
			rec := &CleanupRecord{
				ID:         newID(),
				DatasetID:  datasetID,
				SnapshotID: snap.ID,
				CreatedAt:  now,
			}
			if reason, ok := protected[snap.ID]; ok {
				rec.Decision = Retain
				rec.Reason = reason
			} else if snap.Status == SnapshotFailed {
				// 失败快照不可能被恢复链或未完成子快照引用，始终安全回收。
				rec.Decision = Delete
				rec.Reason = ReasonFailedSnapshot
				rec.Detail = snap.FailReason
			} else if !hasPolicy {
				// 未配置策略时安全默认保留，避免无授权删除。
				rec.Decision = Retain
				rec.Reason = ReasonNoPolicy
			} else {
				rec.Decision = Delete
				rec.Reason = ReasonPolicyExpired
			}
			if rec.Decision == Delete {
				snap.Deleted = true
				snap.DeleteReason = rec.Reason
				snap.DeletedAt = now
				report.DeletedCount++
			}
			st.CleanupRecords = append(st.CleanupRecords, rec)
			report.Decisions = append(report.Decisions, *rec)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return report, nil
}

// ---------------------------------------------------------------------------
// 查询
// ---------------------------------------------------------------------------

// GetChain 返回目标快照回溯到完整快照的备份链（索引 0 为完整快照）。
func (s *Service) GetChain(ctx context.Context, snapshotID string) ([]Snapshot, error) {
	var out []Snapshot
	err := s.store.View(ctx, func(st *state) error {
		chain, err := chainLocked(st, snapshotID)
		if err != nil {
			return err
		}
		for _, snap := range chain {
			out = append(out, *copySnapshot(snap))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListSnapshots 列出数据集的活动快照（默认不含已删除），按序号升序。
func (s *Service) ListSnapshots(ctx context.Context, datasetID string, includeDeleted bool) ([]Snapshot, error) {
	var out []Snapshot
	err := s.store.View(ctx, func(st *state) error {
		for _, snap := range st.Snapshots {
			if snap.DatasetID != datasetID {
				continue
			}
			if snap.Deleted && !includeDeleted {
				continue
			}
			out = append(out, *copySnapshot(snap))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// GetJob 返回恢复任务当前状态（含冻结链与步骤）。
func (s *Service) GetJob(ctx context.Context, jobID string) (*RestoreJob, error) {
	var out *RestoreJob
	err := s.store.View(ctx, func(st *state) error {
		job, ok := st.Jobs[jobID]
		if !ok {
			return fmt.Errorf("%w: job %q", ErrNotFound, jobID)
		}
		out = copyJob(job)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListCleanupRecords 返回数据集的清理决定记录（按时间升序）。
func (s *Service) ListCleanupRecords(ctx context.Context, datasetID string) ([]CleanupRecord, error) {
	var out []CleanupRecord
	err := s.store.View(ctx, func(st *state) error {
		for _, rec := range st.CleanupRecords {
			if rec.DatasetID == datasetID {
				out = append(out, *rec)
			}
		}
		return nil
	})
	return out, err
}

// ListOutbox 返回全部 outbox 通知（按时间升序）。
func (s *Service) ListOutbox(ctx context.Context) ([]OutboxMessage, error) {
	var out []OutboxMessage
	err := s.store.View(ctx, func(st *state) error {
		for _, msg := range st.Outbox {
			out = append(out, *msg)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// MarkOutboxDelivered 将通知标记为已投递（幂等）。
func (s *Service) MarkOutboxDelivered(ctx context.Context, messageID string) error {
	return s.store.Update(ctx, func(st *state) error {
		msg, ok := st.Outbox[messageID]
		if !ok {
			return fmt.Errorf("%w: outbox message %q", ErrNotFound, messageID)
		}
		if !msg.Delivered {
			msg.Delivered = true
			msg.DeliveredAt = s.now()
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// 内部辅助（均在事务内调用，st 已加锁）
// ---------------------------------------------------------------------------

// chainLocked 从 targetID 回溯到完整快照，校验无环、同数据集、全部已完成。
// 返回的切片索引 0 为完整快照，最后一个元素为目标快照。
func chainLocked(st *state, targetID string) ([]*Snapshot, error) {
	target, ok := st.Snapshots[targetID]
	if !ok || target.Deleted {
		return nil, fmt.Errorf("%w: snapshot %q", ErrNotFound, targetID)
	}
	var rev []*Snapshot
	seen := make(map[string]bool)
	for cur := target; cur != nil; {
		if seen[cur.ID] {
			return nil, fmt.Errorf("%w: snapshot %q reachable twice", ErrChainCycle, cur.ID)
		}
		seen[cur.ID] = true
		if cur.Deleted {
			return nil, fmt.Errorf("%w: snapshot %q has been deleted", ErrNotFound, cur.ID)
		}
		switch cur.Status {
		case SnapshotFailed:
			return nil, fmt.Errorf("%w: snapshot %q", ErrSnapshotFailed, cur.ID)
		case SnapshotPending:
			return nil, fmt.Errorf("%w: snapshot %q is pending", ErrSnapshotNotCompleted, cur.ID)
		}
		rev = append(rev, cur)
		if cur.Kind == KindFull {
			if cur.ParentID != "" {
				return nil, fmt.Errorf("%w: full snapshot %q must not have a parent", ErrInvalidTransition, cur.ID)
			}
			break
		}
		parent, ok := st.Snapshots[cur.ParentID]
		if !ok {
			return nil, fmt.Errorf("%w: parent %q of snapshot %q", ErrNotFound, cur.ParentID, cur.ID)
		}
		if parent.DatasetID != cur.DatasetID {
			return nil, fmt.Errorf("%w: snapshot %q and parent %q", ErrCrossDataset, cur.ID, parent.ID)
		}
		cur = parent
	}
	if len(rev) == 0 || rev[len(rev)-1].Kind != KindFull {
		return nil, fmt.Errorf("%w: chain of snapshot %q does not reach a full snapshot", ErrInvalidTransition, targetID)
	}
	chain := make([]*Snapshot, len(rev))
	for i, snap := range rev {
		chain[len(rev)-1-i] = snap
	}
	return chain, nil
}

// activeLeaseLocked 返回 targetEnv 上当前有效的租约与其任务。
// 租约有效当且仅当租约存在、对应任务仍在运行且任务持有该租约。
func activeLeaseLocked(st *state, targetEnv string) (*Lease, *RestoreJob, bool) {
	lease, ok := st.Leases[targetEnv]
	if !ok {
		return nil, nil, false
	}
	job, ok := st.Jobs[lease.JobID]
	if !ok || job.Status != RestoreRunning || job.LeaseID != lease.ID {
		return nil, nil, false
	}
	return lease, job, true
}

// createRestoreLocked 冻结链并创建任务 + 租约。调用前必须确认目标环境无有效租约。
func (s *Service) createRestoreLocked(st *state, targetEnv, targetSnapshotID string) (*RestoreJob, error) {
	chain, err := chainLocked(st, targetSnapshotID)
	if err != nil {
		return nil, err
	}
	now := s.now()
	job := &RestoreJob{
		ID:               newID(),
		TargetEnv:        targetEnv,
		DatasetID:        chain[0].DatasetID,
		TargetSnapshotID: targetSnapshotID,
		Status:           RestoreRunning,
		CreatedAt:        now,
	}
	for i, snap := range chain {
		job.Chain = append(job.Chain, FrozenSnapshot{
			Index:      i,
			SnapshotID: snap.ID,
			Kind:       snap.Kind,
			Digest:     snap.Digest,
		})
		job.Steps = append(job.Steps, &RestoreStep{
			Index:            i,
			SnapshotID:       snap.ID,
			Status:           StepPending,
			ExecutionVersion: 1,
			UpdatedAt:        now,
		})
	}
	st.FencingSeq[targetEnv]++
	lease := &Lease{
		ID:         newID(),
		TargetEnv:  targetEnv,
		JobID:      job.ID,
		Fencing:    st.FencingSeq[targetEnv],
		AcquiredAt: now,
	}
	job.LeaseID = lease.ID
	job.Fencing = lease.Fencing
	st.Jobs[job.ID] = job
	st.Leases[targetEnv] = lease
	return job, nil
}

// authorizeLocked 校验回执/操作携带的租约与栅栏令牌。
func (s *Service) authorizeLocked(st *state, jobID, leaseID string, fencing uint64) (*RestoreJob, error) {
	job, ok := st.Jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("%w: job %q", ErrNotFound, jobID)
	}
	if job.Status != RestoreRunning {
		return nil, fmt.Errorf("%w: job %q is %s", ErrJobFinished, jobID, job.Status)
	}
	if leaseID != job.LeaseID {
		return nil, fmt.Errorf("%w: job %q holds lease %q", ErrStaleLease, jobID, job.LeaseID)
	}
	if fencing != job.Fencing {
		return nil, fmt.Errorf("%w: job %q holds fencing %d", ErrStaleLease, jobID, job.Fencing)
	}
	return job, nil
}

// currentStepLocked 返回第一个未成功的步骤；全部成功时返回 nil。
func currentStepLocked(job *RestoreJob) *RestoreStep {
	for _, step := range job.Steps {
		if step.Status != StepSucceeded {
			return step
		}
	}
	return nil
}

func outboxExistsLocked(st *state, jobID string) bool {
	for _, msg := range st.Outbox {
		if msg.JobID == jobID {
			return true
		}
	}
	return false
}

func datasetSnapshotsLocked(st *state, datasetID string) []*Snapshot {
	var out []*Snapshot
	for _, snap := range st.Snapshots {
		if snap.DatasetID == datasetID && !snap.Deleted {
			out = append(out, snap)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func copySnapshot(s *Snapshot) *Snapshot {
	cp := *s
	return &cp
}

func copyJob(j *RestoreJob) *RestoreJob {
	cp := *j
	cp.Chain = append([]FrozenSnapshot(nil), j.Chain...)
	cp.Steps = make([]*RestoreStep, len(j.Steps))
	for i, step := range j.Steps {
		sc := *step
		cp.Steps[i] = &sc
	}
	return &cp
}
