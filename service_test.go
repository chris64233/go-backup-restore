package backuprestore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testClock 是可手动推进的时钟，保证测试确定性。
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestService(t *testing.T) (*Service, *testClock) {
	t.Helper()
	clock := newTestClock()
	return NewService(NewMemoryStore()).WithClock(clock.Now), clock
}

func mustCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	if got := CodeOf(err); got != want {
		t.Fatalf("expected error code %s, got %s (err=%v)", want, got, err)
	}
}

// registerChain 登记并完整化一条 full + n 个增量的链，返回快照 ID（根在前）。
func registerChain(t *testing.T, ctx context.Context, svc *Service, dataset string, incrementals int) []string {
	t.Helper()
	ids := []string{}
	full, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: dataset, Kind: KindFull, Digest: dataset + "-full",
	})
	if err != nil {
		t.Fatalf("register full: %v", err)
	}
	if _, err := svc.MarkSnapshotCompleted(ctx, full.ID); err != nil {
		t.Fatalf("complete full: %v", err)
	}
	ids = append(ids, full.ID)
	parent := full.ID
	for i := 0; i < incrementals; i++ {
		inc, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
			DatasetID: dataset, Kind: KindIncremental, ParentID: parent,
			Digest: dataset + "-inc-" + string(rune('a'+i)),
		})
		if err != nil {
			t.Fatalf("register incremental %d: %v", i, err)
		}
		if _, err := svc.MarkSnapshotCompleted(ctx, inc.ID); err != nil {
			t.Fatalf("complete incremental %d: %v", i, err)
		}
		ids = append(ids, inc.ID)
		parent = inc.ID
	}
	return ids
}

// ---------------------------------------------------------------------------
// 快照登记
// ---------------------------------------------------------------------------

func TestRegisterSnapshot_Rules(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	// 完整快照不能声明父快照。
	_, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindFull, ParentID: "x", Digest: "d1",
	})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 增量快照必须有父快照。
	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, Digest: "d2",
	})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 父快照不存在。
	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: "ghost", Digest: "d3",
	})
	mustCode(t, err, ErrCodeNotFound)

	// 正常登记 full，digest 唯一。
	full, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindFull, Digest: "dg-full",
	})
	if err != nil {
		t.Fatalf("register full: %v", err)
	}
	if full.Status != StatusPending {
		t.Fatalf("new snapshot should be pending, got %s", full.Status)
	}
	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindFull, Digest: "dg-full",
	})
	mustCode(t, err, ErrCodeAlreadyExists)

	// 父快照未完成时不能挂增量。
	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: full.ID, Digest: "dg-inc",
	})
	mustCode(t, err, ErrCodeConflict)

	// 父快照失败时也不能挂增量。
	if _, err := svc.MarkSnapshotFailed(ctx, full.ID, "upload broken"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: full.ID, Digest: "dg-inc",
	})
	mustCode(t, err, ErrCodeConflict)

	// 终态不可再次迁移。
	_, err = svc.MarkSnapshotCompleted(ctx, full.ID)
	mustCode(t, err, ErrCodeConflict)

	// 跨数据集父快照被拒绝。
	ids := registerChain(t, ctx, svc, "ds-a", 0)
	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds-b", Kind: KindIncremental, ParentID: ids[0], Digest: "dg-x",
	})
	mustCode(t, err, ErrCodeInvalidArgument)
}

func TestRegisterSnapshot_ChainGrowsOnCompletedAncestors(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 3)
	if len(ids) != 4 {
		t.Fatalf("want 4 snapshots, got %d", len(ids))
	}
	chain, err := svc.GetBackupChain(ctx, ids[3])
	if err != nil {
		t.Fatalf("get chain: %v", err)
	}
	if len(chain) != 4 {
		t.Fatalf("chain length = %d, want 4", len(chain))
	}
	for i, sn := range chain {
		if sn.ID != ids[i] {
			t.Fatalf("chain[%d] = %s, want %s", i, sn.ID, ids[i])
		}
	}
	if chain[0].Kind != KindFull {
		t.Fatalf("chain root should be full, got %s", chain[0].Kind)
	}
	// 未知快照的链查询。
	_, err = svc.GetBackupChain(ctx, "nope")
	mustCode(t, err, ErrCodeNotFound)
}

// ---------------------------------------------------------------------------
// 恢复创建与步骤推进
// ---------------------------------------------------------------------------

func TestCreateRestore_FreezesChainAndAcquiresLease(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)

	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env-1", Holder: "worker-a",
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	if len(task.Chain) != 3 || len(task.Steps) != 3 {
		t.Fatalf("frozen chain/steps length = %d/%d, want 3/3", len(task.Chain), len(task.Steps))
	}
	for i, f := range task.Chain {
		if f.SnapshotID != ids[i] {
			t.Fatalf("frozen chain[%d] = %s, want %s", i, f.SnapshotID, ids[i])
		}
	}
	if task.LeaseEpoch != 1 || task.LeaseID == "" || task.LeaseHolder != "worker-a" {
		t.Fatalf("unexpected lease: %+v", task)
	}

	// 同一目标环境不能再建第二个有效恢复。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[1], TargetEnvironment: "env-1", Holder: "worker-b",
	})
	mustCode(t, err, ErrCodeConflict)

	// 不同环境可以。
	if _, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env-2", Holder: "worker-b",
	}); err != nil {
		t.Fatalf("create restore on other env: %v", err)
	}
}

func TestCreateRestore_RejectsUnrestorableSnapshots(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindFull, Digest: "dg-p",
	})
	if err != nil {
		t.Fatal(err)
	}
	// pending 不可恢复。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: pending.ID, TargetEnvironment: "env", Holder: "w",
	})
	mustCode(t, err, ErrCodeConflict)

	// failed 不可恢复。
	if _, err := svc.MarkSnapshotFailed(ctx, pending.ID, "boom"); err != nil {
		t.Fatal(err)
	}
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: pending.ID, TargetEnvironment: "env", Holder: "w",
	})
	mustCode(t, err, ErrCodeConflict)

	// 不存在的快照。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: "ghost", TargetEnvironment: "env", Holder: "w",
	})
	mustCode(t, err, ErrCodeNotFound)
}

func TestRestoreSteps_OrderedRetryAndReceiptMatching(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env", Holder: "w1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 错误租约不能派发。
	_, err = svc.DispatchNextStep(ctx, task.ID, "lease-999", 1)
	mustCode(t, err, ErrCodeLease)

	// 派发第 0 步。
	d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if d.Step == nil || d.Step.Index != 0 || d.Step.ExecutionVersion != 1 {
		t.Fatalf("unexpected dispatch: %+v", d.Step)
	}
	// 在途步骤不会重复派发。
	d2, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if d2.Step != nil {
		t.Fatalf("expected no dispatch while step running, got %+v", d2.Step)
	}

	// 失败回执 -> 可重试，执行版本递增。
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: 1, Success: false, Detail: "io error",
	}); err != nil {
		t.Fatal(err)
	}
	d3, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if d3.Step == nil || d3.Step.Index != 0 || d3.Step.ExecutionVersion != 2 || d3.Step.Attempts != 2 {
		t.Fatalf("retry dispatch wrong: %+v", d3.Step)
	}
	// 旧执行版本的迟到回执被拒绝。
	_, err = svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	})
	mustCode(t, err, ErrCodeConflict)
	// 正确版本成功。
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: 2, Success: true,
	}); err != nil {
		t.Fatal(err)
	}

	// 有序推进：依次完成第 1、2 步。
	for i := 1; i <= 2; i++ {
		dd, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if dd.Step == nil || dd.Step.Index != i {
			t.Fatalf("expected step %d, got %+v", i, dd.Step)
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: i, ExecutionVersion: dd.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// 全部完成后完成任务，写出唯一 outbox。
	res, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if res.Task.Status != TaskSucceeded || res.Event == nil {
		t.Fatalf("completion wrong: %+v", res)
	}
	// 重复完成幂等：不产生第二条通知。
	res2, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Event == nil || res2.Event.ID != res.Event.ID {
		t.Fatalf("idempotent completion should reuse event %s, got %+v", res.Event.ID, res2.Event)
	}
	events, err := svc.ListOutboxEvents(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 outbox event, got %d", len(events))
	}

	// 终态任务上的操作被拒绝。
	_, err = svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	mustCode(t, err, ErrCodeConflict)

	// 同一环境现在可以开始新的恢复。
	if _, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env", Holder: "w2",
	}); err != nil {
		t.Fatalf("new restore after completion: %v", err)
	}
}

func TestCompleteRestore_RequiresAllStepsSucceeded(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 1)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[1], TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	mustCode(t, err, ErrCodeConflict)
}

// ---------------------------------------------------------------------------
// 租约接管
// ---------------------------------------------------------------------------

func TestTakeoverLease_FencesOldHolder(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 1)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[1], TargetEnvironment: "env", Holder: "old-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, oldEpoch := task.LeaseID, task.LeaseEpoch

	// 旧持有者派发了第 0 步（在途）。
	d, err := svc.DispatchNextStep(ctx, task.ID, oldLease, oldEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v %+v", err, d.Step)
	}
	oldVersion := d.Step.ExecutionVersion

	// 新持有者接管。
	task2, err := svc.TakeoverLease(ctx, task.ID, "new-worker", "old worker unreachable")
	if err != nil {
		t.Fatal(err)
	}
	if task2.LeaseEpoch != oldEpoch+1 || task2.LeaseID == oldLease {
		t.Fatalf("lease not advanced: %+v", task2)
	}
	if task2.Steps[0].Status != StepFailed {
		t.Fatalf("running step should be reset to failed, got %s", task2.Steps[0].Status)
	}

	// 旧租约的迟到回执不可干扰接管者。
	_, err = svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: oldLease, Epoch: oldEpoch,
		StepIndex: 0, ExecutionVersion: oldVersion, Success: true,
	})
	mustCode(t, err, ErrCodeLease)
	// 旧租约也不能再派发。
	_, err = svc.DispatchNextStep(ctx, task.ID, oldLease, oldEpoch)
	mustCode(t, err, ErrCodeLease)

	// 接管时旧尝试的执行版本也已失效（即使伪造新租约 ID）。
	_, err = svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task2.LeaseID, Epoch: task2.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: oldVersion, Success: true,
	})
	mustCode(t, err, ErrCodeConflict)

	// 新持有者重新派发并完成。
	d2, err := svc.DispatchNextStep(ctx, task.ID, task2.LeaseID, task2.LeaseEpoch)
	if err != nil || d2.Step == nil {
		t.Fatalf("dispatch after takeover: %v", err)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task2.LeaseID, Epoch: task2.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d2.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	d3, err := svc.DispatchNextStep(ctx, task.ID, task2.LeaseID, task2.LeaseEpoch)
	if err != nil || d3.Step == nil {
		t.Fatalf("dispatch step 1: %v", err)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task2.LeaseID, Epoch: task2.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: d3.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	res, err := svc.CompleteRestore(ctx, task.ID, task2.LeaseID, task2.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if res.Task.LeaseHolder != "new-worker" {
		t.Fatalf("holder = %s, want new-worker", res.Task.LeaseHolder)
	}

	// 租约审计记录完整：acquired -> taken_over -> released。
	events, err := svc.ListLeaseEvents(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("want 3 lease events, got %d: %+v", len(events), events)
	}
	if events[0].Action != LeaseAcquired || events[1].Action != LeaseTakenOver || events[2].Action != LeaseReleased {
		t.Fatalf("unexpected lease event sequence: %+v", events)
	}

	// 已完成任务不能再被接管。
	_, err = svc.TakeoverLease(ctx, task.ID, "third", "too late")
	mustCode(t, err, ErrCodeConflict)
}

// ---------------------------------------------------------------------------
// 保留清理
// ---------------------------------------------------------------------------

func decisionFor(run *RetentionRun, snapshotID string) *RetentionDecision {
	for i := range run.Decisions {
		if run.Decisions[i].SnapshotID == snapshotID {
			return &run.Decisions[i]
		}
	}
	return nil
}

func hasReason(d *RetentionDecision, reason string) bool {
	for _, r := range d.Reasons {
		if r == reason {
			return true
		}
	}
	return false
}

func TestRunRetention_ProtectsActiveRestorePolicyAndPending(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)

	// ds 链：full -> inc-a -> inc-b -> inc-c（全部完成）。
	ids := registerChain(t, ctx, svc, "ds", 3)
	// 一个失败快照、一个 pending 子快照。
	failed, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[3], Digest: "dg-failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkSnapshotFailed(ctx, failed.ID, "corrupt"); err != nil {
		t.Fatal(err)
	}
	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[3], Digest: "dg-pending",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 有效恢复冻结 inc-b 的链（full, inc-a, inc-b）。
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Minute)
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}})
	if err != nil {
		t.Fatal(err)
	}

	// 被有效恢复链引用：full/inc-a/inc-b 保留。
	for _, id := range ids[:3] {
		d := decisionFor(run, id)
		if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonActiveRestore) {
			t.Fatalf("snapshot %s should be retained by active restore, got %+v", id, d)
		}
	}
	// 保留策略选中最近的 inc-c。
	d := decisionFor(run, ids[3])
	if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonRetentionPolicy) {
		t.Fatalf("latest completed should be retained by policy, got %+v", d)
	}
	// pending 子快照及其祖先（inc-c 已有 policy 原因，这里检查 pending 自身）。
	d = decisionFor(run, pending.ID)
	if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonSnapshotInProgress) {
		t.Fatalf("pending snapshot should be retained, got %+v", d)
	}
	// 失败快照被删除并记录原因。
	d = decisionFor(run, failed.ID)
	if d == nil || d.Action != RetentionDeleted || !hasReason(d, ReasonFailedUnreferenced) {
		t.Fatalf("failed snapshot should be deleted, got %+v", d)
	}
	if _, err := svc.GetSnapshot(ctx, failed.ID); CodeOf(err) != ErrCodeNotFound {
		t.Fatalf("failed snapshot should be gone, err=%v", err)
	}
	// 被保护的都还在。
	for _, id := range append(ids, pending.ID) {
		if _, err := svc.GetSnapshot(ctx, id); err != nil {
			t.Fatalf("protected snapshot %s missing: %v", id, err)
		}
	}
	// 决定已持久化。
	runs, err := svc.ListRetentionRuns(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("retention run not persisted: %+v", runs)
	}

	// 恢复完成后解除冻结：pending 子快照失败（失败终态不再保护祖先），
	// 再以 keep 0 清理——此前被活动恢复冻结的整条链现在都应可删除。
	for i := 0; i < 3; i++ {
		dd, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil || dd.Step == nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: dd.Step.Index, ExecutionVersion: dd.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkSnapshotFailed(ctx, pending.ID, "aborted"); err != nil {
		t.Fatal(err)
	}
	run2, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range append(ids, pending.ID) {
		d := decisionFor(run2, id)
		if d == nil || d.Action != RetentionDeleted {
			t.Fatalf("snapshot %s should be deletable after restore finished and child failed, got %+v", id, d)
		}
	}
	snaps, err := svc.ListDatasetSnapshots(ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected dataset empty after unfreeze, got %d snapshots", len(snaps))
	}
}

func TestRunRetention_KeepZeroDeletesExpiredChain(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)

	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		d := decisionFor(run, id)
		if d == nil || d.Action != RetentionDeleted || !hasReason(d, ReasonExpiredUnreferenced) {
			t.Fatalf("snapshot %s should be deleted as expired, got %+v", id, d)
		}
	}
	snaps, err := svc.ListDatasetSnapshots(ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 0 {
		t.Fatalf("expected empty dataset, got %d snapshots", len(snaps))
	}
}

func TestRunRetention_UncoveredDatasetUntouched(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	registerChain(t, ctx, svc, "ds-a", 1)
	registerChain(t, ctx, svc, "ds-b", 1)

	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds-a", KeepLatestCompleted: 0}}); err != nil {
		t.Fatal(err)
	}
	snaps, err := svc.ListDatasetSnapshots(ctx, "ds-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 2 {
		t.Fatalf("uncovered dataset should be untouched, got %d snapshots", len(snaps))
	}
}

// ---------------------------------------------------------------------------
// 并发
// ---------------------------------------------------------------------------

func TestConcurrentCreateRestore_SingleActivePerEnvironment(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 1)

	const workers = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = svc.CreateRestore(ctx, CreateRestoreInput{
				TargetSnapshotID: ids[1], TargetEnvironment: "env-hot", Holder: "w",
			})
		}(i)
	}
	close(start)
	wg.Wait()

	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		if CodeOf(err) != ErrCodeConflict {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly 1 active restore per environment, got %d", succeeded)
	}
}

func TestConcurrentRegisterAndRetention_NeverLosesProtectedSnapshots(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 0)

	// 恢复冻结 full。
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[0], TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 并发：一边持续登记新增量（引用 full），一边反复跑保留清理。
	stop := make(chan struct{})
	var seq atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		parent := ids[0]
		for {
			select {
			case <-stop:
				return
			default:
			}
			n := seq.Add(1)
			snap, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
				DatasetID: "ds", Kind: KindIncremental, ParentID: parent,
				Digest: fmt.Sprintf("dg-conc-%d", n),
			})
			if err != nil {
				// 父快照可能刚被清理删除；从根重新接。
				parent = ids[0]
				continue
			}
			if _, err := svc.MarkSnapshotCompleted(ctx, snap.ID); err == nil {
				parent = snap.ID
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// keep 0：凡是不被引用的都删，最容易暴露竞态。
			_, _ = svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
		}
	}()
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 无论交错如何，被有效恢复冻结的 full 必须始终存在。
	if _, err := svc.GetSnapshot(ctx, ids[0]); err != nil {
		t.Fatalf("snapshot frozen by active restore was deleted: %v", err)
	}
	_ = task
}

// ---------------------------------------------------------------------------
// 错误分类
// ---------------------------------------------------------------------------

func TestErrorClassification(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	_, err := svc.GetSnapshot(ctx, "missing")
	mustCode(t, err, ErrCodeNotFound)

	_, err = svc.RegisterSnapshot(ctx, RegisterSnapshotInput{})
	mustCode(t, err, ErrCodeInvalidArgument)

	_, err = svc.GetRestoreTask(ctx, "missing")
	mustCode(t, err, ErrCodeNotFound)

	_, err = svc.RunRetention(ctx, nil)
	mustCode(t, err, ErrCodeInvalidArgument)

	if CodeOf(nil) != "" {
		t.Fatalf("nil error should have empty code")
	}
}
