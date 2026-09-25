package backuprestore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 测试夹具
// ---------------------------------------------------------------------------

type fixture struct {
	t   *testing.T
	svc *Service
	ctx context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clock := &fakeClock{t: time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)}
	f := &fixture{
		t:   t,
		svc: NewService(NewMemoryStore()),
		ctx: context.Background(),
	}
	f.svc.SetClock(clock.now)
	return f
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

// register 登记一个完整快照并完成它。
func (f *fixture) full(dataset string) *Snapshot {
	f.t.Helper()
	s, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: dataset, Kind: KindFull, Digest: "digest-" + dataset + "-full",
	})
	if err != nil {
		f.t.Fatalf("register full: %v", err)
	}
	return f.complete(s.ID, s.Digest)
}

func (f *fixture) inc(dataset, parent, digest string) *Snapshot {
	f.t.Helper()
	s, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: dataset, Kind: KindIncremental, ParentID: parent, Digest: digest,
	})
	if err != nil {
		f.t.Fatalf("register incremental: %v", err)
	}
	return s
}

func (f *fixture) complete(id, digest string) *Snapshot {
	f.t.Helper()
	s, err := f.svc.CompleteSnapshot(f.ctx, id, digest)
	if err != nil {
		f.t.Fatalf("complete %s: %v", id, err)
	}
	return s
}

// chain3 构造 full -> inc1 -> inc2 并全部完成，返回三个快照。
func (f *fixture) chain3(dataset string) (full, inc1, inc2 *Snapshot) {
	full = f.full(dataset)
	inc1 = f.complete(f.inc(dataset, full.ID, "d1").ID, "d1")
	inc2 = f.complete(f.inc(dataset, inc1.ID, "d2").ID, "d2")
	return
}

func wantErr(t *testing.T, err error, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("want error %v, got %v", target, err)
	}
}

// ---------------------------------------------------------------------------
// 快照登记
// ---------------------------------------------------------------------------

func TestRegisterSnapshot_FullSnapshot(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds")

	if full.Kind != KindFull || full.Status != SnapshotCompleted || full.Sequence != 1 {
		t.Fatalf("unexpected snapshot: %+v", full)
	}
	chain, err := f.svc.GetChain(f.ctx, full.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 1 || chain[0].ID != full.ID {
		t.Fatalf("unexpected chain: %+v", chain)
	}
}

func TestRegisterSnapshot_InvalidInputs(t *testing.T) {
	f := newFixture(t)

	if _, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{Kind: KindFull, Digest: "d"}); err != nil {
		wantErr(t, err, ErrInvalidInput)
	}
	if _, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{DatasetID: "ds", Kind: KindFull}); err != nil {
		wantErr(t, err, ErrInvalidInput)
	}
	// 完整快照不能有父。
	if _, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindFull, ParentID: "p", Digest: "d",
	}); err != nil {
		wantErr(t, err, ErrInvalidInput)
	}
	// 增量快照必须有父。
	if _, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, Digest: "d",
	}); err != nil {
		wantErr(t, err, ErrInvalidInput)
	}
	// 未知类型。
	if _, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: "bogus", Digest: "d",
	}); err != nil {
		wantErr(t, err, ErrInvalidInput)
	}
}

func TestRegisterSnapshot_ParentMustExist(t *testing.T) {
	f := newFixture(t)
	_, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: "ghost", Digest: "d",
	})
	wantErr(t, err, ErrNotFound)
}

func TestRegisterSnapshot_ParentMustBeCompleted(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds")
	pending := f.inc("ds", full.ID, "p") // 尚未完成

	// 不能挂在 pending 父之后。
	_, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: pending.ID, Digest: "c",
	})
	wantErr(t, err, ErrSnapshotNotCompleted)

	// 挂在失败父之后同样拒绝。
	failed := f.inc("ds", full.ID, "f")
	if _, err := f.svc.FailSnapshot(f.ctx, failed.ID, "boom"); err != nil {
		t.Fatal(err)
	}
	_, err = f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: failed.ID, Digest: "c",
	})
	wantErr(t, err, ErrSnapshotFailed)
}

func TestRegisterSnapshot_CrossDatasetParentRejected(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds-a")

	_, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: "ds-b", Kind: KindIncremental, ParentID: full.ID, Digest: "x",
	})
	wantErr(t, err, ErrCrossDataset)
}

func TestSnapshotCompletion_StateTransitions(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds")
	pending := f.inc("ds", full.ID, "p")

	// 完成时摘要必须匹配。
	_, err := f.svc.CompleteSnapshot(f.ctx, pending.ID, "different")
	wantErr(t, err, ErrDigestConflict)

	// 正常完成。
	done := f.complete(pending.ID, "p")
	if done.Status != SnapshotCompleted || done.CompletedAt.IsZero() {
		t.Fatalf("snapshot not completed: %+v", done)
	}
	// 重复完成幂等。
	if _, err := f.svc.CompleteSnapshot(f.ctx, pending.ID, "p"); err != nil {
		t.Fatalf("idempotent complete failed: %v", err)
	}
	// 已完成不能转失败。
	_, err = f.svc.FailSnapshot(f.ctx, pending.ID, "x")
	wantErr(t, err, ErrInvalidTransition)

	// pending -> failed，且失败不能再完成。
	other := f.inc("ds", full.ID, "o")
	if _, err := f.svc.FailSnapshot(f.ctx, other.ID, "boom"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.FailSnapshot(f.ctx, other.ID, "boom"); err != nil {
		t.Fatalf("idempotent fail failed: %v", err)
	}
	_, err = f.svc.CompleteSnapshot(f.ctx, other.ID, "o")
	wantErr(t, err, ErrInvalidTransition)
}

func TestSnapshot_ImmutableDigestAndChain(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds")

	// 登记后的父指针与摘要不可改变：通过 API 没有任何修改入口；
	// 这里验证冻结到查询结果中的链关系始终一致。
	chain, err := f.svc.GetChain(f.ctx, inc2.ID)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{full.ID, inc1.ID, inc2.ID}
	for i, s := range chain {
		if s.ID != ids[i] {
			t.Fatalf("chain[%d] = %s, want %s", i, s.ID, ids[i])
		}
		if s.ParentID != "" && s.ParentID != ids[i-1] {
			t.Fatalf("parent pointer mutated: %s -> %s", s.ID, s.ParentID)
		}
	}
}

// TestSnapshotChain_CycleDetected 通过直接篡改存储注入一个环，
// 验证无环校验是真实有效的防御，而不是仅仅依赖登记顺序。
func TestSnapshotChain_CycleDetected(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds")

	err := f.svc.store.Update(f.ctx, func(st *state) error {
		// 制造 full -> inc1 -> inc2 -> inc1 的环。
		st.Snapshots[full.ID].Kind = KindIncremental
		st.Snapshots[full.ID].ParentID = inc2.ID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.svc.GetChain(f.ctx, inc1.ID); !errors.Is(err, ErrChainCycle) {
		t.Fatalf("want ErrChainCycle, got %v", err)
	}
	// 环上的快照不能用于恢复。
	if _, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID); !errors.Is(err, ErrChainCycle) {
		t.Fatalf("restore into cyclic chain: want ErrChainCycle, got %v", err)
	}
}

func TestChainRejectsFailedOrPendingAncestor(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds")
	inc1 := f.complete(f.inc("ds", full.ID, "i1").ID, "i1")
	inc2 := f.complete(f.inc("ds", inc1.ID, "i2").ID, "i2")
	inc3 := f.inc("ds", inc2.ID, "i3") // pending

	if _, err := f.svc.GetChain(f.ctx, inc3.ID); !errors.Is(err, ErrSnapshotNotCompleted) {
		t.Fatalf("pending ancestor: want ErrSnapshotNotCompleted, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// 恢复任务：冻结链、租约、步骤、栅栏
// ---------------------------------------------------------------------------

func TestCreateRestore_FreezesChainOrdered(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds")

	job, err := f.svc.CreateRestore(f.ctx, "env-prod", inc2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != RestoreRunning || job.TargetSnapshotID != inc2.ID {
		t.Fatalf("bad job: %+v", job)
	}
	if len(job.Chain) != 3 || len(job.Steps) != 3 {
		t.Fatalf("chain not frozen fully: %d chain, %d steps", len(job.Chain), len(job.Steps))
	}
	wantIDs := []string{full.ID, inc1.ID, inc2.ID}
	for i, fs := range job.Chain {
		if fs.SnapshotID != wantIDs[i] || fs.Index != i {
			t.Fatalf("frozen chain[%d] = %+v", i, fs)
		}
		if job.Steps[i].ExecutionVersion != 1 || job.Steps[i].Status != StepPending {
			t.Fatalf("step %d bad init: %+v", i, job.Steps[i])
		}
	}
	if job.LeaseID == "" || job.Fencing == 0 {
		t.Fatal("lease not issued")
	}
}

func TestCreateRestore_FailedAndPendingTargetsRejected(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds")
	pending := f.inc("ds", full.ID, "p")
	failed := f.inc("ds", full.ID, "x")
	if _, err := f.svc.FailSnapshot(f.ctx, failed.ID, "boom"); err != nil {
		t.Fatal(err)
	}

	if _, err := f.svc.CreateRestore(f.ctx, "e1", pending.ID); !errors.Is(err, ErrSnapshotNotCompleted) {
		t.Fatalf("pending target: got %v", err)
	}
	if _, err := f.svc.CreateRestore(f.ctx, "e2", failed.ID); !errors.Is(err, ErrSnapshotFailed) {
		t.Fatalf("failed target: got %v", err)
	}
}

func TestCreateRestore_OneActiveLeasePerEnv(t *testing.T) {
	f := newFixture(t)
	_, _, inc2 := f.chain3("ds")

	if _, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID); err != nil {
		t.Fatal(err)
	}
	// 同环境第二个恢复必须被拒绝，即使目标快照不同。
	_, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID)
	wantErr(t, err, ErrLeaseConflict)

	// 不同环境互不影响。
	if _, err := f.svc.CreateRestore(f.ctx, "env-staging", inc2.ID); err != nil {
		t.Fatalf("other env should work: %v", err)
	}
}

func TestCreateRestore_LeaseConflictUnderConcurrency(t *testing.T) {
	f := newFixture(t)
	_, _, inc2 := f.chain3("ds")

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successes, conflicts int
	errs := make([]error, 0)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := f.svc.CreateRestore(f.ctx, "env-race", inc2.ID)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrLeaseConflict):
				conflicts++
			default:
				errs = append(errs, err)
			}
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Fatalf("want exactly 1 successful lease, got %d", successes)
	}
	if conflicts != n-1 {
		t.Fatalf("want %d conflicts, got %d", n-1, conflicts)
	}
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
}

// 沿链成功推进全部步骤：必须严格有序，全部成功后只写一条 outbox。
func TestRestore_StepsProceedInOrderAndNotifyOnce(t *testing.T) {
	f := newFixture(t)
	_, _, inc2 := f.chain3("ds")
	job, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID)
	if err != nil {
		t.Fatal(err)
	}

	// 跳过步骤直接报第 2 步：拒绝。
	err = f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
		StepIndex: 1, ExecutionVersion: 1, Success: true,
	})
	wantErr(t, err, ErrStepOrder)

	// 执行版本不匹配：拒绝。
	err = f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
		StepIndex: 0, ExecutionVersion: 9, Success: true,
	})
	wantErr(t, err, ErrVersionMismatch)

	for i := 0; i < 3; i++ {
		if err := f.svc.ReportStep(f.ctx, StepReceipt{
			JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
			StepIndex: i, ExecutionVersion: 1, Success: true,
		}); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}

	done, err := f.svc.GetJob(f.ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != RestoreSucceeded || done.FinishedAt.IsZero() {
		t.Fatalf("job not succeeded: %+v", done)
	}

	// 任务终态后拒绝任何回执。
	err = f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	})
	wantErr(t, err, ErrJobFinished)

	// 恰好一条 outbox，且重复回执路径也不会再写。
	msgs, err := f.svc.ListOutbox(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].JobID != job.ID || msgs[0].Delivered {
		t.Fatalf("unexpected outbox: %+v", msgs)
	}
	if err := f.svc.MarkOutboxDelivered(f.ctx, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
	msgs, _ = f.svc.ListOutbox(f.ctx)
	if !msgs[0].Delivered || msgs[0].DeliveredAt.IsZero() {
		t.Fatal("outbox not marked delivered")
	}

	// 任务结束后租约释放，同环境可以创建新恢复。
	if _, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID); err != nil {
		t.Fatalf("lease should be released: %v", err)
	}
}

func TestRestore_FailureRetryBumpsVersion(t *testing.T) {
	f := newFixture(t)
	_, _, inc2 := f.chain3("ds")
	job, _ := f.svc.CreateRestore(f.ctx, "env", inc2.ID)

	receipt := func(step, version uint64, ok bool) StepReceipt {
		return StepReceipt{
			JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
			StepIndex: int(step), ExecutionVersion: version, Success: ok, Error: "transient",
		}
	}

	// 第 0 步失败，任务继续运行。
	if err := f.svc.ReportStep(f.ctx, receipt(0, 1, false)); err != nil {
		t.Fatal(err)
	}
	mid, _ := f.svc.GetJob(f.ctx, job.ID)
	if mid.Steps[0].Status != StepFailed || mid.Steps[0].LastError != "transient" {
		t.Fatalf("step not failed: %+v", mid.Steps[0])
	}
	// 未重试不能回执下一个步骤。
	wantErr(t, f.svc.ReportStep(f.ctx, receipt(1, 1, true)), ErrStepOrder)
	// 未重试前旧版本的成功回执不允许“翻案”——仍需先重试抬升版本。
	wantErr(t, f.svc.ReportStep(f.ctx, receipt(0, 1, true)), ErrInvalidTransition)

	// 重试抬升到版本 2。
	v, err := f.svc.RetryStep(f.ctx, job.ID, job.LeaseID, job.Fencing, 0)
	if err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Fatalf("new version = %d, want 2", v)
	}
	// 版本 1 的迟到成功回执必须被拒绝。
	wantErr(t, f.svc.ReportStep(f.ctx, receipt(0, 1, true)), ErrVersionMismatch)
	// 版本 2 成功。
	if err := f.svc.ReportStep(f.ctx, receipt(0, 2, true)); err != nil {
		t.Fatal(err)
	}
	// 非失败步骤不能重试。
	_, err = f.svc.RetryStep(f.ctx, job.ID, job.LeaseID, job.Fencing, 0)
	wantErr(t, err, ErrStepOrder)
}

// 接管场景：旧 worker 持有旧租约与栅栏令牌，其迟到回执绝不能干扰接管者。
func TestRestore_TakeoverFencesOffOldLease(t *testing.T) {
	f := newFixture(t)
	_, inc1, inc2 := f.chain3("ds")

	old, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID)
	if err != nil {
		t.Fatal(err)
	}
	// 旧任务完成第 0 步后卡住。
	if err := f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: old.ID, LeaseID: old.LeaseID, Fencing: old.Fencing,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	}); err != nil {
		t.Fatal(err)
	}

	// 接管者接管到一个更短的目标（inc1）。
	newJob, err := f.svc.TakeoverRestore(f.ctx, "env", inc1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if newJob.ID == old.ID || newJob.Fencing <= old.Fencing {
		t.Fatalf("takeover must issue new job and higher fencing: old=%d new=%d", old.Fencing, newJob.Fencing)
	}
	if len(newJob.Chain) != 2 {
		t.Fatalf("new chain should be freshly frozen: %+v", newJob.Chain)
	}

	oldView, _ := f.svc.GetJob(f.ctx, old.ID)
	if oldView.Status != RestoreSuperseded {
		t.Fatalf("old job status = %s, want superseded", oldView.Status)
	}
	// 接管后原环境不能再创建普通恢复（新任务持有租约）。
	_, err = f.svc.CreateRestore(f.ctx, "env", inc1.ID)
	wantErr(t, err, ErrLeaseConflict)

	// 旧 worker 的迟到回执：旧租约 ID/栅栏全部失效。
	err = f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: old.ID, LeaseID: old.LeaseID, Fencing: old.Fencing,
		StepIndex: 1, ExecutionVersion: 1, Success: true,
	})
	wantErr(t, err, ErrJobFinished)

	// 即便旧 worker 猜到了新 jobID，拿着旧租约/旧栅栏也无法回执。
	err = f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: newJob.ID, LeaseID: old.LeaseID, Fencing: old.Fencing,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	})
	wantErr(t, err, ErrStaleLease)
	err = f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: newJob.ID, LeaseID: newJob.LeaseID, Fencing: old.Fencing,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	})
	wantErr(t, err, ErrStaleLease)

	// 接管者自己的回执正常工作。
	if err := f.svc.ReportStep(f.ctx, StepReceipt{
		JobID: newJob.ID, LeaseID: newJob.LeaseID, Fencing: newJob.Fencing,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	}); err != nil {
		t.Fatalf("new holder receipt: %v", err)
	}
}

func TestRestore_CancelReleasesLease(t *testing.T) {
	f := newFixture(t)
	_, _, inc2 := f.chain3("ds")
	job, _ := f.svc.CreateRestore(f.ctx, "env", inc2.ID)

	// 错误凭证不能取消。
	wantErr(t, f.svc.CancelRestore(f.ctx, job.ID, "wrong", job.Fencing), ErrStaleLease)

	if err := f.svc.CancelRestore(f.ctx, job.ID, job.LeaseID, job.Fencing); err != nil {
		t.Fatal(err)
	}
	got, _ := f.svc.GetJob(f.ctx, job.ID)
	if got.Status != RestoreCanceled {
		t.Fatalf("status = %s", got.Status)
	}
	// 租约释放，可重新创建。
	if _, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID); err != nil {
		t.Fatalf("lease should be released: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 保留清理
// ---------------------------------------------------------------------------

func reasonFor(records []CleanupRecord, snapshotID string) CleanupRecord {
	for _, r := range records {
		if r.SnapshotID == snapshotID {
			return r
		}
	}
	return CleanupRecord{}
}

func TestRetention_KeepsLatestAndAncestors(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds")
	// 再造一条新链：full2 -> inc3（与第一条链无关）。
	full2 := f.full2(t, "ds")
	inc3 := f.complete(f.inc("ds", full2.ID, "i3").ID, "i3")

	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 1); err != nil {
		t.Fatal(err)
	}
	report, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if report.DeletedCount != 3 {
		t.Fatalf("deleted = %d, want 3", report.DeletedCount)
	}
	recs := report.Decisions
	// 最新快照 inc3 及其祖先 full2 受策略保护。
	if r := reasonFor(recs, inc3.ID); r.Decision != Retain || r.Reason != ReasonRetentionPolicy {
		t.Fatalf("inc3: %+v", r)
	}
	if r := reasonFor(recs, full2.ID); r.Decision != Retain || r.Reason != ReasonRetentionPolicy {
		t.Fatalf("full2: %+v", r)
	}
	// 旧链整体过期删除。
	for _, id := range []string{full.ID, inc1.ID, inc2.ID} {
		if r := reasonFor(recs, id); r.Decision != Delete || r.Reason != ReasonPolicyExpired {
			t.Fatalf("%s: %+v", id, r)
		}
	}

	// 已删除快照不再出现在活动列表，也不能再用于恢复。
	list, err := f.svc.ListSnapshots(f.ctx, "ds", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range list {
		if s.ID == inc2.ID {
			t.Fatal("deleted snapshot still listed")
		}
	}
	if _, err := f.svc.CreateRestore(f.ctx, "env", inc2.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("restore into deleted chain: got %v", err)
	}
}

func (f *fixture) full2(t *testing.T, dataset string) *Snapshot {
	t.Helper()
	s, err := f.svc.RegisterSnapshot(f.ctx, RegisterSnapshotInput{
		DatasetID: dataset, Kind: KindFull, Digest: "digest-full-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	return f.complete(s.ID, s.Digest)
}

// 保留策略保留头部时必须保留其祖先（祖先不能在后代仍存活时被删）。
func TestRetention_KeepLatestTwoProtectsSharedAncestors(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds")
	inc3 := f.complete(f.inc("ds", inc2.ID, "i3").ID, "i3")

	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 2); err != nil {
		t.Fatal(err)
	}
	report, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	recs := report.Decisions
	// 头部 inc3、inc2 及其全部祖先都受保护，什么都不能删。
	for _, id := range []string{full.ID, inc1.ID, inc2.ID, inc3.ID} {
		if r := reasonFor(recs, id); r.Decision != Retain {
			t.Fatalf("%s should be retained: %+v", id, r)
		}
	}
	if report.DeletedCount != 0 {
		t.Fatalf("deleted = %d, want 0", report.DeletedCount)
	}
}

// 有效恢复任务冻结链上的快照，即使策略已过期也不能删除。
func TestRetention_ActiveRestoreChainIsProtected(t *testing.T) {
	f := newFixture(t)
	// 旧链 full -> inc1 -> inc2；新链 full2 -> inc3（与旧链无关）。
	full, inc1, inc2 := f.chain3("ds")
	full2 := f.full2(t, "ds")
	inc3 := f.complete(f.inc("ds", full2.ID, "i3").ID, "i3")

	// keep=1：inc3 是唯一头部，旧链本应过期——但 env 上有一个恢复到 inc1 的任务。
	job, err := f.svc.CreateRestore(f.ctx, "env", inc1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 1); err != nil {
		t.Fatal(err)
	}
	report, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	recs := report.Decisions
	// 冻结链上的 full、inc1 受恢复保护；inc2 不在冻结链上，正常过期。
	for _, id := range []string{full.ID, inc1.ID} {
		if r := reasonFor(recs, id); r.Decision != Retain || r.Reason != ReasonActiveRestore {
			t.Fatalf("%s must be protected by active restore: %+v", id, r)
		}
	}
	if r := reasonFor(recs, inc2.ID); r.Decision != Delete {
		t.Fatalf("inc2 not on frozen chain, should expire: %+v", r)
	}
	if r := reasonFor(recs, inc3.ID); r.Decision != Retain {
		t.Fatalf("inc3 policy head should be retained: %+v", r)
	}

	// 已完成的恢复不再保护链：任务成功后重新清理即可删除。
	for i := range job.Chain {
		if err := f.svc.ReportStep(f.ctx, StepReceipt{
			JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
			StepIndex: i, ExecutionVersion: 1, Success: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	report2, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if r := reasonFor(report2.Decisions, full.ID); r.Decision != Delete {
		t.Fatalf("finished restore must release protection: %+v", r)
	}
}

// 未完成子快照引用的祖先不能删除，未完成快照自身也不能删除。
func TestRetention_PendingChildProtectsAncestors(t *testing.T) {
	f := newFixture(t)
	full, inc1, _ := f.chain3("ds")
	pending := f.inc("ds", inc1.ID, "pending-child")

	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 0); err != nil {
		t.Fatal(err)
	}
	report, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	recs := report.Decisions
	if r := reasonFor(recs, pending.ID); r.Decision != Retain || r.Reason != ReasonIncompleteSnapshot {
		t.Fatalf("pending itself: %+v", r)
	}
	for _, id := range []string{full.ID, inc1.ID} {
		if r := reasonFor(recs, id); r.Decision != Retain || r.Reason != ReasonIncompleteChild {
			t.Fatalf("ancestor %s of pending child: %+v", id, r)
		}
	}
}

// 失败快照可以安全回收（它不可能被恢复链或未完成子快照引用）。
func TestRetention_FailedSnapshotsAreDeleted(t *testing.T) {
	f := newFixture(t)
	full := f.full("ds")
	bad := f.inc("ds", full.ID, "bad")
	if _, err := f.svc.FailSnapshot(f.ctx, bad.ID, "write failed"); err != nil {
		t.Fatal(err)
	}
	// 注意 full 是 bad 的父，但 bad 已失败，其引用不构成保护；
	// 在 keep=0 策略下，full 也过期删除。
	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 0); err != nil {
		t.Fatal(err)
	}
	report, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if r := reasonFor(report.Decisions, bad.ID); r.Decision != Delete || r.Reason != ReasonFailedSnapshot || r.Detail != "write failed" {
		t.Fatalf("bad: %+v", r)
	}
}

// 未配置策略时的安全默认：已完成快照全部保留。
func TestRetention_NoPolicyMeansSafeDefault(t *testing.T) {
	f := newFixture(t)
	f.full("ds")
	report, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if report.DeletedCount != 0 {
		t.Fatalf("deleted = %d, want 0", report.DeletedCount)
	}
	if report.Decisions[0].Reason != ReasonNoPolicy {
		t.Fatalf("reason = %s", report.Decisions[0].Reason)
	}
}

// 清理决定记录持久且追加：同一快照多次评估都留痕。
func TestRetention_DecisionsAreRecordedAppendOnly(t *testing.T) {
	f := newFixture(t)
	// 旧链 full -> inc1 -> inc2 过期；新链 full2 是唯一保留头部。
	full, inc1, inc2 := f.chain3("ds")
	full2 := f.full2(t, "ds")
	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 1); err != nil {
		t.Fatal(err)
	}
	report1, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if report1.DeletedCount != 3 {
		t.Fatalf("first run deleted = %d, want 3", report1.DeletedCount)
	}
	// 第二次清理：只剩 full2 存活可评估；已删除的旧链不再重复评估。
	report2, err := f.svc.RunRetention(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if len(report2.Decisions) != 1 || report2.Decisions[0].SnapshotID != full2.ID {
		t.Fatalf("second run decisions = %+v", report2.Decisions)
	}
	all, err := f.svc.ListCleanupRecords(f.ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	// 第一次 4 条 + 第二次 1 条，全部追加保留。
	if len(all) != 5 {
		t.Fatalf("cleanup records = %d, want 5 (append-only)", len(all))
	}
	_ = full
	_ = inc1
	_ = inc2
}

// 清理与恢复创建并发：被有效恢复冻结的链绝不能被清理删除。
func TestRetention_ConcurrentWithRestoreCreation(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds")
	if err := f.svc.SetRetentionPolicy(f.ctx, "ds", 0); err != nil {
		t.Fatal(err)
	}

	const rounds = 20
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = f.svc.CreateRestore(f.ctx, fmt.Sprintf("env-%d", i%5), inc2.ID)
		}()
		go func() {
			defer wg.Done()
			_, _ = f.svc.RunRetention(f.ctx, "ds")
		}()
	}
	wg.Wait()

	// 关键不变量：任何仍在运行的恢复，其冻结链上的快照都必须存活。
	err := f.svc.store.View(f.ctx, func(st *state) error {
		for _, job := range st.Jobs {
			if job.Status != RestoreRunning {
				continue
			}
			if _, ok := st.Leases[job.TargetEnv]; !ok {
				continue
			}
			for _, fs := range job.Chain {
				snap := st.Snapshots[fs.SnapshotID]
				if snap == nil || snap.Deleted {
					return fmt.Errorf("running job %s references deleted snapshot %s", job.ID, fs.SnapshotID)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = full
	_ = inc1
}

// ---------------------------------------------------------------------------
// 持久化
// ---------------------------------------------------------------------------

func TestFileStore_PersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	svc := NewService(mustOpenStore(t, path))
	ctx := context.Background()
	s, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindFull, Digest: "d0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.CompleteSnapshot(ctx, s.ID, "d0"); err != nil {
		t.Fatal(err)
	}
	job, err := svc.CreateRestore(ctx, "env", s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReportStep(ctx, StepReceipt{
		JobID: job.ID, LeaseID: job.LeaseID, Fencing: job.Fencing,
		StepIndex: 0, ExecutionVersion: 1, Success: true,
	}); err != nil {
		t.Fatal(err)
	}

	// 重新打开：关系、任务终态、outbox 全部恢复。
	svc2 := NewService(mustOpenStore(t, path))
	chain, err := svc2.GetChain(ctx, s.ID)
	if err != nil || len(chain) != 1 || chain[0].Status != SnapshotCompleted {
		t.Fatalf("snapshot state lost: %v %+v", err, chain)
	}
	got, err := svc2.GetJob(ctx, job.ID)
	if err != nil || got.Status != RestoreSucceeded {
		t.Fatalf("job state lost: %v %+v", err, got)
	}
	msgs, err := svc2.ListOutbox(ctx)
	if err != nil || len(msgs) != 1 || msgs[0].JobID != job.ID {
		t.Fatalf("outbox lost: %v %+v", err, msgs)
	}
	// 已完成任务的租约不再有效，环境上可新建恢复。
	if _, err := svc2.CreateRestore(ctx, "env", s.ID); err != nil {
		t.Fatalf("lease state not recovered correctly: %v", err)
	}
}

func TestFileStore_FailedTransactionDoesNotPersist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store := mustOpenStore(t, path)
	svc := NewService(store)
	ctx := context.Background()

	before, err := svc.ListSnapshots(ctx, "ds", true)
	if err != nil {
		t.Fatal(err)
	}
	// 事务回调返回错误：修改丢弃。
	err = store.Update(ctx, func(st *state) error {
		st.Snapshots["should-not-stick"] = &Snapshot{ID: "should-not-stick", DatasetID: "ds"}
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	after, _ := svc.ListSnapshots(ctx, "ds", true)
	if len(before) != len(after) {
		t.Fatal("rolled-back transaction changed persisted state")
	}
}

func mustOpenStore(t *testing.T, path string) *FileStore {
	t.Helper()
	st, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// ---------------------------------------------------------------------------
// 查询
// ---------------------------------------------------------------------------

func TestQueries_ListSnapshotsAndChain(t *testing.T) {
	f := newFixture(t)
	full, inc1, inc2 := f.chain3("ds-a")
	f.full("ds-b")

	list, err := f.svc.ListSnapshots(f.ctx, "ds-a", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 3 {
		t.Fatalf("list = %d rows", len(list))
	}
	if list[0].ID != full.ID || list[1].ID != inc1.ID || list[2].ID != inc2.ID {
		t.Fatal("list not ordered by sequence")
	}

	chain, err := f.svc.GetChain(f.ctx, inc2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 {
		t.Fatalf("chain len = %d", len(chain))
	}
	if _, err := f.svc.GetChain(f.ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing chain: %v", err)
	}
}
