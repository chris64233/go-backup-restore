package backuprestore

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// runCompactionSteps 把任务的全部步骤派发并回执成功，返回最后的任务状态。
func runCompactionSteps(t *testing.T, ctx context.Context, svc *Service, task *CompactionTask) *CompactionTask {
	t.Helper()
	cur := task
	for {
		d, err := svc.DispatchCompactionStep(ctx, cur.ID, cur.LeaseID, cur.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch compaction step: %v", err)
		}
		if d.Step == nil {
			return d.Task
		}
		if _, err := svc.AckCompactionStep(ctx, AckStepInput{
			TaskID: cur.ID, LeaseID: cur.LeaseID, Epoch: cur.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatalf("ack compaction step %d: %v", d.Step.Index, err)
		}
		cur = d.Task
	}
}

// ---------------------------------------------------------------------------
// 创建：冻结与校验
// ---------------------------------------------------------------------------

func TestCreateCompaction_FreezesChainAndDatasetVersion(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2) // full, inc-a, inc-b

	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		IdempotencyKey:  "cmp-ds-1",
		StartSnapshotID: ids[0],
		EndSnapshotID:   ids[2],
		Holder:          "worker-1",
	})
	if err != nil {
		t.Fatalf("create compaction: %v", err)
	}
	if task.Status != TaskPending || task.LeaseEpoch != 1 || task.LeaseHolder != "worker-1" {
		t.Fatalf("unexpected task: %+v", task)
	}
	if task.DatasetID != "ds" || task.DatasetVersion != 3 {
		t.Fatalf("frozen dataset version = %d, want 3", task.DatasetVersion)
	}
	if len(task.Chain) != 3 || len(task.Steps) != 3 {
		t.Fatalf("frozen chain/steps = %d/%d, want 3/3", len(task.Chain), len(task.Steps))
	}
	for i, f := range task.Chain {
		if f.SnapshotID != ids[i] || f.Digest == "" {
			t.Fatalf("frozen chain[%d] wrong: %+v", i, f)
		}
	}
	if task.ExpectedDigest == "" {
		t.Fatal("expected digest must be computed at creation")
	}

	// 创建后再登记新快照会推进数据集版本，但任务内冻结的版本不变。
	if _, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[2], Digest: "ds-inc-later",
	}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := svc.GetCompactionTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.DatasetVersion != 3 {
		t.Fatalf("frozen dataset version changed to %d", reloaded.DatasetVersion)
	}
}

func TestCreateCompaction_ValidatesChain(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)
	other := registerChain(t, ctx, svc, "ds-other", 0)

	// 缺参。
	_, err := svc.CreateCompaction(ctx, CreateCompactionInput{Holder: "w"})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 起点/终点不存在。
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: "ghost", EndSnapshotID: ids[2], Holder: "w"})
	mustCode(t, err, ErrCodeNotFound)
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: "ghost", Holder: "w"})
	mustCode(t, err, ErrCodeNotFound)

	// 起点必须是完整快照。
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[1], EndSnapshotID: ids[2], Holder: "w"})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 终点不在起点的链上（反向）。
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: ids[0], Holder: "w"})
	if err != nil {
		t.Fatalf("single-snapshot chain should be allowed: %v", err)
	}
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: other[0], Holder: "w"})
	mustCode(t, err, ErrCodeInvalidArgument) // 跨数据集

	// 链上有未完成快照时拒绝。
	pendingIDs := registerChain(t, ctx, svc, "ds-p", 1)
	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds-p", Kind: KindIncremental, ParentID: pendingIDs[1], Digest: "ds-p-inc-pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	// 终点本身 pending。
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: pendingIDs[0], EndSnapshotID: pending.ID, Holder: "w"})
	mustCode(t, err, ErrCodeConflict)
	// 链中间有失败快照。
	if _, err := svc.MarkSnapshotFailed(ctx, pending.ID, "boom"); err != nil {
		t.Fatal(err)
	}
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: pendingIDs[0], EndSnapshotID: pending.ID, Holder: "w"})
	mustCode(t, err, ErrCodeConflict)
}

func TestCreateCompaction_IdempotencyKey(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 1)

	in := CreateCompactionInput{
		IdempotencyKey: "task-no-42", StartSnapshotID: ids[0], EndSnapshotID: ids[1], Holder: "w",
	}
	first, err := svc.CreateCompaction(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	// 相同任务号重复创建：返回同一任务，不生成新任务。
	second, err := svc.CreateCompaction(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID {
		t.Fatalf("idempotent create returned %s, want %s", second.ID, first.ID)
	}
	// 不同任务号才创建新任务。
	third, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		IdempotencyKey: "task-no-43", StartSnapshotID: ids[0], EndSnapshotID: ids[1], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	if third.ID == first.ID {
		t.Fatal("different idempotency key must create a new task")
	}
	tasks, err := svc.ListCompactionTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 compaction tasks, got %d", len(tasks))
	}
}

// ---------------------------------------------------------------------------
// 步骤执行、重试与租约 fencing
// ---------------------------------------------------------------------------

func TestCompactionSteps_RetryAndLeaseFencing(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 1)
	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: ids[1], Holder: "old-worker",
	})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, oldEpoch := task.LeaseID, task.LeaseEpoch

	// 错误租约不能派发。
	_, err = svc.DispatchCompactionStep(ctx, task.ID, "lease-999", 1)
	mustCode(t, err, ErrCodeLease)

	// 派发第 0 步并失败回执 -> 可重试，执行版本递增。
	d, err := svc.DispatchCompactionStep(ctx, task.ID, oldLease, oldEpoch)
	if err != nil || d.Step == nil || d.Step.ExecutionVersion != 1 {
		t.Fatalf("dispatch: %v %+v", err, d.Step)
	}
	if _, err := svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: oldLease, Epoch: oldEpoch,
		StepIndex: 0, ExecutionVersion: 1, Success: false, Detail: "io error",
	}); err != nil {
		t.Fatal(err)
	}
	d, err = svc.DispatchCompactionStep(ctx, task.ID, oldLease, oldEpoch)
	if err != nil || d.Step == nil || d.Step.ExecutionVersion != 2 || d.Step.Attempts != 2 {
		t.Fatalf("retry dispatch: %v %+v", err, d.Step)
	}

	// 新持有者接管，在途步骤被重置。
	task2, err := svc.TakeoverCompactionLease(ctx, task.ID, "new-worker", "old worker unreachable")
	if err != nil {
		t.Fatal(err)
	}
	if task2.LeaseEpoch != oldEpoch+1 || task2.LeaseID == oldLease {
		t.Fatalf("lease not advanced: %+v", task2)
	}
	if task2.Steps[0].Status != StepFailed {
		t.Fatalf("running step should be reset to failed, got %s", task2.Steps[0].Status)
	}

	// 旧租约的回执不能推进新接管者的任务。
	_, err = svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: oldLease, Epoch: oldEpoch,
		StepIndex: 0, ExecutionVersion: 2, Success: true,
	})
	mustCode(t, err, ErrCodeLease)
	_, err = svc.DispatchCompactionStep(ctx, task.ID, oldLease, oldEpoch)
	mustCode(t, err, ErrCodeLease)
	// 旧执行版本的回执（即使伪造新租约）也被拒绝。
	_, err = svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task2.LeaseID, Epoch: task2.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: 2, Success: true,
	})
	mustCode(t, err, ErrCodeConflict)

	// 新持有者重新派发并跑完，任务可发布。
	runCompactionSteps(t, ctx, svc, task2)
	prog, err := svc.GetCompactionProgress(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prog.Status != TaskRunning || prog.SucceededSteps != 2 || prog.TotalSteps != 2 {
		t.Fatalf("progress wrong: %+v", prog)
	}
	if prog.Attempts != 4 { // 第 0 步尝试 3 次（含接管前 2 次），第 1 步 1 次
		t.Fatalf("attempts = %d, want 4", prog.Attempts)
	}

	// 租约审计：acquired -> taken_over。
	events, err := svc.ListLeaseEvents(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != LeaseAcquired || events[1].Action != LeaseTakenOver {
		t.Fatalf("unexpected lease events: %+v", events)
	}
}

// ---------------------------------------------------------------------------
// 发布：摘要校验、原子性、幂等
// ---------------------------------------------------------------------------

func TestPublishCompaction_DigestVerificationAndAtomicPublish(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)
	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		IdempotencyKey: "cmp-1", StartSnapshotID: ids[0], EndSnapshotID: ids[2], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 步骤未完成不能发布。
	_, err = svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest)
	mustCode(t, err, ErrCodeConflict)

	runCompactionSteps(t, ctx, svc, task)

	// 摘要校验不通过不能发布。
	_, err = svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, "sha256:forged")
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, "")
	mustCode(t, err, ErrCodeConflict)

	// 发布前读者仍看到原链，不存在半成品快照。
	before, err := svc.ListDatasetSnapshots(ctx, "ds")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 3 {
		t.Fatalf("readers must see only the original chain before publish, got %d snapshots", len(before))
	}

	// 校验通过：原子发布。
	res, err := svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if res.Task.Status != TaskSucceeded || res.NewSnapshot == nil || res.Event == nil {
		t.Fatalf("publish result wrong: %+v", res)
	}
	if res.NewSnapshot.Kind != KindFull || res.NewSnapshot.Status != StatusCompleted ||
		res.NewSnapshot.ParentID != "" || res.NewSnapshot.Digest != task.ExpectedDigest {
		t.Fatalf("new snapshot wrong: %+v", res.NewSnapshot)
	}
	if res.Event.Type != OutboxCompactionCompleted {
		t.Fatalf("outbox type = %s, want %s", res.Event.Type, OutboxCompactionCompleted)
	}

	// 替代关系：每个原快照都映射到新完整快照。
	for _, oldID := range ids {
		newID, ok, err := svc.GetReplacement(ctx, oldID)
		if err != nil || !ok || newID != res.NewSnapshot.ID {
			t.Fatalf("replacement for %s = %q,%v,%v", oldID, newID, ok, err)
		}
	}
	if _, ok, _ := svc.GetReplacement(ctx, res.NewSnapshot.ID); ok {
		t.Fatal("new snapshot itself must not have a replacement")
	}

	// 新旧链查询：旧链为冻结链，新链只有新完整快照自身。
	chains, err := svc.GetCompactionChains(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(chains.Old) != 3 || len(chains.New) != 1 || chains.New[0].ID != res.NewSnapshot.ID {
		t.Fatalf("chains wrong: %+v", chains)
	}
	newChain, err := svc.GetBackupChain(ctx, res.NewSnapshot.ID)
	if err != nil || len(newChain) != 1 {
		t.Fatalf("new snapshot chain: %v len=%d", err, len(newChain))
	}

	// 重复发布幂等：同一事件，不产生第二条 outbox。
	res2, err := svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Event == nil || res2.Event.ID != res.Event.ID || res2.NewSnapshot.ID != res.NewSnapshot.ID {
		t.Fatalf("idempotent publish should reuse result, got %+v", res2)
	}
	events, err := svc.ListOutboxEvents(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 outbox event, got %d", len(events))
	}

	// 进度与任务终态。
	prog, err := svc.GetCompactionProgress(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prog.Status != TaskSucceeded || prog.NewSnapshotID != res.NewSnapshot.ID {
		t.Fatalf("progress after publish: %+v", prog)
	}

	// 发布后原链不立即删除。
	for _, id := range ids {
		if _, err := svc.GetSnapshot(ctx, id); err != nil {
			t.Fatalf("original chain snapshot %s must survive publish: %v", id, err)
		}
	}
}

func TestPublishCompaction_ConcurrentSameChainSingleWinner(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)

	// 两个压缩任务覆盖同一条链（不同任务号）。
	var tasks []*CompactionTask
	for i := 0; i < 2; i++ {
		task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
			IdempotencyKey:  fmt.Sprintf("cmp-race-%d", i),
			StartSnapshotID: ids[0], EndSnapshotID: ids[2], Holder: "w",
		})
		if err != nil {
			t.Fatal(err)
		}
		runCompactionSteps(t, ctx, svc, task)
		tasks = append(tasks, task)
	}

	// 并发发布：最多一个成为有效结果。
	var wg sync.WaitGroup
	results := make([]error, 2)
	var newIDs [2]string
	for i := range tasks {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := svc.PublishCompaction(ctx, tasks[i].ID, tasks[i].LeaseID, tasks[i].LeaseEpoch, tasks[i].ExpectedDigest)
			if err == nil {
				newIDs[i] = res.NewSnapshot.ID
			}
			results[i] = err
		}(i)
	}
	wg.Wait()

	winners := 0
	for _, err := range results {
		if err == nil {
			winners++
			continue
		}
		mustCode(t, err, ErrCodeConflict)
	}
	if winners != 1 {
		t.Fatalf("expected exactly 1 winning publish, got %d", winners)
	}
	// 全部原快照的替代关系都指向唯一的新快照。
	for _, oldID := range ids {
		newID, ok, err := svc.GetReplacement(ctx, oldID)
		if err != nil || !ok {
			t.Fatalf("replacement for %s missing: %v", oldID, err)
		}
		found := false
		for _, id := range newIDs {
			if id == newID {
				found = true
			}
		}
		if !found {
			t.Fatalf("replacement %s is not one of the published snapshots %v", newID, newIDs)
		}
	}
	// 只有一条 outbox 通知。
	events, err := svc.ListOutboxEvents(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 compaction outbox event, got %d", len(events))
	}
}

// ---------------------------------------------------------------------------
// 回收：引用阻断与逐个回收
// ---------------------------------------------------------------------------

func TestRetention_ActiveCompactionProtectsChain(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)

	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: ids[2], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 有效压缩任务冻结原链：keep 0 也删不掉。
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		d := decisionFor(run, id)
		if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonActiveCompaction) {
			t.Fatalf("snapshot %s should be retained by active compaction, got %+v", id, d)
		}
		if _, err := svc.GetSnapshot(ctx, id); err != nil {
			t.Fatalf("snapshot %s reclaimed during active compaction: %v", id, err)
		}
	}

	// 引用查询能解释阻断原因。
	refs, err := svc.ExplainSnapshotReferences(ctx, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if !refs.Blocked() || len(refs.ActiveCompactionIDs) != 1 || refs.ActiveCompactionIDs[0] != task.ID {
		t.Fatalf("references wrong: %+v", refs)
	}

	// 发布完成后任务不再阻断；无其他引用时清理逐个回收原链。
	runCompactionSteps(t, ctx, svc, task)
	res, err := svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest)
	if err != nil {
		t.Fatal(err)
	}
	refs, err = svc.ExplainSnapshotReferences(ctx, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if refs.Blocked() {
		t.Fatalf("chain should be unblocked after publish, got %+v", refs)
	}
	run2, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}})
	if err != nil {
		t.Fatal(err)
	}
	// 新完整快照是最近一个已完成快照，被策略保留；原链逐个回收。
	for _, id := range ids {
		d := decisionFor(run2, id)
		if d == nil || d.Action != RetentionDeleted {
			t.Fatalf("original snapshot %s should be reclaimed after publish, got %+v", id, d)
		}
		if _, err := svc.GetSnapshot(ctx, id); CodeOf(err) != ErrCodeNotFound {
			t.Fatalf("original snapshot %s should be gone, err=%v", id, err)
		}
	}
	d := decisionFor(run2, res.NewSnapshot.ID)
	if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonRetentionPolicy) {
		t.Fatalf("new full snapshot should be retained by policy, got %+v", d)
	}
	// 替代关系在回收后仍可查询。
	newID, ok, err := svc.GetReplacement(ctx, ids[0])
	if err != nil || !ok || newID != res.NewSnapshot.ID {
		t.Fatalf("replacement must survive chain reclamation: %q,%v,%v", newID, ok, err)
	}
}

func TestRetention_RestoreAndPendingChildBlockChainReclaim(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 1)

	// 压缩并发布。
	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: ids[1], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	runCompactionSteps(t, ctx, svc, task)
	if _, err := svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest); err != nil {
		t.Fatal(err)
	}

	// 有效恢复引用原链 -> 阻断回收。
	restore, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[1], TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	// 未完成子快照引用链尾 -> 阻断链尾回收。
	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[1], Digest: "ds-inc-pending",
	})
	if err != nil {
		t.Fatal(err)
	}

	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		d := decisionFor(run, id)
		if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonActiveRestore) {
			t.Fatalf("snapshot %s should be retained by active restore, got %+v", id, d)
		}
	}
	refs, err := svc.ExplainSnapshotReferences(ctx, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if len(refs.ActiveRestoreTaskIDs) != 1 || refs.ActiveRestoreTaskIDs[0] != restore.ID {
		t.Fatalf("restore reference missing: %+v", refs)
	}
	if len(refs.PendingChildIDs) != 1 || refs.PendingChildIDs[0] != pending.ID {
		t.Fatalf("pending child reference missing: %+v", refs)
	}
}

// TestConcurrentRestoreAndReclaim_NoBrokenChain 与恢复创建竞争的回收
// 绝不会让恢复拿到断裂链：恢复要么冻结完整链，要么因链已不存在而失败。
func TestConcurrentRestoreAndReclaim_NoBrokenChain(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)

	// 压缩发布使原链可被回收。
	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: ids[0], EndSnapshotID: ids[2], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	runCompactionSteps(t, ctx, svc, task)
	if _, err := svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest); err != nil {
		t.Fatal(err)
	}

	const attempts = 32
	var wg sync.WaitGroup
	frozen := make([]*RestoreTask, attempts)
	createErrs := make([]error, attempts)
	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			tk, err := svc.CreateRestore(ctx, CreateRestoreInput{
				TargetSnapshotID: ids[2], TargetEnvironment: fmt.Sprintf("env-%d", i), Holder: "w",
			})
			frozen[i], createErrs[i] = tk, err
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < attempts; i++ {
			_, _ = svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
		}
	}()
	close(start)
	wg.Wait()

	succeeded := 0
	for i := 0; i < attempts; i++ {
		if createErrs[i] != nil {
			// 链可能已被回收：只允许“不存在/冲突”类失败，绝不返回断裂链。
			code := CodeOf(createErrs[i])
			if code != ErrCodeConflict && code != ErrCodeNotFound {
				t.Fatalf("unexpected create error: %v", createErrs[i])
			}
			continue
		}
		succeeded++
		// 冻结链必须完整且每个快照此刻仍然存在（已被恢复冻结）。
		if len(frozen[i].Chain) != 3 {
			t.Fatalf("restore %s froze a broken chain of %d snapshots", frozen[i].ID, len(frozen[i].Chain))
		}
		for _, f := range frozen[i].Chain {
			if _, err := svc.GetSnapshot(ctx, f.SnapshotID); err != nil {
				t.Fatalf("restore %s chain snapshot %s was reclaimed: %v", frozen[i].ID, f.SnapshotID, err)
			}
		}
	}
	t.Logf("restores succeeded: %d/%d", succeeded, attempts)
}

// ---------------------------------------------------------------------------
// 持久化
// ---------------------------------------------------------------------------

func TestFileStore_CompactionPersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := dir + "/state.json"

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store)
	ids := registerChain(t, ctx, svc, "ds", 1)
	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		IdempotencyKey: "cmp-persist", StartSnapshotID: ids[0], EndSnapshotID: ids[1], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	runCompactionSteps(t, ctx, svc, task)
	res, err := svc.PublishCompaction(ctx, task.ID, task.LeaseID, task.LeaseEpoch, task.ExpectedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	svc2 := NewService(store2)

	reloaded, err := svc2.GetCompactionTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != TaskSucceeded || reloaded.NewSnapshotID != res.NewSnapshot.ID ||
		reloaded.DatasetVersion != 2 || len(reloaded.Replacements) != 2 {
		t.Fatalf("compaction task wrong after reload: %+v", reloaded)
	}
	// 任务号幂等键在重启后仍然生效。
	again, err := svc2.CreateCompaction(ctx, CreateCompactionInput{
		IdempotencyKey: "cmp-persist", StartSnapshotID: ids[0], EndSnapshotID: ids[1], Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != task.ID {
		t.Fatalf("idempotency key lost after reload: got %s, want %s", again.ID, task.ID)
	}
	// 替代关系在重启后仍可查询。
	newID, ok, err := svc2.GetReplacement(ctx, ids[0])
	if err != nil || !ok || newID != res.NewSnapshot.ID {
		t.Fatalf("replacement after reload: %q,%v,%v", newID, ok, err)
	}
	// 数据集版本不回退：压缩发布递增过版本，重启后继续递增。
	if _, err := svc2.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: res.NewSnapshot.ID, Digest: "ds-inc-after",
	}); err != nil {
		t.Fatal(err)
	}
	task2, err := svc2.CreateCompaction(ctx, CreateCompactionInput{
		StartSnapshotID: res.NewSnapshot.ID, EndSnapshotID: res.NewSnapshot.ID, Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	if task2.DatasetVersion != 4 { // 2 次登记 + 1 次发布 + 1 次登记
		t.Fatalf("dataset version regressed after reload: %d", task2.DatasetVersion)
	}
}
