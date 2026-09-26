package backuprestore

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// createCompaction 是创建压缩任务的测试辅助：失败即终止测试。
func createCompaction(t *testing.T, ctx context.Context, svc *Service, from, to, key string) *CompactionTask {
	t.Helper()
	task, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		FromSnapshotID: from, ToSnapshotID: to, Holder: "worker-1", IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("create compaction: %v", err)
	}
	return task
}

// runCompactionSteps 依次派发并成功回执压缩任务的全部步骤。
func runCompactionSteps(t *testing.T, ctx context.Context, svc *Service, taskID string) *CompactionTask {
	t.Helper()
	task, err := svc.GetCompactionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("get compaction: %v", err)
	}
	for {
		d, err := svc.DispatchCompactionStep(ctx, taskID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch compaction step: %v", err)
		}
		task = d.Task
		if d.Step == nil {
			return task
		}
		if _, err := svc.AckCompactionStep(ctx, AckStepInput{
			TaskID: taskID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
			Success: true,
		}); err != nil {
			t.Fatalf("ack compaction step %d: %v", d.Step.Index, err)
		}
	}
}

// publishCompaction 用任务冻结的期望摘要发布压缩结果。
func publishCompaction(t *testing.T, ctx context.Context, svc *Service, taskID string) *PublishCompactionResult {
	t.Helper()
	task, err := svc.GetCompactionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("get compaction: %v", err)
	}
	res, err := svc.PublishCompaction(ctx, PublishCompactionInput{
		TaskID: taskID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		ContentDigest: task.ExpectedDigest,
	})
	if err != nil {
		t.Fatalf("publish compaction: %v", err)
	}
	return res
}

func chainIDs(chain []*Snapshot) []string {
	ids := make([]string, 0, len(chain))
	for _, sn := range chain {
		ids = append(ids, sn.ID)
	}
	return ids
}

func equalIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCreateCompactionFreezesChainDigestsAndVersion(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2) // full, inc-a, inc-b

	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")

	if task.Status != TaskPending || task.LeaseEpoch != 1 || task.LeaseHolder != "worker-1" {
		t.Fatalf("unexpected task state: %+v", task)
	}
	if task.DatasetVersion != 3 { // 3 次登记 => 版本 3
		t.Fatalf("expected frozen dataset version 3, got %d", task.DatasetVersion)
	}
	if len(task.Chain) != 3 || len(task.Steps) != 3 {
		t.Fatalf("expected frozen chain of 3, got %+v", task.Chain)
	}
	for i, f := range task.Chain {
		if f.SnapshotID != ids[i] {
			t.Fatalf("frozen chain[%d] = %s, want %s", i, f.SnapshotID, ids[i])
		}
		if f.Digest == "" {
			t.Fatalf("frozen chain[%d] missing digest", i)
		}
	}
	if task.ExpectedDigest == "" {
		t.Fatal("expected digest must be derived at creation")
	}

	// 冻结后数据集继续演进，任务上的版本与链摘要不变。
	extra, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[2], Digest: "ds-inc-c",
	})
	if err != nil {
		t.Fatalf("register extra: %v", err)
	}
	if _, err := svc.MarkSnapshotCompleted(ctx, extra.ID); err != nil {
		t.Fatalf("complete extra: %v", err)
	}
	again, err := svc.GetCompactionTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("get compaction: %v", err)
	}
	if again.DatasetVersion != 3 {
		t.Fatalf("frozen dataset version changed to %d", again.DatasetVersion)
	}
}

func TestCreateCompactionIdempotencyKey(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)

	first := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	second, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		FromSnapshotID: ids[0], ToSnapshotID: ids[2], Holder: "worker-2", IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("idempotent re-create: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("same key must return same task, got %s vs %s", second.ID, first.ID)
	}

	// 键相同但链不同：冲突，绝不悄悄复用。
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		FromSnapshotID: ids[0], ToSnapshotID: ids[1], Holder: "worker-1", IdempotencyKey: "key-1",
	})
	mustCode(t, err, ErrCodeConflict)
}

func TestCreateCompactionValidation(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)      // full, inc-a, inc-b
	other := registerChain(t, ctx, svc, "other", 1) // 另一数据集的链

	cases := []struct {
		name string
		in   CreateCompactionInput
		code ErrorCode
	}{
		{"missing holder", CreateCompactionInput{FromSnapshotID: ids[0], ToSnapshotID: ids[2], IdempotencyKey: "k"}, ErrCodeInvalidArgument},
		{"missing key", CreateCompactionInput{FromSnapshotID: ids[0], ToSnapshotID: ids[2], Holder: "w"}, ErrCodeInvalidArgument},
		{"from not found", CreateCompactionInput{FromSnapshotID: "snap-x", ToSnapshotID: ids[2], Holder: "w", IdempotencyKey: "k"}, ErrCodeNotFound},
		{"to not found", CreateCompactionInput{FromSnapshotID: ids[0], ToSnapshotID: "snap-x", Holder: "w", IdempotencyKey: "k"}, ErrCodeNotFound},
		{"from not full", CreateCompactionInput{FromSnapshotID: ids[1], ToSnapshotID: ids[2], Holder: "w", IdempotencyKey: "k"}, ErrCodeInvalidArgument},
		{"not ancestor", CreateCompactionInput{FromSnapshotID: other[0], ToSnapshotID: ids[2], Holder: "w", IdempotencyKey: "k"}, ErrCodeInvalidArgument},
		{"single node", CreateCompactionInput{FromSnapshotID: ids[0], ToSnapshotID: ids[0], Holder: "w", IdempotencyKey: "k"}, ErrCodeInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateCompaction(ctx, tc.in)
			mustCode(t, err, tc.code)
		})
	}

	// 链上有未完成快照：不允许压缩。
	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[2], Digest: "ds-inc-pending",
	})
	if err != nil {
		t.Fatalf("register pending: %v", err)
	}
	_, err = svc.CreateCompaction(ctx, CreateCompactionInput{
		FromSnapshotID: ids[0], ToSnapshotID: pending.ID, Holder: "w", IdempotencyKey: "k2",
	})
	mustCode(t, err, ErrCodeConflict)
}

func TestCompactionStepRetryAndStaleReceipt(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")

	d, err := svc.DispatchCompactionStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v, step=%v", err, d.Step)
	}
	// 执行失败 -> 可重试。
	if _, err := svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
		Success: false, Detail: "io error",
	}); err != nil {
		t.Fatalf("ack failure: %v", err)
	}
	// 重新派发，执行版本递增。
	d2, err := svc.DispatchCompactionStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d2.Step == nil {
		t.Fatalf("re-dispatch: %v", err)
	}
	if d2.Step.ExecutionVersion != d.Step.ExecutionVersion+1 {
		t.Fatalf("execution version not bumped: %d -> %d", d.Step.ExecutionVersion, d2.Step.ExecutionVersion)
	}
	// 旧版本的迟到回执被拒绝，状态不变。
	_, err = svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	})
	mustCode(t, err, ErrCodeConflict)
	// 当前版本回执成功。
	if _, err := svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: d2.Step.Index, ExecutionVersion: d2.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatalf("ack current version: %v", err)
	}
}

func TestCompactionTakeoverFencesOldLease(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")

	d, err := svc.DispatchCompactionStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v", err)
	}
	oldLease, oldEpoch := task.LeaseID, task.LeaseEpoch

	taken, err := svc.TakeoverCompactionLease(ctx, task.ID, "worker-2", "worker-1 lost")
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if taken.LeaseEpoch != oldEpoch+1 || taken.LeaseID == oldLease {
		t.Fatalf("lease not rotated: %+v", taken)
	}
	// 旧租约持有者的回执不能推进新接管者的任务。
	_, err = svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: oldLease, Epoch: oldEpoch,
		StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	})
	mustCode(t, err, ErrCodeLease)
	// 旧租约连派发都被拒绝。
	_, err = svc.DispatchCompactionStep(ctx, task.ID, oldLease, oldEpoch)
	mustCode(t, err, ErrCodeLease)

	// 新持有者从头推进直至发布。
	runCompactionSteps(t, ctx, svc, task.ID)
	res := publishCompaction(t, ctx, svc, task.ID)
	if res.NewSnapshot == nil || res.NewSnapshot.Kind != KindFull {
		t.Fatalf("expected new full snapshot, got %+v", res.NewSnapshot)
	}
}

func TestPublishRequiresMatchingDigest(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, task.ID)

	// 摘要不匹配：拒绝发布，任务保持可重试，读者仍看到原链。
	_, err := svc.PublishCompaction(ctx, PublishCompactionInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		ContentDigest: "sha256:wrong",
	})
	mustCode(t, err, ErrCodeConflict)

	cur, err := svc.GetCompactionTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("get compaction: %v", err)
	}
	if cur.Status != TaskRunning || cur.NewSnapshotID != "" {
		t.Fatalf("task must stay unpublished, got %+v", cur)
	}
	snaps, err := svc.ListDatasetSnapshots(ctx, "ds")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("no new snapshot may be visible before publish, got %d", len(snaps))
	}
	eff, err := svc.GetEffectiveChain(ctx, ids[2])
	if err != nil {
		t.Fatalf("effective chain: %v", err)
	}
	if !equalIDs(chainIDs(eff), ids) {
		t.Fatalf("readers must still see the original chain, got %v", chainIDs(eff))
	}

	// 正确摘要：原子发布。
	res := publishCompaction(t, ctx, svc, task.ID)
	if res.NewSnapshot.Status != StatusCompleted || res.NewSnapshot.Kind != KindFull {
		t.Fatalf("published snapshot must be a completed full, got %+v", res.NewSnapshot)
	}
	if len(res.Replacements) != 3 {
		t.Fatalf("expected 3 replacements, got %d", len(res.Replacements))
	}
	for _, r := range res.Replacements {
		if r.NewSnapshotID != res.NewSnapshot.ID {
			t.Fatalf("replacement points at %s, want %s", r.NewSnapshotID, res.NewSnapshot.ID)
		}
	}
	// 发布后：新链 = 新完整快照；旧链查询保持原始回溯。
	eff, err = svc.GetEffectiveChain(ctx, ids[2])
	if err != nil {
		t.Fatalf("effective chain: %v", err)
	}
	if !equalIDs(chainIDs(eff), []string{res.NewSnapshot.ID}) {
		t.Fatalf("effective chain = %v, want [%s]", chainIDs(eff), res.NewSnapshot.ID)
	}
	raw, err := svc.GetBackupChain(ctx, ids[2])
	if err != nil {
		t.Fatalf("raw chain: %v", err)
	}
	if !equalIDs(chainIDs(raw), ids) {
		t.Fatalf("raw chain must stay untouched, got %v", chainIDs(raw))
	}
}

func TestPublishConcurrentCompactionsSingleWinner(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)

	a := createCompaction(t, ctx, svc, ids[0], ids[2], "key-a")
	b := createCompaction(t, ctx, svc, ids[0], ids[2], "key-b")
	runCompactionSteps(t, ctx, svc, a.ID)
	runCompactionSteps(t, ctx, svc, b.ID)

	// 并发发布同一链：恰好一个成为有效结果。
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, taskID := range []string{a.ID, b.ID} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			task, err := svc.GetCompactionTask(ctx, id)
			if err != nil {
				results <- err
				return
			}
			_, err = svc.PublishCompaction(ctx, PublishCompactionInput{
				TaskID: id, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
				ContentDigest: task.ExpectedDigest,
			})
			results <- err
		}(taskID)
	}
	wg.Wait()
	close(results)
	succeeded, conflicts := 0, 0
	for err := range results {
		switch CodeOf(err) {
		case "":
			succeeded++
		case ErrCodeConflict, ErrCodeAlreadyExists:
			conflicts++
		default:
			t.Fatalf("unexpected publish error: %v", err)
		}
	}
	if succeeded != 1 || conflicts != 1 {
		t.Fatalf("exactly one compaction may win, got %d success / %d conflict", succeeded, conflicts)
	}

	reps, err := svc.ListReplacements(ctx, "ds")
	if err != nil {
		t.Fatalf("list replacements: %v", err)
	}
	if len(reps) != 3 {
		t.Fatalf("exactly one replacement set may exist, got %d", len(reps))
	}
}

func TestPublishIdempotentReplay(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, task.ID)
	first := publishCompaction(t, ctx, svc, task.ID)

	// 重复发布（如调用方超时重试）：返回同一结果，不产生第二个快照。
	second, err := svc.PublishCompaction(ctx, PublishCompactionInput{
		TaskID: task.ID, LeaseID: "stale-lease", Epoch: 99,
		ContentDigest: first.Task.ExpectedDigest,
	})
	if err != nil {
		t.Fatalf("idempotent publish replay: %v", err)
	}
	if second.NewSnapshot.ID != first.NewSnapshot.ID || len(second.Replacements) != 3 {
		t.Fatalf("replay must return the same result, got %+v", second)
	}
	snaps, err := svc.ListDatasetSnapshots(ctx, "ds")
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snaps) != 4 {
		t.Fatalf("replay must not create snapshots, got %d", len(snaps))
	}
}

func TestRetentionProtectsActiveCompactionChain(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")

	// 压缩执行期间，即使保留策略一个不留，原链也被冻结保护。
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, d := range run.Decisions {
		if d.Action != RetentionRetained {
			t.Fatalf("snapshot %s must be retained during compaction: %+v", d.SnapshotID, d)
		}
		found := false
		for _, r := range d.Reasons {
			if r == ReasonActiveCompaction {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected reason active_compaction, got %v", d.Reasons)
		}
	}
}

func TestRetentionReclaimsChainAfterPublish(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, task.ID)
	res := publishCompaction(t, ctx, svc, task.ID)

	// 发布后原链不再被压缩任务引用；保留最近 1 个（新完整快照）-> 原链逐个回收。
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	deleted := map[string][]string{}
	retained := map[string][]string{}
	for _, d := range run.Decisions {
		if d.Action == RetentionDeleted {
			deleted[d.SnapshotID] = d.Reasons
		} else {
			retained[d.SnapshotID] = d.Reasons
		}
	}
	for _, id := range ids {
		reasons, ok := deleted[id]
		if !ok {
			t.Fatalf("old chain snapshot %s must be reclaimed, retained reasons=%v", id, retained[id])
		}
		if len(reasons) != 1 || reasons[0] != ReasonReplacedByCompaction {
			t.Fatalf("expected replaced_by_compaction for %s, got %v", id, reasons)
		}
	}
	if _, ok := retained[res.NewSnapshot.ID]; !ok {
		t.Fatalf("new full snapshot must be retained by policy")
	}
	for _, id := range ids {
		if _, err := svc.GetSnapshot(ctx, id); CodeOf(err) != ErrCodeNotFound {
			t.Fatalf("snapshot %s must be gone, err=%v", id, err)
		}
	}
}

func TestRetentionBlockedByChildIncremental(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, task.ID)
	publishCompaction(t, ctx, svc, task.ID)

	// 链外还有增量快照挂在旧链尾：它及其祖先（含被替代的旧链）都不得回收。
	child, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[2], Digest: "ds-inc-c",
	})
	if err != nil {
		t.Fatalf("register child: %v", err)
	}
	if _, err := svc.MarkSnapshotCompleted(ctx, child.ID); err != nil {
		t.Fatalf("complete child: %v", err)
	}

	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}}); err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, id := range ids {
		if _, err := svc.GetSnapshot(ctx, id); err != nil {
			t.Fatalf("snapshot %s referenced by child chain must survive: %v", id, err)
		}
	}
	// 引用阻断原因查询：旧链尾被子快照引用。
	refs, err := svc.ExplainSnapshotReferences(ctx, ids[2])
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	hasChild := false
	for _, b := range refs.Blocks {
		if b.Kind == "child_snapshot" && b.ID == child.ID {
			hasChild = true
		}
	}
	if !hasChild {
		t.Fatalf("expected child_snapshot block, got %+v", refs.Blocks)
	}
	if refs.ReplacedBy == nil {
		t.Fatal("expected replacement info on compacted snapshot")
	}
}

func TestRestoreAndReclamationRaceKeepsChainIntact(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, task.ID)
	publishCompaction(t, ctx, svc, task.ID)

	// 恢复创建先于清理：冻结旧链，清理不得回收。
	restore, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "prod-a", Holder: "worker-1",
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}}); err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, id := range ids {
		if _, err := svc.GetSnapshot(ctx, id); err != nil {
			t.Fatalf("snapshot %s frozen by restore must survive: %v", id, err)
		}
	}
	// 恢复完成后旧链才可回收。
	if _, err := svc.CompleteRestore(ctx, restore.ID, restore.LeaseID, restore.LeaseEpoch); err == nil {
		t.Fatal("restore with pending steps must not complete")
	}
	runCompactionLikeRestore(t, ctx, svc, restore)
	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}}); err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, id := range ids {
		if _, err := svc.GetSnapshot(ctx, id); CodeOf(err) != ErrCodeNotFound {
			t.Fatalf("snapshot %s must be reclaimed after restore finished, err=%v", id, err)
		}
	}
	// 清理之后再创建恢复：得到明确的 not_found，而不是断裂链。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "prod-b", Holder: "worker-1",
	})
	mustCode(t, err, ErrCodeNotFound)
}

// runCompactionLikeRestore 把恢复任务的全部步骤执行成功并完成（测试辅助）。
func runCompactionLikeRestore(t *testing.T, ctx context.Context, svc *Service, task *RestoreTask) {
	t.Helper()
	for {
		d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch restore step: %v", err)
		}
		task = d.Task
		if d.Step == nil {
			break
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatalf("ack restore step: %v", err)
		}
	}
	if _, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err != nil {
		t.Fatalf("complete restore: %v", err)
	}
}

func TestEffectiveChainAppliesNestedCompactions(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2) // root, inc-a, inc-b

	// 第一次压缩：root..inc-b -> F1。
	c1 := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, c1.ID)
	r1 := publishCompaction(t, ctx, svc, c1.ID)
	f1 := r1.NewSnapshot.ID

	// 在新完整快照上继续登记增量，再压缩 F1..inc-d -> F2。
	more := []string{f1}
	parent := f1
	for i, dg := range []string{"ds-inc-c", "ds-inc-d"} {
		sn, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
			DatasetID: "ds", Kind: KindIncremental, ParentID: parent, Digest: dg,
		})
		if err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
		if _, err := svc.MarkSnapshotCompleted(ctx, sn.ID); err != nil {
			t.Fatalf("complete %d: %v", i, err)
		}
		more = append(more, sn.ID)
		parent = sn.ID
	}
	c2 := createCompaction(t, ctx, svc, f1, more[2], "key-2")
	runCompactionSteps(t, ctx, svc, c2.ID)
	r2 := publishCompaction(t, ctx, svc, c2.ID)

	// 新链查询：inc-d 直接落在 F2 上；旧链查询保持原样。
	eff, err := svc.GetEffectiveChain(ctx, more[2])
	if err != nil {
		t.Fatalf("effective chain: %v", err)
	}
	if !equalIDs(chainIDs(eff), []string{r2.NewSnapshot.ID}) {
		t.Fatalf("effective chain = %v, want [%s]", chainIDs(eff), r2.NewSnapshot.ID)
	}
	raw, err := svc.GetBackupChain(ctx, more[2])
	if err != nil {
		t.Fatalf("raw chain: %v", err)
	}
	if !equalIDs(chainIDs(raw), more) {
		t.Fatalf("raw chain = %v, want %v", chainIDs(raw), more)
	}
	// 旧链尾的有效视图停在 F1（其替代段不在该链上，不再继续折叠）。
	effOld, err := svc.GetEffectiveChain(ctx, ids[2])
	if err != nil {
		t.Fatalf("effective chain of old tip: %v", err)
	}
	if !equalIDs(chainIDs(effOld), []string{f1}) {
		t.Fatalf("effective chain of old tip = %v, want [%s]", chainIDs(effOld), f1)
	}
}

func TestCompactionProgressAndReferenceQueries(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")

	p, err := svc.GetCompactionProgress(ctx, task.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if p.Total != 3 || p.Pending != 3 || p.Status != TaskPending {
		t.Fatalf("unexpected progress: %+v", p)
	}

	// 推进一步后：1 成功 / 2 待办。
	d, err := svc.DispatchCompactionStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := svc.AckCompactionStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatalf("ack: %v", err)
	}
	p, err = svc.GetCompactionProgress(ctx, task.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if p.Succeeded != 1 || p.Pending != 2 {
		t.Fatalf("unexpected progress after one step: %+v", p)
	}

	// 引用阻断原因：有效压缩任务冻结整链。
	refs, err := svc.ExplainSnapshotReferences(ctx, ids[0])
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	hasCompaction, hasChild := false, false
	for _, b := range refs.Blocks {
		if b.Kind == "compaction_task" && b.ID == task.ID {
			hasCompaction = true
		}
		if b.Kind == "child_snapshot" && b.ID == ids[1] {
			hasChild = true
		}
	}
	if !hasCompaction || !hasChild {
		t.Fatalf("expected compaction_task and child_snapshot blocks, got %+v", refs.Blocks)
	}

	// 发布后进度视图指向新快照。
	runCompactionSteps(t, ctx, svc, task.ID)
	res := publishCompaction(t, ctx, svc, task.ID)
	p, err = svc.GetCompactionProgress(ctx, task.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if p.Status != TaskSucceeded || p.NewSnapshotID != res.NewSnapshot.ID {
		t.Fatalf("unexpected final progress: %+v", p)
	}
}

func TestCompactionPersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/state.json"

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc := NewService(store)
	ids := registerChain(t, ctx, svc, "ds", 2)
	task := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, task.ID)
	res := publishCompaction(t, ctx, svc, task.ID)
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重开：任务、替代关系、有效链全部恢复；任务号幂等依然生效。
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()
	svc2 := NewService(store2)

	reps, err := svc2.ListReplacements(ctx, "ds")
	if err != nil || len(reps) != 3 {
		t.Fatalf("replacements after restart: %v, %d", err, len(reps))
	}
	eff, err := svc2.GetEffectiveChain(ctx, ids[2])
	if err != nil {
		t.Fatalf("effective chain after restart: %v", err)
	}
	if !equalIDs(chainIDs(eff), []string{res.NewSnapshot.ID}) {
		t.Fatalf("effective chain after restart = %v", chainIDs(eff))
	}
	again, err := svc2.CreateCompaction(ctx, CreateCompactionInput{
		FromSnapshotID: ids[0], ToSnapshotID: ids[2], Holder: "w", IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("idempotent create after restart: %v", err)
	}
	if again.ID != task.ID || again.Status != TaskSucceeded {
		t.Fatalf("idempotency key must survive restart, got %+v", again)
	}
}

func TestCompactionLeaseEventsAudited(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := createCompaction(t, ctx, svc, ids[0], ids[1], "key-1")
	if _, err := svc.TakeoverCompactionLease(ctx, task.ID, "worker-2", "failover"); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	runCompactionSteps(t, ctx, svc, task.ID)
	publishCompaction(t, ctx, svc, task.ID)

	events, err := svc.ListLeaseEvents(ctx, task.ID)
	if err != nil {
		t.Fatalf("lease events: %v", err)
	}
	actions := []string{}
	for _, e := range events {
		actions = append(actions, fmt.Sprintf("%s@%d", e.Action, e.Epoch))
	}
	want := []string{LeaseAcquired + "@1", LeaseTakenOver + "@2", LeaseReleased + "@2"}
	if !equalIDs(actions, want) {
		t.Fatalf("lease audit = %v, want %v", actions, want)
	}
}
