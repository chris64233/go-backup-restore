package backuprestore

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// registerTimedChain 登记并完成一条链，每登记一个快照前推进时钟，
// 使每个快照都有严格递增的完成时间，供时间点选择测试使用。
// 返回快照 ID（根在前）与各自的完成时间（同序）。
func registerTimedChain(t *testing.T, ctx context.Context, svc *Service, clock *testClock, dataset string, n int) ([]string, []time.Time) {
	t.Helper()
	ids := make([]string, 0, n)
	times := make([]time.Time, 0, n)
	var parent string
	for i := 0; i < n; i++ {
		clock.Advance(time.Hour)
		kind := KindFull
		in := RegisterSnapshotInput{DatasetID: dataset, Kind: kind, Digest: fmt.Sprintf("%s-d-%d", dataset, i)}
		if i > 0 {
			in.Kind = KindIncremental
			in.ParentID = parent
		}
		snap, err := svc.RegisterSnapshot(ctx, in)
		if err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
		completed, err := svc.MarkSnapshotCompleted(ctx, snap.ID)
		if err != nil {
			t.Fatalf("complete %d: %v", i, err)
		}
		ids = append(ids, snap.ID)
		times = append(times, *completed.CompletedAt)
		parent = snap.ID
	}
	return ids, times
}

// ---------------------------------------------------------------------------
// 时间点选择
// ---------------------------------------------------------------------------

func TestPointInTimeSelection_PicksLatestCompletedNotAfterTarget(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, completedAt := registerTimedChain(t, ctx, svc, clock, "ds", 3)

	// 目标时间落在第 1、2 个快照之间：选中第 1 个（索引 1），绝不选未来的第 2 个。
	between := completedAt[1].Add(30 * time.Minute)
	sel, err := svc.GetPointInTimeSelection(ctx, "ds", between)
	if err != nil {
		t.Fatalf("selection: %v", err)
	}
	if sel.SelectedSnapshotID != ids[1] {
		t.Fatalf("selected %s, want %s", sel.SelectedSnapshotID, ids[1])
	}

	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: between, TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatalf("create pit restore: %v", err)
	}
	if task.Mode != RestoreModePointInTime || task.SelectedSnapshotID != ids[1] {
		t.Fatalf("task selection wrong: %+v", task)
	}
	if !task.TargetTime.Equal(between) {
		t.Fatalf("frozen target time %s != %s", task.TargetTime, between)
	}
	if len(task.Chain) != 2 { // 此时未压缩：full + 第 1 个增量
		t.Fatalf("plan length = %d, want 2", len(task.Chain))
	}

	// 目标时间恰好等于某快照完成时间：该快照可见（边界含等号）。
	sel, err = svc.GetPointInTimeSelection(ctx, "ds", completedAt[2])
	if err != nil {
		t.Fatalf("selection at exact boundary: %v", err)
	}
	if sel.SelectedSnapshotID != ids[2] {
		t.Fatalf("exact boundary selected %s, want %s", sel.SelectedSnapshotID, ids[2])
	}

	// 早于任何已完成快照：not_found，不得退而选择最近一个文件。
	_, err = svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: completedAt[0].Add(-time.Minute),
		TargetEnvironment: "env2", Holder: "w",
	})
	mustCode(t, err, ErrCodeNotFound)

	// 入参校验。
	_, err = svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetEnvironment: "e", Holder: "w",
	})
	mustCode(t, err, ErrCodeInvalidArgument)
}

func TestPointInTimeSelection_IgnoresIncompleteSnapshots(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, _ := registerTimedChain(t, ctx, svc, clock, "ds", 1)

	// 在已完成快照之后登记一个 pending 与一个 failed，目标时间更晚——仍只选已完成的。
	clock.Advance(time.Hour)
	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[0], Digest: "dg-pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	failed, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[0], Digest: "dg-failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkSnapshotFailed(ctx, failed.ID, "boom"); err != nil {
		t.Fatal(err)
	}

	sel, err := svc.GetPointInTimeSelection(ctx, "ds", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if sel.SelectedSnapshotID != ids[0] {
		t.Fatalf("should ignore pending/failed, selected %s", sel.SelectedSnapshotID)
	}

	// 只有一个 pending 快照、没有任何已完成快照的数据集：not_found。
	_ = pending
	_, err = svc.GetPointInTimeSelection(ctx, "ds-empty", clock.Now())
	mustCode(t, err, ErrCodeNotFound)
}

// ---------------------------------------------------------------------------
// 与压缩链兼容：计划折叠、时间边界、链式压缩与冻结稳定性
// ---------------------------------------------------------------------------

func TestPointInTimeRestore_PlanUsesCompactionReplacements(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2) // full, inc-a, inc-b（同一完成时刻）

	ct := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, ct.ID)
	pub := publishCompaction(t, ctx, svc, ct.ID)

	clock.Advance(time.Minute)
	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: clock.Now(), TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatalf("pit restore: %v", err)
	}
	// 计划折叠为压缩产物单个节点，而不是原链 3 个文件。
	if len(task.Chain) != 1 {
		t.Fatalf("plan should collapse to compaction result, got %d nodes: %+v", len(task.Chain), task.Chain)
	}
	node := task.Chain[0]
	if node.SnapshotID != pub.NewSnapshot.ID || node.Source != FrozenSourceCompaction {
		t.Fatalf("plan node wrong: %+v", node)
	}
	if node.CompactionTaskID != ct.ID {
		t.Fatalf("plan node must reference compaction task, got %q", node.CompactionTaskID)
	}
	wantReplaced := map[string]bool{ids[0]: true, ids[1]: true, ids[2]: true}
	if len(node.ReplacedSnapshots) != 3 {
		t.Fatalf("replaced closure = %v", node.ReplacedSnapshots)
	}
	for _, r := range node.ReplacedSnapshots {
		if !wantReplaced[r] {
			t.Fatalf("unexpected replaced snapshot %s", r)
		}
	}

	// 进度查询给出实际使用的备份链与压缩溯源。
	prog, err := svc.GetRestoreProgress(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prog.SelectedSnapshotID != ids[2] || len(prog.Plan) != 1 {
		t.Fatalf("progress wrong: %+v", prog)
	}
	if prog.Plan[0].CompactionTaskID != ct.ID || len(prog.Plan[0].ReplacedSnapshots) != 3 {
		t.Fatalf("progress plan node wrong: %+v", prog.Plan[0])
	}
}

func TestPointInTimeRestore_AsOfIgnoresReplacementsPublishedAfterTarget(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2) // 完成时刻 t0
	clock.Advance(time.Hour)
	boundary := clock.Now() // t0+1h：快照可见、压缩尚未发布

	ct := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, ct.ID)
	publishCompaction(t, ctx, svc, ct.ID) // 发布时刻 = t0+1h（步骤不推进时钟）

	// 目标时间在压缩发布之前：即使压缩现在已生效，这份历史计划也必须保持原链。
	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: boundary.Add(-time.Minute),
		TargetEnvironment: "env-old", Holder: "w",
	})
	if err != nil {
		t.Fatalf("pit restore as-of: %v", err)
	}
	if got := chainIDsOf(task.Chain); !equalIDs(got, ids) {
		t.Fatalf("as-of plan must stay original chain %v, got %v", ids, got)
	}
	for _, f := range task.Chain {
		if f.Source != FrozenSourceOriginal {
			t.Fatalf("as-of plan node must be original, got %+v", f)
		}
	}

	// 目标时间在发布之后：计划折叠为压缩产物。
	task2, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: boundary.Add(time.Hour),
		TargetEnvironment: "env-new", Holder: "w",
	})
	if err != nil {
		t.Fatalf("pit restore after publish: %v", err)
	}
	if len(task2.Chain) != 1 || task2.Chain[0].Source != FrozenSourceCompaction {
		t.Fatalf("post-publish plan must collapse, got %+v", task2.Chain)
	}
}

func TestPointInTimeRestore_ChainedCompactionClosure(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2) // root, inc-a, inc-b
	c1 := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, c1.ID)
	r1 := publishCompaction(t, ctx, svc, c1.ID)
	f1 := r1.NewSnapshot.ID

	// F1 上继续登记 inc-c、inc-d，再压缩 F1..inc-d -> F2。
	parent := f1
	more := []string{f1}
	for _, dg := range []string{"ds-inc-c", "ds-inc-d"} {
		sn, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
			DatasetID: "ds", Kind: KindIncremental, ParentID: parent, Digest: dg,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := svc.MarkSnapshotCompleted(ctx, sn.ID); err != nil {
			t.Fatal(err)
		}
		more = append(more, sn.ID)
		parent = sn.ID
	}
	c2 := createCompaction(t, ctx, svc, f1, more[2], "key-2")
	runCompactionSteps(t, ctx, svc, c2.ID)
	r2 := publishCompaction(t, ctx, svc, c2.ID)

	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: taskNowPlus(svc, time.Minute),
		TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatalf("pit restore: %v", err)
	}
	if len(task.Chain) != 1 || task.Chain[0].SnapshotID != r2.NewSnapshot.ID {
		t.Fatalf("plan should collapse to F2, got %+v", task.Chain)
	}
	// 传递闭包：F2 折叠了 F1/inc-c/inc-d，而 F1 又折叠了 root/inc-a/inc-b。
	closure := map[string]bool{}
	for _, id := range task.Chain[0].ReplacedSnapshots {
		closure[id] = true
	}
	for _, id := range append(ids, f1, more[1], more[2]) {
		if !closure[id] {
			t.Fatalf("chained closure missing %s; got %v", id, task.Chain[0].ReplacedSnapshots)
		}
	}
}

// taskNowPlus 通过服务时钟取“当前 + d”的时间（压缩刚发布，默认时钟下
// 目标时间必然晚于一切替代发布时间）。
func taskNowPlus(svc *Service, d time.Duration) time.Time {
	return svc.now().Add(d)
}

func chainIDsOf(chain []FrozenSnapshot) []string {
	ids := make([]string, 0, len(chain))
	for _, f := range chain {
		ids = append(ids, f.SnapshotID)
	}
	return ids
}

func TestPointInTimeSelection_LaterCompactionFullDoesNotOvertakeNewerIncremental(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	// full -> inc-a -> inc-b，三者完成于同一时刻 t0。
	ids := registerChain(t, ctx, svc, "ds", 2)

	// 很久以后才压缩前缀 full..inc-a 合成 F1：F1 物理诞生时间远晚于 inc-b，
	// 但它表达的数据状态只到 inc-a。绝不能因为 F1 “更新”就选它而丢掉 inc-b。
	clock.Advance(2 * time.Hour)
	ct := createCompaction(t, ctx, svc, ids[0], ids[1], "key-late")
	runCompactionSteps(t, ctx, svc, ct.ID)
	pub := publishCompaction(t, ctx, svc, ct.ID)
	clock.Advance(time.Hour)

	sel, err := svc.GetPointInTimeSelection(ctx, "ds", clock.Now())
	if err != nil {
		t.Fatalf("selection: %v", err)
	}
	if sel.SelectedSnapshotID != ids[2] {
		t.Fatalf("must select newest data version inc-b (%s), got %s", ids[2], sel.SelectedSnapshotID)
	}

	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: clock.Now(), TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatalf("pit restore: %v", err)
	}
	// 执行计划在物理层折叠前缀：F1（=full..inc-a）+ inc-b，既用压缩产物又不丢增量。
	if len(task.Chain) != 2 {
		t.Fatalf("plan = %v, want [compaction F1, inc-b]", chainIDsOf(task.Chain))
	}
	if task.Chain[0].SnapshotID != pub.NewSnapshot.ID || task.Chain[0].Source != FrozenSourceCompaction {
		t.Fatalf("plan node 0 should be compaction F1, got %+v", task.Chain[0])
	}
	if task.Chain[1].SnapshotID != ids[2] || task.Chain[1].Source != FrozenSourceOriginal {
		t.Fatalf("plan node 1 should remain inc-b, got %+v", task.Chain[1])
	}
}

func TestPointInTimeRestore_FrozenPlanSurvivesLaterCompactionAndRetention(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)

	// 压缩发布之前先创建时间点恢复：计划冻结原始链。
	boundary := clock.Now().Add(time.Hour)
	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: boundary, TargetEnvironment: "env", Holder: "w",
	})
	if err != nil {
		t.Fatalf("pit restore: %v", err)
	}
	if got := chainIDsOf(task.Chain); !equalIDs(got, ids) {
		t.Fatalf("initial plan = %v, want %v", got, ids)
	}

	// 并发链压缩随后发布成功：不得更换这份计划。
	ct := createCompaction(t, ctx, svc, ids[0], ids[2], "key-1")
	runCompactionSteps(t, ctx, svc, ct.ID)
	publishCompaction(t, ctx, svc, ct.ID)

	reloaded, err := svc.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := chainIDsOf(reloaded.Chain); !equalIDs(got, ids) {
		t.Fatalf("frozen plan was swapped by concurrent compaction: %v", got)
	}
	if reloaded.TargetSnapshotID != ids[2] {
		t.Fatalf("frozen target changed: %s", reloaded.TargetSnapshotID)
	}

	// 计划引用的数据在任务结束前不得回收（即使保留策略一个不留、且已被压缩替代）。
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, id := range ids {
		d := decisionFor(run, id)
		if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonActiveRestore) {
			t.Fatalf("frozen snapshot %s must be retained active_restore, got %+v", id, d)
		}
		if _, err := svc.GetSnapshot(ctx, id); err != nil {
			t.Fatalf("frozen snapshot %s was reclaimed: %v", id, err)
		}
	}
}

// ---------------------------------------------------------------------------
// 取消与执行并发：唯一终态 + 可查询链与失败位置
// ---------------------------------------------------------------------------

func TestCancelRestore_LeavesSingleTerminalState(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	ids := registerChain(t, ctx, svc, "ds", 2)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env", Holder: "w1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// 先让第 0 步成功、第 1 步失败，制造一个明确的失败位置。
	d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch 0: %v", err)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	d, err = svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch 1: %v", err)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: d.Step.ExecutionVersion, Success: false, Detail: "disk full",
	}); err != nil {
		t.Fatal(err)
	}

	canceled, err := svc.CancelRestore(ctx, task.ID, "ops", "user aborted")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if canceled.Status != TaskCanceled || canceled.CanceledAt == nil || canceled.CancelReason != "user aborted" {
		t.Fatalf("canceled task wrong: %+v", canceled)
	}

	// 取消后一切执行动作都被拒绝，且不会把状态改成第二个终态。
	_, err = svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	})
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.TakeoverLease(ctx, task.ID, "w2", "too late")
	mustCode(t, err, ErrCodeConflict)

	// 重复取消幂等，状态仍然只有 canceled 一个。
	again, err := svc.CancelRestore(ctx, task.ID, "ops", "user aborted")
	if err != nil {
		t.Fatalf("idempotent cancel: %v", err)
	}
	if again.Status != TaskCanceled {
		t.Fatalf("status changed on repeated cancel: %s", again.Status)
	}

	// 取消后仍能查到实际使用的备份链与失败位置。
	prog, err := svc.GetRestoreProgress(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prog.Status != TaskCanceled || len(prog.Plan) != 3 {
		t.Fatalf("progress after cancel wrong: %+v", prog)
	}
	if prog.FirstFailed == nil || prog.FirstFailed.Index != 1 || prog.FirstFailed.LastDetail != "disk full" {
		t.Fatalf("failure location wrong: %+v", prog.FirstFailed)
	}
	if prog.CompletedSteps != 1 {
		t.Fatalf("completed steps = %d, want 1", prog.CompletedSteps)
	}

	// 取消不写完成 outbox。
	events, err := svc.ListOutboxEvents(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("canceled restore must not emit completion outbox, got %d", len(events))
	}

	// 已成功的任务不能再取消。
	ids2 := registerChain(t, ctx, svc, "ds2", 0)
	t2, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids2[0], TargetEnvironment: "env2", Holder: "w",
	})
	if err != nil {
		t.Fatal(err)
	}
	runCompactionLikeRestore(t, ctx, svc, t2)
	_, err = svc.CancelRestore(ctx, t2.ID, "ops", "late")
	mustCode(t, err, ErrCodeConflict)
}

func TestCancelVsComplete_ExactlyOneTerminalState(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	// 多轮竞速：取消与完成在同一可序列化存储上并发，最终必须恰好一个终态。
	for round := 0; round < 20; round++ {
		ids := registerChain(t, ctx, svc, fmt.Sprintf("ds-r%d", round), 1)
		task, err := svc.CreateRestore(ctx, CreateRestoreInput{
			TargetSnapshotID:  ids[1],
			TargetEnvironment: fmt.Sprintf("env-r%d", round), Holder: "w",
		})
		if err != nil {
			t.Fatal(err)
		}
		runCompactionLikeRestoreStepsOnly(t, ctx, svc, task)

		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelErr, completeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, cancelErr = svc.CancelRestore(ctx, task.ID, "ops", "race")
		}()
		go func() {
			defer wg.Done()
			<-start
			_, completeErr = svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		}()
		close(start)
		wg.Wait()

		final, err := svc.GetRestoreTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch final.Status {
		case TaskSucceeded:
			if cancelErr == nil || completeErr != nil {
				t.Fatalf("round %d: status succeeded but cancelErr=%v completeErr=%v", round, cancelErr, completeErr)
			}
		case TaskCanceled:
			if completeErr == nil || cancelErr != nil {
				t.Fatalf("round %d: status canceled but cancelErr=%v completeErr=%v", round, cancelErr, completeErr)
			}
		default:
			t.Fatalf("round %d: unexpected terminal status %s", round, final.Status)
		}
	}
}

// TestCancelVsExecutionConcurrent 让取消与持续派发/回执并发，验证最终状态一致、
// 已成功步骤数与落库状态吻合（不会出现取消后仍有步骤被推进）。
func TestCancelVsExecutionConcurrent(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	for round := 0; round < 20; round++ {
		ids := registerChain(t, ctx, svc, fmt.Sprintf("ds-c%d", round), 5)
		task, err := svc.CreateRestore(ctx, CreateRestoreInput{
			TargetSnapshotID:  ids[5],
			TargetEnvironment: fmt.Sprintf("env-c%d", round), Holder: "w",
		})
		if err != nil {
			t.Fatal(err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		// 执行方：不断派发并成功回执当前步骤。
		go func() {
			defer wg.Done()
			leaseID, epoch := task.LeaseID, task.LeaseEpoch
			for {
				select {
				case <-stop:
					return
				default:
				}
				d, err := svc.DispatchNextStep(ctx, task.ID, leaseID, epoch)
				if err != nil {
					continue // 取消提交后派发开始返回 conflict
				}
				if d.Step == nil {
					continue
				}
				_, _ = svc.AckStep(ctx, AckStepInput{
					TaskID: task.ID, LeaseID: leaseID, Epoch: epoch,
					StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
				})
			}
		}()
		// 取消方。
		go func() {
			defer wg.Done()
			<-stop // 由主 goroutine 触发取消
			_, _ = svc.CancelRestore(ctx, task.ID, "ops", "concurrent cancel")
		}()
		// 让执行方跑一小会儿再取消。
		time.Sleep(2 * time.Millisecond)
		close(stop)
		wg.Wait()

		final, err := svc.GetRestoreTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if final.Status != TaskCanceled {
			t.Fatalf("round %d: expected canceled, got %s", round, final.Status)
		}
		// 取消后再无任何步骤可以推进：落库的成功步骤数必须稳定。
		succeededBefore := 0
		for _, st := range final.Steps {
			if st.Status == StepSucceeded {
				succeededBefore++
			}
		}
		if d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err == nil && d.Step != nil {
			t.Fatalf("round %d: dispatch after cancel advanced a step", round)
		}
		again, err := svc.GetRestoreTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		succeededAfter := 0
		for _, st := range again.Steps {
			if st.Status == StepSucceeded {
				succeededAfter++
			}
		}
		if succeededAfter != succeededBefore {
			t.Fatalf("round %d: steps advanced after cancel: %d -> %d", round, succeededBefore, succeededAfter)
		}
	}
}

// runCompactionLikeRestoreStepsOnly 只把恢复任务的步骤全部跑成功，不做完成收尾。
func runCompactionLikeRestoreStepsOnly(t *testing.T, ctx context.Context, svc *Service, task *RestoreTask) {
	t.Helper()
	for {
		d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		task = d.Task
		if d.Step == nil {
			return
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// 中断恢复：持久化、重启后从安全位置继续、不重复应用已完成增量
// ---------------------------------------------------------------------------

func TestPointInTimeRestore_ResumesAcrossRestartWithoutReapplying(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store)
	ids := registerChain(t, ctx, svc, "ds", 3) // full, inc-a, inc-b, inc-c
	task, err := svc.CreatePointInTimeRestore(ctx, CreatePointInTimeRestoreInput{
		DatasetID: "ds", TargetTime: svc.now().Add(time.Minute),
		TargetEnvironment: "env", Holder: "worker-1",
	})
	if err != nil {
		t.Fatalf("pit restore: %v", err)
	}
	if len(task.Chain) != 4 {
		t.Fatalf("plan length = %d, want 4", len(task.Chain))
	}

	// 第 0 步成功；第 1 步派发后执行方崩溃（停留 running）；第 2 步曾失败一次。
	d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step.Index != 0 {
		t.Fatalf("dispatch 0: %v", err)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	d, err = svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step.Index != 1 {
		t.Fatalf("dispatch 1: %v", err)
	}
	crashedVersion := d.Step.ExecutionVersion
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：计划与步骤状态全部恢复。
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	svc2 := NewService(store2)

	task, err = svc2.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if task.Mode != RestoreModePointInTime || len(task.Chain) != 4 || task.Status != TaskRunning {
		t.Fatalf("task state after restart wrong: %+v", task)
	}
	if task.Steps[0].Status != StepSucceeded || task.Steps[1].Status != StepRunning {
		t.Fatalf("step states after restart wrong: %s / %s", task.Steps[0].Status, task.Steps[1].Status)
	}
	prog, err := svc2.GetRestoreProgress(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if prog.CompletedSteps != 1 || prog.FirstFailed == nil || prog.FirstFailed.Index != 1 {
		t.Fatalf("resume position wrong: %+v", prog)
	}

	// 新执行方接管：在途的第 1 步重置为可重试，执行版本提升；第 0 步保持成功。
	taken, err := svc2.TakeoverLease(ctx, task.ID, "worker-2", "worker-1 crashed")
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if taken.Steps[0].Status != StepSucceeded {
		t.Fatalf("completed step must not be reset: %s", taken.Steps[0].Status)
	}
	if taken.Steps[1].Status != StepFailed {
		t.Fatalf("running step should be reset to failed, got %s", taken.Steps[1].Status)
	}

	// 崩溃前在途尝试的迟到回执不能再推进任务。
	_, err = svc2.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: crashedVersion, Success: true,
	})
	mustCode(t, err, ErrCodeLease)

	// 续跑：派发必须从第 1 步开始，绝不重新应用已成功的第 0 步。
	d, err = svc2.DispatchNextStep(ctx, task.ID, taken.LeaseID, taken.LeaseEpoch)
	if err != nil {
		t.Fatalf("dispatch after restart: %v", err)
	}
	if d.Step == nil || d.Step.Index != 1 {
		t.Fatalf("resume must continue at step 1, got %+v", d.Step)
	}
	if d.Step.ExecutionVersion <= crashedVersion {
		t.Fatalf("execution version must advance past crashed attempt: %d <= %d",
			d.Step.ExecutionVersion, crashedVersion)
	}

	// 跑完剩余步骤并完成；只应有一条 outbox。
	for {
		if d.Step != nil {
			if _, err := svc2.AckStep(ctx, AckStepInput{
				TaskID: task.ID, LeaseID: taken.LeaseID, Epoch: taken.LeaseEpoch,
				StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
			}); err != nil {
				t.Fatalf("ack step %d: %v", d.Step.Index, err)
			}
		}
		var err error
		d, err = svc2.DispatchNextStep(ctx, task.ID, taken.LeaseID, taken.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		if d.Step == nil {
			break
		}
	}
	res, err := svc2.CompleteRestore(ctx, task.ID, taken.LeaseID, taken.LeaseEpoch)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if res.Task.Status != TaskSucceeded {
		t.Fatalf("final status = %s", res.Task.Status)
	}
	events, err := svc2.ListOutboxEvents(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("exactly one outbox event expected across restart, got %d", len(events))
	}

	// 终态计划保持重启前那份（时间点选择可复现）。
	if got := chainIDsOf(res.Task.Chain); !equalIDs(got, ids) {
		t.Fatalf("final plan %v != original %v", got, ids)
	}
}
