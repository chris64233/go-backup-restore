package backuprestore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// registerTimedChain 登记一条 full + n 个增量的链，每个快照的 CreatedAt 间隔
// 1 小时（登记时钟由外部传入并推进），返回快照 ID（根在前）与各自的完成时间。
func registerTimedChain(t *testing.T, ctx context.Context, svc *Service, clock *testClock, dataset string, incrementals int) ([]string, []time.Time) {
	t.Helper()
	ids := make([]string, 0, incrementals+1)
	times := make([]time.Time, 0, incrementals+1)

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
	times = append(times, clock.Now())

	parent := full.ID
	for i := 0; i < incrementals; i++ {
		clock.Advance(time.Hour)
		inc, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
			DatasetID: dataset, Kind: KindIncremental, ParentID: parent,
			Digest: fmt.Sprintf("%s-inc-%d", dataset, i),
		})
		if err != nil {
			t.Fatalf("register incremental %d: %v", i, err)
		}
		if _, err := svc.MarkSnapshotCompleted(ctx, inc.ID); err != nil {
			t.Fatalf("complete incremental %d: %v", i, err)
		}
		ids = append(ids, inc.ID)
		times = append(times, clock.Now())
		parent = inc.ID
	}
	return ids, times
}

func createPITRestore(t *testing.T, ctx context.Context, svc *Service, dataset, env string, target time.Time) *RestoreTask {
	t.Helper()
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		DatasetID: dataset, TargetTime: target, TargetEnvironment: env, Holder: "worker-1",
	})
	if err != nil {
		t.Fatalf("create point-in-time restore: %v", err)
	}
	return task
}

func frozenIDs(chain []FrozenSnapshot) []string {
	out := make([]string, 0, len(chain))
	for _, f := range chain {
		out = append(out, f.SnapshotID)
	}
	return out
}

// runRestoreSteps 执行恢复任务的全部步骤（遇到失败步骤直接 fatal），完成任务。
func runRestoreSteps(t *testing.T, ctx context.Context, svc *Service, task *RestoreTask) {
	t.Helper()
	for {
		d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		task = d.Task
		if d.Step == nil {
			break
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatalf("ack step %d: %v", d.Step.Index, err)
		}
	}
	if _, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

// ---------------------------------------------------------------------------
// 时间点选择
// ---------------------------------------------------------------------------

func TestPointInTime_Selection(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 3) // t0..t3

	// 再加一个 pending 增量：任何时间点都不允许选中它。
	clock.Advance(time.Hour)
	pending, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: ids[3], Digest: "ds-inc-pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	tPending := clock.Now()
	if pending.Status != StatusPending {
		t.Fatalf("setup snapshot should remain pending, got %s", pending.Status)
	}

	cases := []struct {
		name    string
		target  time.Time
		wantID  string
		wantLen int
		wantErr ErrorCode
	}{
		{"before any snapshot", at[0].Add(-time.Minute), "", 0, ErrCodeNotFound},
		{"exactly at full", at[0], ids[0], 1, ""},
		{"between full and inc1", at[0].Add(30 * time.Minute), ids[0], 1, ""},
		{"exactly at inc1", at[1], ids[1], 2, ""},
		{"between inc1 and inc2", at[1].Add(30 * time.Minute), ids[1], 2, ""},
		{"exactly at inc3", at[3], ids[3], 4, ""},
		{"after inc3 (pending exists)", tPending, ids[3], 4, ""},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task, err := svc.CreateRestore(ctx, CreateRestoreInput{
				DatasetID: "ds", TargetTime: tc.target,
				TargetEnvironment: fmt.Sprintf("env-%d", i), Holder: "w",
			})
			if tc.wantErr != "" {
				mustCode(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			if task.TargetSnapshotID != tc.wantID {
				t.Fatalf("target = %s, want %s", task.TargetSnapshotID, tc.wantID)
			}
			if task.TargetTime == nil || !task.TargetTime.Equal(tc.target.UTC()) {
				t.Fatalf("target time not frozen: %+v", task.TargetTime)
			}
			if len(task.Chain) != tc.wantLen || len(task.LogicalChain) != tc.wantLen || len(task.Steps) != tc.wantLen {
				t.Fatalf("chain lengths = phys %d logical %d steps %d, want %d",
					len(task.Chain), len(task.LogicalChain), len(task.Steps), tc.wantLen)
			}
			if !equalIDs(frozenIDs(task.Chain), ids[:tc.wantLen]) {
				t.Fatalf("physical chain = %v, want %v", frozenIDs(task.Chain), ids[:tc.wantLen])
			}
			// 未压缩时物理链与逻辑链同源，每个节点来源标注为自身。
			for j, f := range task.Chain {
				if f.OriginSnapshotID != f.SnapshotID || f.ViaCompactionTaskID != "" {
					t.Fatalf("physical[%d] unexpected origin annotation: %+v", j, f)
				}
			}
		})
	}
}

func TestPointInTime_InputValidation(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)

	// 两种目标指定方式互斥。
	_, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: "snap-1", DatasetID: "ds",
		TargetTime: time.Now(), TargetEnvironment: "env", Holder: "w",
	})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 两者都不给。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{TargetEnvironment: "env", Holder: "w"})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 给时间不给数据集。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		TargetTime: time.Now(), TargetEnvironment: "env", Holder: "w",
	})
	mustCode(t, err, ErrCodeInvalidArgument)

	// 未知数据集 / 时间窗口内无快照。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		DatasetID: "ghost", TargetTime: time.Now(), TargetEnvironment: "env", Holder: "w",
	})
	mustCode(t, err, ErrCodeNotFound)
}

// ---------------------------------------------------------------------------
// 与压缩链兼容：替代关系折叠进物理计划
// ---------------------------------------------------------------------------

func TestPointInTime_FoldsPublishedCompaction(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2) // full t0, inc1 t1, inc2 t2

	// 压缩 full..inc2 -> F1 并发布。
	ct := createCompaction(t, ctx, svc, ids[0], ids[2], "compact-1")
	runCompactionSteps(t, ctx, svc, ct.ID)
	pub := publishCompaction(t, ctx, svc, ct.ID)
	f1 := pub.NewSnapshot.ID

	// 目标时刻 = inc2：逻辑目标 inc2，逻辑链 [full,inc1,inc2]，
	// 物理计划折叠为单个替代完整快照 F1。
	task := createPITRestore(t, ctx, svc, "ds", "env-a", at[2])
	if task.TargetSnapshotID != ids[2] {
		t.Fatalf("logical target = %s, want %s", task.TargetSnapshotID, ids[2])
	}
	if !equalIDs(frozenIDs(task.LogicalChain), ids) {
		t.Fatalf("logical chain = %v, want %v", frozenIDs(task.LogicalChain), ids)
	}
	if !equalIDs(frozenIDs(task.Chain), []string{f1}) {
		t.Fatalf("physical chain = %v, want [%s]", frozenIDs(task.Chain), f1)
	}
	if task.Chain[0].OriginSnapshotID != ids[2] || task.Chain[0].ViaCompactionTaskID != ct.ID {
		t.Fatalf("physical node must carry origin/via annotation, got %+v", task.Chain[0])
	}
	if len(task.Steps) != 1 || task.Steps[0].SnapshotID != f1 {
		t.Fatalf("steps must be built from physical chain, got %+v", task.Steps)
	}
	runRestoreSteps(t, ctx, svc, task)
}

func TestPointInTime_AfterRetentionUsesArchiveAndReplacements(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)

	ct := createCompaction(t, ctx, svc, ids[0], ids[2], "compact-1")
	runCompactionSteps(t, ctx, svc, ct.ID)
	pub := publishCompaction(t, ctx, svc, ct.ID)
	f1 := pub.NewSnapshot.ID

	// 保留清理：只留最近 1 个已完成（F1），原链全部归档回收。
	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}}); err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, id := range ids {
		if _, err := svc.GetSnapshot(ctx, id); CodeOf(err) != ErrCodeNotFound {
			t.Fatalf("original %s should be reclaimed, err=%v", id, err)
		}
	}

	// 段尾时间点（inc2）：逻辑目标从归档中选出，计划经替代关系折叠到 F1，仍可恢复。
	task := createPITRestore(t, ctx, svc, "ds", "env-a", at[2])
	if task.TargetSnapshotID != ids[2] || !equalIDs(frozenIDs(task.LogicalChain), ids) {
		t.Fatalf("plan should be reconstructed from archive: target=%s logical=%v",
			task.TargetSnapshotID, frozenIDs(task.LogicalChain))
	}
	if !equalIDs(frozenIDs(task.Chain), []string{f1}) {
		t.Fatalf("physical chain = %v, want [%s]", frozenIDs(task.Chain), f1)
	}
	runRestoreSteps(t, ctx, svc, task)

	// 段中间时间点（inc1）：F1 只能表达段尾状态，inc1 实体已回收 ->
	// 明确的冲突（超出可恢复窗口），而不是断裂链或错误数据。
	_, err := svc.CreateRestore(ctx, CreateRestoreInput{
		DatasetID: "ds", TargetTime: at[1], TargetEnvironment: "env-b", Holder: "w",
	})
	mustCode(t, err, ErrCodeConflict)
}

func TestPointInTime_NestedCompactions(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2) // full, inc1, inc2

	c1 := createCompaction(t, ctx, svc, ids[0], ids[2], "c1")
	runCompactionSteps(t, ctx, svc, c1.ID)
	r1 := publishCompaction(t, ctx, svc, c1.ID)
	f1 := r1.NewSnapshot.ID

	// 在 F1 上再登记增量并做第二次压缩 F1..inc3 -> F2。
	clock.Advance(time.Hour)
	inc3, err := svc.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds", Kind: KindIncremental, ParentID: f1, Digest: "ds-inc-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.MarkSnapshotCompleted(ctx, inc3.ID); err != nil {
		t.Fatal(err)
	}
	t3 := clock.Now()
	c2 := createCompaction(t, ctx, svc, f1, inc3.ID, "c2")
	runCompactionSteps(t, ctx, svc, c2.ID)
	r2 := publishCompaction(t, ctx, svc, c2.ID)
	f2 := r2.NewSnapshot.ID

	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}}); err != nil {
		t.Fatalf("retention: %v", err)
	}

	// 最新时间点：折叠两次 -> F2。
	task := createPITRestore(t, ctx, svc, "ds", "env-new", t3)
	if !equalIDs(frozenIDs(task.Chain), []string{f2}) {
		t.Fatalf("physical chain = %v, want [%s]", frozenIDs(task.Chain), f2)
	}
	if task.Chain[0].ViaCompactionTaskID != c2.ID {
		t.Fatalf("physical node should be attributed to outermost compaction, got %+v", task.Chain[0])
	}

	// 旧段尾时间点（inc2）：只能折到 F1，而 F1 已被第二轮清理回收 -> 冲突。
	_, err = svc.CreateRestore(ctx, CreateRestoreInput{
		DatasetID: "ds", TargetTime: at[2], TargetEnvironment: "env-old", Holder: "w",
	})
	mustCode(t, err, ErrCodeConflict)
}

// ---------------------------------------------------------------------------
// 计划冻结：创建后的并发压缩/回收不能更换计划
// ---------------------------------------------------------------------------

func TestPointInTime_PlanFrozenAgainstConcurrentCompaction(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)

	// 1) 先按 inc2 时间点冻结计划（原始链 full,inc1,inc2）。
	task := createPITRestore(t, ctx, svc, "ds", "env", at[2])
	want := append([]string(nil), ids...)
	if !equalIDs(frozenIDs(task.Chain), want) {
		t.Fatalf("initial plan = %v, want %v", frozenIDs(task.Chain), want)
	}

	// 2) 任务执行期间并发压缩同一段并发布：不能更换已冻结计划。
	ct := createCompaction(t, ctx, svc, ids[0], ids[2], "compact-late")
	runCompactionSteps(t, ctx, svc, ct.ID)
	pub := publishCompaction(t, ctx, svc, ct.ID)

	// 3) 保留清理：活动恢复冻结的原始物理链受 active_restore 保护，不得回收。
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 1}})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, id := range ids {
		d := decisionFor(run, id)
		if d == nil || d.Action != RetentionRetained || !hasReason(d, ReasonActiveRestore) {
			t.Fatalf("frozen physical %s must survive concurrent compaction, got %+v", id, d)
		}
	}

	// 4) 重新读任务：计划仍是原始链，步骤针对原始快照执行成功。
	reloaded, err := svc.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !equalIDs(frozenIDs(reloaded.Chain), want) {
		t.Fatalf("plan was swapped by concurrent compaction: %v", frozenIDs(reloaded.Chain))
	}
	if reloaded.TargetSnapshotID != ids[2] {
		t.Fatalf("logical target changed: %s", reloaded.TargetSnapshotID)
	}
	runRestoreSteps(t, ctx, svc, reloaded)

	// 对照：任务结束后新建的时间点恢复才会看到压缩后的计划。
	after := createPITRestore(t, ctx, svc, "ds", "env-2", at[2])
	if !equalIDs(frozenIDs(after.Chain), []string{pub.NewSnapshot.ID}) {
		t.Fatalf("fresh plan should fold to new snapshot, got %v", frozenIDs(after.Chain))
	}
}

// ---------------------------------------------------------------------------
// 取消与执行并发：单一终态 + fencing
// ---------------------------------------------------------------------------

func TestCancelRestore_FencesRunningExecutor(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	registerTimedChain(t, ctx, svc, clock, "ds", 2)
	task := createPITRestore(t, ctx, svc, "ds", "env", clock.Now())

	d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v %+v", err, d.Step)
	}
	runningVersion := d.Step.ExecutionVersion
	oldLease, oldEpoch := task.LeaseID, task.LeaseEpoch

	cancelled, err := svc.CancelRestore(ctx, CancelRestoreInput{
		TaskID: task.ID, Reason: "user abort", By: "operator",
	})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.Status != TaskCancelled || cancelled.CancelledAt == nil ||
		cancelled.CancelReason != "user abort" || cancelled.CancelledBy != "operator" {
		t.Fatalf("unexpected cancelled task: %+v", cancelled)
	}
	if cancelled.LeaseEpoch != oldEpoch+1 || cancelled.LeaseID == oldLease {
		t.Fatalf("cancel must rotate lease: %+v", cancelled)
	}
	if cancelled.Steps[0].Status != StepFailed {
		t.Fatalf("running step must be reset to failed, got %s", cancelled.Steps[0].Status)
	}

	// 旧持有者的在途回执 / 派发 / 完成全部被 fencing 挡下。
	_, err = svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: oldLease, Epoch: oldEpoch,
		StepIndex: 0, ExecutionVersion: runningVersion, Success: true,
	})
	mustCode(t, err, ErrCodeLease)
	_, err = svc.DispatchNextStep(ctx, task.ID, oldLease, oldEpoch)
	mustCode(t, err, ErrCodeLease)
	_, err = svc.CompleteRestore(ctx, task.ID, oldLease, oldEpoch)
	mustCode(t, err, ErrCodeConflict)

	// 终态不可再取消或接管。
	_, err = svc.CancelRestore(ctx, CancelRestoreInput{TaskID: task.ID})
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.TakeoverLease(ctx, task.ID, "w2", "late")
	mustCode(t, err, ErrCodeConflict)

	// 取消的任务不产生完成通知。
	events, err := svc.ListOutboxEvents(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("cancelled task must not emit outbox, got %+v", events)
	}

	// 审计记录 acquired -> cancelled。
	le, err := svc.ListLeaseEvents(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(le) != 2 || le[0].Action != LeaseAcquired || le[1].Action != LeaseCancelled {
		t.Fatalf("unexpected lease audit: %+v", le)
	}

	// 同一环境取消后可以新建恢复。
	if _, err := svc.CreateRestore(ctx, CreateRestoreInput{
		DatasetID: "ds", TargetTime: clock.Now(), TargetEnvironment: "env", Holder: "w2",
	}); err != nil {
		t.Fatalf("new restore after cancel: %v", err)
	}
}

func TestCancelRestore_NotFoundAndAlreadyDone(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)

	_, err := svc.CancelRestore(ctx, CancelRestoreInput{TaskID: "ghost"})
	mustCode(t, err, ErrCodeNotFound)

	_, at := registerTimedChain(t, ctx, svc, clock, "ds", 1)
	task := createPITRestore(t, ctx, svc, "ds", "env", at[1])
	runRestoreSteps(t, ctx, svc, task)

	_, err = svc.CancelRestore(ctx, CancelRestoreInput{TaskID: task.ID})
	mustCode(t, err, ErrCodeConflict)
}

// TestCancelVsComplete_LeavesSingleState 让“完成”与“取消”真正并发竞争：
// 无论谁赢得事务，任务只留下一个终态，且成功通知与取消互斥、不会同时存在。
func TestCancelVsComplete_LeavesSingleState(t *testing.T) {
	ctx := context.Background()

	for iter := 0; iter < 24; iter++ {
		svc, clock := newTestService(t)
		_, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)
		task := createPITRestore(t, ctx, svc, "ds", fmt.Sprintf("env-%d", iter), at[2])

		// 先把全部步骤做成功（任务只差 Complete 最后一跳）。
		for i := 0; i < 3; i++ {
			d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
			if err != nil || d.Step == nil {
				t.Fatalf("dispatch %d: %v", i, err)
			}
			if _, err := svc.AckStep(ctx, AckStepInput{
				TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
				StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
			}); err != nil {
				t.Fatalf("ack %d: %v", i, err)
			}
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		var cancelErr, completeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, cancelErr = svc.CancelRestore(ctx, CancelRestoreInput{TaskID: task.ID, Reason: "race"})
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
		if !final.Status.terminal() {
			t.Fatalf("iter %d: race left non-terminal status %s", iter, final.Status)
		}
		event, _ := svc.GetOutboxByTaskInTest(ctx, task.ID)
		switch final.Status {
		case TaskSucceeded:
			if completeErr != nil {
				t.Fatalf("iter %d: succeeded but complete returned %v", iter, completeErr)
			}
			if cancelErr == nil || CodeOf(cancelErr) != ErrCodeConflict {
				t.Fatalf("iter %d: loser cancel must conflict, got %v", iter, cancelErr)
			}
			if event == nil {
				t.Fatalf("iter %d: succeeded task must have exactly one outbox event", iter)
			}
		case TaskCancelled:
			if cancelErr != nil {
				t.Fatalf("iter %d: cancelled but cancel returned %v", iter, cancelErr)
			}
			if completeErr == nil || CodeOf(completeErr) != ErrCodeConflict {
				t.Fatalf("iter %d: loser complete must conflict, got %v", iter, completeErr)
			}
			if event != nil {
				t.Fatalf("iter %d: cancelled task must not have outbox event", iter)
			}
		}
	}
}

// GetOutboxByTaskInTest 是测试辅助：任务是否有完成通知。
func (s *Service) GetOutboxByTaskInTest(ctx context.Context, taskID string) (*OutboxEvent, error) {
	var out *OutboxEvent
	err := s.store.View(func(tx *Tx) error {
		e, ok := tx.GetOutboxByTask(taskID)
		if ok {
			cp := *e
			out = &cp
		}
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// 失败位置查询与中断恢复（进程重启后不重复应用已完成增量）
// ---------------------------------------------------------------------------

func TestFailureLocation_TrackedAndCleared(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)
	task := createPITRestore(t, ctx, svc, "ds", "env", at[2])

	loc, err := svc.GetFailureLocation(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loc.HasFailure() {
		t.Fatalf("fresh task should have no failure location, got %+v", loc)
	}

	d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d.Step.ExecutionVersion,
		Success: false, Detail: "disk io error",
	}); err != nil {
		t.Fatal(err)
	}
	loc, err = svc.GetFailureLocation(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !loc.HasFailure() || loc.Index != 0 || loc.SnapshotID != ids[0] || loc.Detail != "disk io error" {
		t.Fatalf("unexpected failure location: %+v", loc)
	}

	// 重试成功：失败位置清除。
	d2, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d2.Step == nil || d2.Step.Index != 0 {
		t.Fatalf("re-dispatch: %v %+v", err, d2.Step)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d2.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	loc, _ = svc.GetFailureLocation(ctx, task.ID)
	if loc.HasFailure() {
		t.Fatalf("failure location should clear after retry success, got %+v", loc)
	}
}

func TestPointInTime_ResumeAfterRestartSkipsCompletedSteps(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/state.json"

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc := NewService(store)
	clock := newTestClock()
	svc.WithClock(clock.Now)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)

	// 先压缩发布，使时间点计划为 [F1] + 后两段……这里改为：压缩只覆盖根与 inc1，
	// 保留 inc2 在压缩之后，计划应是 [F1, inc2] 两个物理步骤。
	ct, err := svc.CreateCompaction(ctx, CreateCompactionInput{
		FromSnapshotID: ids[0], ToSnapshotID: ids[1], Holder: "worker-1", IdempotencyKey: "c-partial",
	})
	if err != nil {
		t.Fatalf("create compaction: %v", err)
	}
	runCompactionSteps(t, ctx, svc, ct.ID)
	pub := publishCompaction(t, ctx, svc, ct.ID)

	task := createPITRestore(t, ctx, svc, "ds", "env", at[2])
	wantPhysical := []string{pub.NewSnapshot.ID, ids[2]}
	if !equalIDs(frozenIDs(task.Chain), wantPhysical) {
		t.Fatalf("physical chain = %v, want %v", frozenIDs(task.Chain), wantPhysical)
	}

	// 第 0 步成功，第 1 步派发后执行失败（进程此刻崩溃）。
	d0, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d0.Step == nil || d0.Step.Index != 0 {
		t.Fatalf("dispatch step 0: %v %+v", err, d0.Step)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d0.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	d1, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d1.Step == nil || d1.Step.Index != 1 {
		t.Fatalf("dispatch step 1: %v %+v", err, d1.Step)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: d1.Step.ExecutionVersion,
		Success: false, Detail: "network reset",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重启：计划、步骤状态与失败位置全部从落盘状态恢复。
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	svc2 := NewService(store2)

	resumed, err := svc2.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("load task after restart: %v", err)
	}
	if resumed.Status != TaskRunning {
		t.Fatalf("status after restart = %s, want running", resumed.Status)
	}
	if !equalIDs(frozenIDs(resumed.Chain), wantPhysical) {
		t.Fatalf("physical plan changed across restart: %v", frozenIDs(resumed.Chain))
	}
	if resumed.Steps[0].Status != StepSucceeded {
		t.Fatalf("step 0 must remain succeeded across restart")
	}
	if resumed.Steps[1].Status != StepFailed {
		t.Fatalf("step 1 must remain failed across restart, got %s", resumed.Steps[1].Status)
	}
	loc, err := svc2.GetFailureLocation(ctx, task.ID)
	if err != nil || !loc.HasFailure() || loc.Index != 1 || loc.Detail != "network reset" || loc.SnapshotID != ids[2] {
		t.Fatalf("failure location not recovered: %+v err=%v", loc, err)
	}

	// 继续：只派发失败的第 1 步；已成功的第 0 步绝不重复应用
	//（其 Attempts/ExecutionVersion 保持崩溃前的值）。
	d, err := svc2.DispatchNextStep(ctx, task.ID, resumed.LeaseID, resumed.LeaseEpoch)
	if err != nil {
		t.Fatalf("resume dispatch: %v", err)
	}
	if d.Step == nil || d.Step.Index != 1 {
		t.Fatalf("resume must continue at failed step 1, got %+v", d.Step)
	}
	again, err := svc2.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Steps[0].Attempts != 1 || again.Steps[0].ExecutionVersion != 1 {
		t.Fatalf("succeeded step 0 must not be re-applied: %+v", again.Steps[0])
	}
	if _, err := svc2.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: resumed.LeaseID, Epoch: resumed.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatalf("ack resumed step: %v", err)
	}
	res, err := svc2.CompleteRestore(ctx, task.ID, resumed.LeaseID, resumed.LeaseEpoch)
	if err != nil {
		t.Fatalf("complete after restart: %v", err)
	}
	if res.Task.Status != TaskSucceeded || res.Event == nil {
		t.Fatalf("unexpected completion: %+v", res)
	}
}

// TestPointInTime_RestoreConcurrentWithRetention 时间点恢复创建与保留清理并发：
// 可序列化事务保证——先创建则清理保护冻结计划（active_restore），
// 先清理则创建得到明确错误，绝不会出现计划引用了半回收链的情况。
func TestPointInTime_RestoreConcurrentWithRetention(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	_, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)

	// 预置一个活动恢复冻结整链：并发期间清理只能让路；
	// “先清理则创建报错”的另一侧已由 TestRestoreAndReclamationRaceKeepsChainIntact 覆盖。
	createPITRestore(t, ctx, svc, "ds", "env-seed", at[2])

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var created, conflict, other int
	var mu sync.Mutex
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, err := svc.CreateRestore(ctx, CreateRestoreInput{
				DatasetID: "ds", TargetTime: at[2],
				TargetEnvironment: fmt.Sprintf("env-%d", i), Holder: "w",
			})
			mu.Lock()
			switch CodeOf(err) {
			case "":
				created++
			case ErrCodeNotFound, ErrCodeConflict:
				conflict++
			default:
				other++
			}
			mu.Unlock()
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
			_, _ = svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
		}
	}()
	time.Sleep(120 * time.Millisecond)
	close(stop)
	wg.Wait()

	if other != 0 {
		t.Fatalf("unexpected error codes during race (created=%d rejected=%d other=%d)", created, conflict, other)
	}
	if created == 0 {
		t.Fatalf("expected some restore plans to be created, got 0")
	}
	// 所有成功创建的计划必须仍然可执行：其物理链上的快照一个都不能已被回收。
	tasks, err := svc.ListRestoreTasks(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		for _, f := range task.Chain {
			if _, err := svc.GetSnapshot(ctx, f.SnapshotID); err != nil {
				t.Fatalf("frozen physical %s of %s was reclaimed: %v", f.SnapshotID, task.ID, err)
			}
		}
	}
}

func TestPointInTime_CrashMidStepResumesViaTakeover(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/state.json"

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc := NewService(store)
	clock := newTestClock()
	svc.WithClock(clock.Now)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 2)

	task := createPITRestore(t, ctx, svc, "ds", "env", at[2])
	// 第 0 步成功；第 1 步已派发但执行者直接崩溃（没有任何回执，停留在 running）。
	d0, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d0.Step == nil || d0.Step.Index != 0 {
		t.Fatalf("dispatch step 0: %v %+v", err, d0.Step)
	}
	if _, err := svc.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
		StepIndex: 0, ExecutionVersion: d0.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatal(err)
	}
	d1, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d1.Step == nil || d1.Step.Index != 1 {
		t.Fatalf("dispatch step 1: %v %+v", err, d1.Step)
	}
	oldVersion := d1.Step.ExecutionVersion
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重启：running 步骤的结果未知（可能已落数据），不能直接重派。
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	svc2 := NewService(store2)

	// 监督者接管租约：epoch+1，在途步骤被 fencing 回 failed，失败位置可查。
	taken, err := svc2.TakeoverLease(ctx, task.ID, "worker-1", "process restarted")
	if err != nil {
		t.Fatalf("takeover after restart: %v", err)
	}
	if taken.Steps[0].Status != StepSucceeded || taken.Steps[1].Status != StepFailed {
		t.Fatalf("unexpected steps after takeover: %+v", taken.Steps)
	}
	loc, err := svc2.GetFailureLocation(ctx, task.ID)
	if err != nil || !loc.HasFailure() || loc.Index != 1 || loc.SnapshotID != ids[1] {
		t.Fatalf("failure location after restart takeover: %+v err=%v", loc, err)
	}

	// 崩溃前那次在途尝试的迟到回执（旧租约/旧版本）不能再推进任务。
	_, err = svc2.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: d1.Task.LeaseID, Epoch: d1.Task.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: oldVersion, Success: true,
	})
	mustCode(t, err, ErrCodeLease)

	// 新租约只重派失败的第 1 步；已完成的第 0 步 Attempts 保持 1，不重复应用。
	d, err := svc2.DispatchNextStep(ctx, task.ID, taken.LeaseID, taken.LeaseEpoch)
	if err != nil || d.Step == nil || d.Step.Index != 1 {
		t.Fatalf("resume dispatch: %v %+v", err, d.Step)
	}
	again, err := svc2.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Steps[0].Attempts != 1 {
		t.Fatalf("completed step 0 must not be re-applied after crash: %+v", again.Steps[0])
	}
	if _, err := svc2.AckStep(ctx, AckStepInput{
		TaskID: task.ID, LeaseID: taken.LeaseID, Epoch: taken.LeaseEpoch,
		StepIndex: 1, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
	}); err != nil {
		t.Fatalf("ack resumed step: %v", err)
	}
	// 失败步骤之后未派发的步骤继续顺序执行（此处还有第 2 步）。
	for i := 2; i < len(again.Steps); i++ {
		dd, err := svc2.DispatchNextStep(ctx, task.ID, taken.LeaseID, taken.LeaseEpoch)
		if err != nil || dd.Step == nil || dd.Step.Index != i {
			t.Fatalf("dispatch step %d after resume: %v %+v", i, err, dd.Step)
		}
		if _, err := svc2.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: taken.LeaseID, Epoch: taken.LeaseEpoch,
			StepIndex: i, ExecutionVersion: dd.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatalf("ack step %d: %v", i, err)
		}
	}
	if _, err := svc2.CompleteRestore(ctx, task.ID, taken.LeaseID, taken.LeaseEpoch); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func TestPointInTime_FailureLocationAfterTakeover(t *testing.T) {
	ctx := context.Background()
	svc, clock := newTestService(t)
	ids, at := registerTimedChain(t, ctx, svc, clock, "ds", 1)
	task := createPITRestore(t, ctx, svc, "ds", "env", at[1])

	d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil || d.Step == nil {
		t.Fatalf("dispatch: %v", err)
	}
	taken, err := svc.TakeoverLease(ctx, task.ID, "worker-2", "worker-1 lost")
	if err != nil {
		t.Fatal(err)
	}
	// 接管把在途步骤标记为失败位置，新持有者直接知道从哪里继续。
	loc, err := svc.GetFailureLocation(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !loc.HasFailure() || loc.Index != 0 || loc.SnapshotID != ids[0] {
		t.Fatalf("takeover should mark interrupted step, got %+v", loc)
	}
	runRestoreSteps(t, ctx, svc, taken)
}
