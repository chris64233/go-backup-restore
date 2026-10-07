package backuprestore

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// createSucceededRestore 登记链、创建恢复并把全部步骤执行成功，
// 返回服务与任务（任务尚未 Available，等待校验发布）。
func createSucceededRestore(t *testing.T, ctx context.Context, svc *Service, dataset, env string, incrementals int) *RestoreTask {
	t.Helper()
	ids := registerChain(t, ctx, svc, dataset, incrementals)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[len(ids)-1], TargetEnvironment: env, Holder: "worker-1",
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	for {
		d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		if d.Step == nil {
			break
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion,
			Success: true,
		}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if _, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err != nil {
		t.Fatalf("complete restore: %v", err)
	}
	got, err := svc.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Available {
		t.Fatalf("restore dir must not be available before verification is published")
	}
	return got
}

func sampleManifest() []RestoreVerificationFileInput {
	return []RestoreVerificationFileInput{
		{Path: "data/a.db", Size: 100, Digest: "sha256:file-a", BlockDigests: []string{"a-b0", "a-b1"}},
		{Path: "data/b.db", Size: 50, Digest: "sha256:file-b", BlockDigests: []string{"b-b0"}},
	}
}

// reportAll 按清单回报全部数据块：observed 摘要由 override 决定，默认等于期望值。
func reportAll(t *testing.T, ctx context.Context, svc *Service, v *RestoreVerification,
	override map[string]string) *RestoreVerification {
	t.Helper()
	results := []BlockVerificationResult{}
	for _, f := range v.Manifest {
		for bi := 0; bi < f.Blocks; bi++ {
			expected := v.Blocks[fileBlockOffset(v.Manifest, f.Index)+bi].ExpectedDigest
			observed := expected
			if got, ok := override[fmt.Sprintf("%d/%d", f.Index, bi)]; ok {
				observed = got
			}
			results = append(results, BlockVerificationResult{
				FileIndex: f.Index, BlockIndex: bi, Success: true, ObservedDigest: observed,
			})
		}
	}
	out, err := svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID, Results: results,
	})
	if err != nil {
		t.Fatalf("report blocks: %v", err)
	}
	return out
}

func TestCreateVerification_FreezesScopeAndIdempotency(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	task := createSucceededRestore(t, ctx, svc, "ds", "env-a", 2)
	target := time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC)

	in := CreateRestoreVerificationInput{
		IdempotencyKey: "verify-1", TaskID: task.ID,
		TargetTime: target, OutputVersion: "v1", Files: sampleManifest(),
	}
	v, err := svc.CreateRestoreVerification(ctx, in)
	if err != nil {
		t.Fatalf("create verification: %v", err)
	}
	if v.Status != VerificationRunning || len(v.RestoreChain) != len(task.Chain) {
		t.Fatalf("unexpected initial verification: status=%s chain=%d", v.Status, len(v.RestoreChain))
	}
	if got := v.Blocks[0]; got.ExpectedDigest != "a-b0" || got.Status != BlockPending {
		t.Fatalf("block not initialized from frozen manifest: %+v", got)
	}

	// 相同请求重复提交：返回原记录。
	same, err := svc.CreateRestoreVerification(ctx, in)
	if err != nil {
		t.Fatalf("duplicate submission must be idempotent: %v", err)
	}
	if same.ID != v.ID {
		t.Fatalf("duplicate submission returned new id %s, want %s", same.ID, v.ID)
	}

	// 输出版本变化：冲突，不静默复用。
	changed := in
	changed.OutputVersion = "v2"
	_, err = svc.CreateRestoreVerification(ctx, changed)
	mustCode(t, err, ErrCodeConflict)

	// 恢复计划（任务）变化：冲突。
	changed = in
	other := createSucceededRestore(t, ctx, svc, "ds-other", "env-b", 1)
	changed.TaskID = other.ID
	_, err = svc.CreateRestoreVerification(ctx, changed)
	mustCode(t, err, ErrCodeConflict)

	// 清单变化（数据块摘要差异）：冲突。
	changed = in
	changed.Files = []RestoreVerificationFileInput{
		{Path: "data/a.db", Size: 100, Digest: "sha256:file-a", BlockDigests: []string{"a-b0", "DIFFERENT"}},
		{Path: "data/b.db", Size: 50, Digest: "sha256:file-b", BlockDigests: []string{"b-b0"}},
	}
	_, err = svc.CreateRestoreVerification(ctx, changed)
	mustCode(t, err, ErrCodeConflict)

	// 目标时间变化：冲突。
	changed = in
	changed.TargetTime = target.Add(time.Minute)
	_, err = svc.CreateRestoreVerification(ctx, changed)
	mustCode(t, err, ErrCodeConflict)

	// 输入校验：未完成的恢复任务不能校验、目标时间必填。
	ids := registerChain(t, ctx, svc, "ds2", 0)
	running, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[0], TargetEnvironment: "env-c", Holder: "w",
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	_, err = svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-bad-task", TaskID: running.ID,
		TargetTime: target, OutputVersion: "v1", Files: sampleManifest(),
	})
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-bad-time", TaskID: task.ID,
		OutputVersion: "v1", Files: sampleManifest(),
	})
	mustCode(t, err, ErrCodeInvalidArgument)
}

func TestVerification_PartialFailureRetryAndPublishGate(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	task := createSucceededRestore(t, ctx, svc, "ds", "env-a", 1)

	v, err := svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-1", TaskID: task.ID,
		TargetTime:    time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC),
		OutputVersion: "v1", Files: sampleManifest(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 第一批：一个块通过、一个块摘要不一致、一个块读取失败 —— 部分成功，保持不可用。
	batch1, err := svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results: []BlockVerificationResult{
			{FileIndex: 0, BlockIndex: 0, Success: true, ObservedDigest: "a-b0"},
			{FileIndex: 0, BlockIndex: 1, Success: true, ObservedDigest: "CORRUPT"},
			{FileIndex: 1, BlockIndex: 0, Success: false, Detail: "read timeout"},
		},
	})
	if err != nil {
		t.Fatalf("report batch1: %v", err)
	}
	if batch1.Blocks[0].Status != BlockMatched ||
		batch1.Blocks[1].Status != BlockFailed ||
		batch1.Blocks[2].Status != BlockFailed {
		t.Fatalf("partial batch statuses wrong: %s %s %s",
			batch1.Blocks[0].Status, batch1.Blocks[1].Status, batch1.Blocks[2].Status)
	}
	if batch1.Blocks[1].Attempts != 1 || batch1.Blocks[2].Attempts != 1 {
		t.Fatalf("attempts should increase on each report")
	}

	// 部分成功不能发布，错误必须指出具体文件/数据块。
	_, err = svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1",
	})
	mustCode(t, err, ErrCodeConflict)

	// 查询视图展示恢复来源与摘要差异，且 Available=false。
	view, err := svc.GetRestoreVerification(ctx, v.ID)
	if err != nil {
		t.Fatalf("get view: %v", err)
	}
	if view.Available || view.Blocks.Failed != 2 || view.Blocks.Matched != 1 || view.Blocks.Pending != 0 {
		t.Fatalf("unexpected view before publish: available=%v summary=%+v", view.Available, view.Blocks)
	}
	if len(view.Blocks.Mismatches) != 2 {
		t.Fatalf("want 2 mismatches, got %d", len(view.Blocks.Mismatches))
	}
	m0 := view.Blocks.Mismatches[0]
	if m0.Path != "data/a.db" || m0.ExpectedDigest != "a-b1" || m0.ObservedDigest != "CORRUPT" {
		t.Fatalf("mismatch must point at concrete file/block: %+v", m0)
	}
	if view.Source.TaskID != task.ID || len(view.Source.FrozenChain) != len(task.Chain) ||
		view.Source.TargetSnapshotID != task.TargetSnapshotID {
		t.Fatalf("restore source not exposed correctly: %+v", view.Source)
	}

	// matched 块不可被不同结论覆盖。
	_, err = svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results:        []BlockVerificationResult{{FileIndex: 0, BlockIndex: 0, Success: true, ObservedDigest: "TAMPERED"}},
	})
	mustCode(t, err, ErrCodeConflict)

	// 失败块重试：读失败块重报通过；损坏块首次仍失败，再次重试后通过。
	if _, err := svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results:        []BlockVerificationResult{{FileIndex: 1, BlockIndex: 0, Success: true, ObservedDigest: "b-b0"}},
	}); err != nil {
		t.Fatalf("retry b: %v", err)
	}
	if _, err := svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results:        []BlockVerificationResult{{FileIndex: 0, BlockIndex: 1, Success: true, ObservedDigest: "STILL-BAD"}},
	}); err != nil {
		t.Fatalf("retry a-b1 fail: %v", err)
	}
	v, err = svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results:        []BlockVerificationResult{{FileIndex: 0, BlockIndex: 1, Success: true, ObservedDigest: "a-b1"}},
	})
	if err != nil {
		t.Fatalf("retry a-b1 ok: %v", err)
	}
	for _, b := range v.Blocks {
		if b.Status != BlockMatched {
			t.Fatalf("block %d/%d still %s after retries", b.FileIndex, b.BlockIndex, b.Status)
		}
	}
	if v.Blocks[1].Attempts != 3 {
		t.Fatalf("retried block attempts want 3, got %d", v.Blocks[1].Attempts)
	}

	// 输出版本被改动时拒绝发布。
	_, err = svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v.ID, OutputVersion: "v9",
	})
	mustCode(t, err, ErrCodeConflict)

	// 全部摘要通过后才发布；发布后恢复目录可用。
	pub, err := svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if !pub.Task.Available || pub.Task.PublishedVerificationID != v.ID {
		t.Fatalf("restore dir must be marked available by published verification: %+v", pub.Task)
	}

	// 发布后回报/重启都被拒绝。
	_, err = svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results:        []BlockVerificationResult{{FileIndex: 0, BlockIndex: 0, Success: true, ObservedDigest: "a-b0"}},
	})
	mustCode(t, err, ErrCodeConflict)
	_, err = svc.RestartVerification(ctx, RestartVerificationInput{VerificationID: v.ID, OutputVersion: "v1"})
	mustCode(t, err, ErrCodeConflict)

	// 重复发布幂等，返回同一结论。
	again, err := svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1",
	})
	if err != nil {
		t.Fatalf("idempotent republish: %v", err)
	}
	if again.Verification.ID != v.ID || !again.Task.Available {
		t.Fatalf("republish must return same published result")
	}
}

func TestVerification_RestartReusesMatchedBlocksButRechecksFailed(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	task := createSucceededRestore(t, ctx, svc, "ds", "env-a", 0)

	v, err := svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-1", TaskID: task.ID,
		TargetTime:    time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC),
		OutputVersion: "v1", Files: sampleManifest(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	v = reportAll(t, ctx, svc, v, map[string]string{"0/1": "CORRUPT"})

	// 输出版本变化时重启直接冲突：避免生成互相矛盾的目录。
	_, err = svc.RestartVerification(ctx, RestartVerificationInput{VerificationID: v.ID, OutputVersion: "v2"})
	mustCode(t, err, ErrCodeConflict)

	// 重启：matched 块保留复用，failed 块重置为 pending。
	restarted, err := svc.RestartVerification(ctx, RestartVerificationInput{VerificationID: v.ID, OutputVersion: "v1"})
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if restarted.Blocks[0].Status != BlockMatched || restarted.Blocks[2].Status != BlockMatched {
		t.Fatalf("matched blocks must be reused: %+v", restarted.Blocks)
	}
	if restarted.Blocks[1].Status != BlockPending || restarted.Blocks[1].ObservedDigest != "" {
		t.Fatalf("failed block must be reset to pending for recheck: %+v", restarted.Blocks[1])
	}

	// 只需重新确认曾失败的块即可发布。
	if _, err := svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results:        []BlockVerificationResult{{FileIndex: 0, BlockIndex: 1, Success: true, ObservedDigest: "a-b1"}},
	}); err != nil {
		t.Fatalf("recheck failed block: %v", err)
	}
	pub, err := svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1",
	})
	if err != nil {
		t.Fatalf("publish after restart: %v", err)
	}
	if !pub.Task.Available {
		t.Fatalf("task must be available after restarted verification publishes")
	}
}

func TestPublishVerification_ConcurrentSingleWinner(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	task := createSucceededRestore(t, ctx, svc, "ds", "env-a", 0)

	// 两条独立校验记录（代表两次重跑），都把全部数据块报为通过。
	createAndPass := func(key string) string {
		v, err := svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
			IdempotencyKey: key, TaskID: task.ID,
			TargetTime:    time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC),
			OutputVersion: "v1", Files: sampleManifest(),
		})
		if err != nil {
			t.Fatalf("create %s: %v", key, err)
		}
		reportAll(t, ctx, svc, v, nil)
		return v.ID
	}
	v1 := createAndPass("verify-1")
	v2 := createAndPass("verify-2")

	// 并发发布：存储串行化事务，恰好一个赢，另一个 conflict，已发布结果不被替换。
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, 2)
	publish := func(id string, slot int) {
		defer wg.Done()
		<-start
		_, errs[slot] = svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
			VerificationID: id, OutputVersion: "v1",
		})
	}
	wg.Add(2)
	go publish(v1, 0)
	go publish(v2, 1)
	close(start)
	wg.Wait()

	codes := map[ErrorCode]int{}
	for _, e := range errs {
		codes[CodeOf(e)]++
	}
	if codes[""] != 1 || codes[ErrCodeConflict] != 1 {
		t.Fatalf("want exactly 1 success and 1 conflict, got %v", errs)
	}

	got, err := svc.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !got.Available || (got.PublishedVerificationID != v1 && got.PublishedVerificationID != v2) {
		t.Fatalf("published verification pointer wrong: %+v", got)
	}

	// 后来的重跑记录仍可查询（保留为新记录），但永远不可发布、不影响已发布结论。
	loser := v1
	if got.PublishedVerificationID == v1 {
		loser = v2
	}
	lv, err := svc.GetRestoreVerification(ctx, loser)
	if err != nil {
		t.Fatalf("get loser: %v", err)
	}
	if lv.Available || lv.Status != VerificationRunning {
		t.Fatalf("losing rerun must stay unpublished/unavailable, got %s available=%v", lv.Status, lv.Available)
	}

	// 已发布结果不能被静默替换：新记录（新目标时间/输出版本）全部通过也只能 conflict。
	v3, err := svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-3", TaskID: task.ID,
		TargetTime:    time.Date(2026, 9, 26, 9, 0, 0, 0, time.UTC),
		OutputVersion: "v2", Files: sampleManifest(),
	})
	if err != nil {
		t.Fatalf("create verify-3: %v", err)
	}
	reportAll(t, ctx, svc, v3, nil)
	_, err = svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v3.ID, OutputVersion: "v2",
	})
	mustCode(t, err, ErrCodeConflict)

	// 列表展示该恢复任务的全部校验记录（已发布 + 后续重跑的新差异记录）。
	list, err := svc.ListRestoreVerifications(ctx, task.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("want 3 verification records kept for task, got %d", len(list))
	}
	publishedCount := 0
	for _, item := range list {
		if item.Status == VerificationPublished {
			publishedCount++
		}
	}
	if publishedCount != 1 {
		t.Fatalf("want exactly 1 published record, got %d", publishedCount)
	}
}

func TestVerification_ProtectsFrozenChainFromRetention(t *testing.T) {
	ctx := context.Background()
	svc, _ := newTestService(t)
	task := createSucceededRestore(t, ctx, svc, "ds", "env-a", 0)
	v, err := svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-1", TaskID: task.ID,
		TargetTime:    time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC),
		OutputVersion: "v1", Files: sampleManifest(),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 校验进行中：即使保留策略为 0，冻结的源链也不得被回收。
	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, d := range run.Decisions {
		if d.Action != RetentionRetained {
			t.Fatalf("frozen chain snapshot %s must be retained during verification, reasons=%v", d.SnapshotID, d.Reasons)
		}
	}
	for _, f := range v.RestoreChain {
		if _, err := svc.GetSnapshot(ctx, f.SnapshotID); err != nil {
			t.Fatalf("snapshot %s deleted while verification running: %v", f.SnapshotID, err)
		}
	}

	// 全部通过并发布后，校验不再 active；保留清理可以回收原链，发布结论不受影响。
	reportAll(t, ctx, svc, v, nil)
	if _, err := svc.PublishRestoreVerification(ctx, PublishRestoreVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	run2, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatalf("retention after publish: %v", err)
	}
	deleted := 0
	for _, d := range run2.Decisions {
		if d.Action == RetentionDeleted {
			deleted++
		}
	}
	if deleted != len(v.RestoreChain) {
		t.Fatalf("after publish all chain snapshots should be reclaimable, deleted=%d chain=%d",
			deleted, len(v.RestoreChain))
	}
}

func TestVerification_PersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc := NewService(store)
	ctx := context.Background()

	ids := registerChain(t, ctx, svc, "ds", 1)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[len(ids)-1], TargetEnvironment: "env-a", Holder: "w",
	})
	if err != nil {
		t.Fatalf("create restore: %v", err)
	}
	for {
		d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil {
			t.Fatalf("dispatch: %v", err)
		}
		if d.Step == nil {
			break
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	if _, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err != nil {
		t.Fatalf("complete: %v", err)
	}
	v, err := svc.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-1", TaskID: task.ID,
		TargetTime:    time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC),
		OutputVersion: "v1", Files: sampleManifest(),
	})
	if err != nil {
		t.Fatalf("create verification: %v", err)
	}
	if _, err := svc.ReportVerificationBlocks(ctx, ReportVerificationBlocksInput{
		VerificationID: v.ID,
		Results: []BlockVerificationResult{
			{FileIndex: 0, BlockIndex: 0, Success: true, ObservedDigest: "a-b0"},
			{FileIndex: 0, BlockIndex: 1, Success: false, Detail: "boom"},
		},
	}); err != nil {
		t.Fatalf("report: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重启：matched/failed 块、冻结清单与幂等索引全部恢复，仍保持不可用。
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	svc2 := NewService(reopened)
	got, err := svc2.GetRestoreVerification(ctx, v.ID)
	if err != nil {
		t.Fatalf("load verification after restart: %v", err)
	}
	if got.Blocks.Matched != 1 || got.Blocks.Failed != 1 || got.Blocks.Total != 3 {
		t.Fatalf("block state did not survive restart: %+v", got.Blocks)
	}
	if got.Available {
		t.Fatalf("unpublished verification must stay unavailable across restart")
	}
	// 相同请求号重放仍返回原记录。
	again, err := svc2.CreateRestoreVerification(ctx, CreateRestoreVerificationInput{
		IdempotencyKey: "verify-1", TaskID: task.ID,
		TargetTime:    time.Date(2026, 9, 26, 8, 0, 0, 0, time.UTC),
		OutputVersion: "v1", Files: sampleManifest(),
	})
	if err != nil {
		t.Fatalf("idempotent replay after restart: %v", err)
	}
	if again.ID != v.ID {
		t.Fatalf("replay after restart returned new id %s, want %s", again.ID, v.ID)
	}
}
