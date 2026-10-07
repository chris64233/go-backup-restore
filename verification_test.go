package backuprestore

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// finishRestore 创建恢复任务并把全部步骤执行成功、完成任务，返回任务。
func finishRestore(t *testing.T, ctx context.Context, svc *Service, targetID, env, holder string) *RestoreTask {
	t.Helper()
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: targetID, TargetEnvironment: env, Holder: holder,
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
			t.Fatalf("ack step %d: %v", d.Step.Index, err)
		}
	}
	if _, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch); err != nil {
		t.Fatalf("complete restore: %v", err)
	}
	task, err = svc.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	return task
}

func sampleManifest() []ManifestFile {
	return []ManifestFile{
		{Name: "data/a.db", Size: 20, Digest: "file-a", BlockDigests: []string{"a0", "a1"}},
		{Name: "data/b.db", Size: 10, Digest: "file-b", BlockDigests: []string{"b0"}},
	}
}

func allGoodResults(files []ManifestFile) []BlockResult {
	var rs []BlockResult
	for _, f := range files {
		for i, d := range f.BlockDigests {
			rs = append(rs, BlockResult{File: f.Name, Index: i, ObservedDigest: d, Success: true})
		}
	}
	return rs
}

func publishVerified(t *testing.T, ctx context.Context, svc *Service, v *RestoreVerification) *RestoreVerification {
	t.Helper()
	if _, err := svc.VerifyBlocks(ctx, VerifyBlocksInput{
		VerificationID: v.ID, Results: allGoodResults(v.Manifest),
	}); err != nil {
		t.Fatalf("verify blocks: %v", err)
	}
	out, err := svc.PublishVerification(ctx, PublishVerificationInput{
		VerificationID: v.ID, OutputVersion: v.OutputVersion,
	})
	if err != nil {
		t.Fatalf("publish verification: %v", err)
	}
	return out
}

var verifyTargetTime = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func createVerificationOrFail(t *testing.T, ctx context.Context, svc *Service,
	task *RestoreTask, key, outputVersion string, manifest []ManifestFile, target time.Time) *RestoreVerification {
	t.Helper()
	v, err := svc.CreateVerification(ctx, CreateVerificationInput{
		RequestKey: key, RestoreTaskID: task.ID, TargetTime: target,
		OutputVersion: outputVersion, OutputDir: "/restore/" + task.TargetEnvironment, Manifest: manifest,
	})
	if err != nil {
		t.Fatalf("create verification: %v", err)
	}
	return v
}

// 范围冻结：创建时锁定恢复计划、目标时间、输出版本和清单；同键重复返回原结论。
func TestVerification_CreateFreezesScopeAndIdempotent(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	manifest := sampleManifest()

	v1 := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", manifest, verifyTargetTime)
	if v1.Status != VerificationVerifying || !v1.Active() {
		t.Fatalf("new verification must be verifying/active, got %s", v1.Status)
	}
	if len(v1.Plan) != len(task.Chain) {
		t.Fatalf("frozen plan length = %d, want %d", len(v1.Plan), len(task.Chain))
	}
	for i, p := range v1.Plan {
		if p.SnapshotID != task.Chain[i].SnapshotID || p.Digest != task.Chain[i].Digest {
			t.Fatalf("plan %d not frozen from restore task", i)
		}
	}
	if len(v1.Blocks) != 3 {
		t.Fatalf("blocks = %d, want 3", len(v1.Blocks))
	}

	// 同一请求重复提交返回同一条原记录。
	v1b := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", manifest, verifyTargetTime)
	if v1b.ID != v1.ID {
		t.Fatalf("duplicate request must return same verification: %s vs %s", v1b.ID, v1.ID)
	}

	// 未完成的恢复任务不能校验。
	task2 := func() *RestoreTask {
		tr, err := svc.CreateRestore(ctx, CreateRestoreInput{
			TargetSnapshotID: ids[1], TargetEnvironment: "env-b", Holder: "w2"})
		if err != nil {
			t.Fatalf("create restore: %v", err)
		}
		return tr
	}()
	_, err := svc.CreateVerification(ctx, CreateVerificationInput{
		RequestKey: "req-x", RestoreTaskID: task2.ID, TargetTime: verifyTargetTime,
		OutputVersion: "v1.0", Manifest: manifest,
	})
	mustCode(t, err, ErrCodeConflict)
}

// 同键但输出版本/目标时间/清单变化 -> conflict，绝不静默复用。
func TestVerification_ConflictOnChangedScope(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	manifest := sampleManifest()
	v := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", manifest, verifyTargetTime)

	cases := []struct {
		name     string
		key      string
		version  string
		time     time.Time
		manifest []ManifestFile
	}{
		{"output version", "req-1", "v2.0", verifyTargetTime, manifest},
		{"target time", "req-1", "v1.0", verifyTargetTime.Add(time.Minute), manifest},
		{"manifest digest", "req-1", "v1.0", verifyTargetTime, []ManifestFile{
			{Name: "data/a.db", Size: 21, Digest: "file-a2", BlockDigests: []string{"a0", "a2"}},
			{Name: "data/b.db", Size: 10, Digest: "file-b", BlockDigests: []string{"b0"}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateVerification(ctx, CreateVerificationInput{
				RequestKey: tc.key, RestoreTaskID: task.ID, TargetTime: tc.time,
				OutputVersion: tc.version, Manifest: tc.manifest,
			})
			mustCode(t, err, ErrCodeConflict)
		})
	}

	// 同一恢复任务存在未发布校验时，另一个不同请求也必须冲突。
	_, err := svc.CreateVerification(ctx, CreateVerificationInput{
		RequestKey: "req-2", RestoreTaskID: task.ID, TargetTime: verifyTargetTime,
		OutputVersion: "v1.0", Manifest: manifest,
	})
	mustCode(t, err, ErrCodeConflict)

	// 原记录未受影响。
	view, err := svc.GetVerification(ctx, v.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if view.Verification.OutputVersion != "v1.0" {
		t.Fatalf("original verification scope mutated")
	}
}

// 分批校验：部分失败保持不可用；失败块可重试；全部通过才能发布。
func TestVerification_PartialFailureRetryAndPublish(t *testing.T) {
	svc, clock := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	v := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", sampleManifest(), verifyTargetTime)

	// 第一批：a 文件两块通过，b0 摘要不一致 -> 部分成功，目录仍不可用。
	clock.Advance(time.Second)
	v, err := svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v.ID, Results: []BlockResult{
		{File: "data/a.db", Index: 0, ObservedDigest: "a0", Success: true},
		{File: "data/a.db", Index: 1, ObservedDigest: "a1", Success: true},
		{File: "data/b.db", Index: 0, ObservedDigest: "WRONG", Success: true, Detail: "checksum mismatch"},
	}})
	if err != nil {
		t.Fatalf("verify batch: %v", err)
	}
	view, _ := svc.GetVerification(ctx, v.ID)
	if view.Passed != 2 || view.Failed != 1 || view.Available {
		t.Fatalf("partial result: passed=%d failed=%d available=%v", view.Passed, view.Failed, view.Available)
	}
	if len(view.Diffs) != 1 || view.Diffs[0].File != "data/b.db" || view.Diffs[0].ExpectedDigest != "b0" ||
		view.Diffs[0].ObservedDigest != "WRONG" {
		t.Fatalf("diff should pinpoint file and block: %+v", view.Diffs)
	}

	// 存在 failed 块时发布被拒，保持 verifying。
	if _, err := svc.PublishVerification(ctx, PublishVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1.0",
	}); CodeOf(err) != ErrCodeConflict {
		t.Fatalf("publish with failed block must conflict, got %v", err)
	}

	// 已通过的块不可被后来的结果回退。
	v, err = svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v.ID, Results: []BlockResult{
		{File: "data/a.db", Index: 0, ObservedDigest: "TAMPERED", Success: false},
	}})
	if err != nil {
		t.Fatalf("rewrite passed block: %v", err)
	}
	var a0 *VerificationBlock
	for i := range v.Blocks {
		if v.Blocks[i].File == "data/a.db" && v.Blocks[i].Index == 0 {
			a0 = &v.Blocks[i]
		}
	}
	if a0.Status != BlockPassed || a0.ObservedDigest != "a0" {
		t.Fatalf("passed block regressed: %+v", a0)
	}

	// 重试失败块：这次摘要一致 -> 通过。
	clock.Advance(time.Second)
	v, err = svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v.ID, Results: []BlockResult{
		{File: "data/b.db", Index: 0, ObservedDigest: "b0", Success: true},
	}})
	if err != nil {
		t.Fatalf("retry block: %v", err)
	}
	view, _ = svc.GetVerification(ctx, v.ID)
	if view.Passed != 3 || view.Failed != 0 || len(view.Diffs) != 0 {
		t.Fatalf("after retry: %+v", view)
	}

	// 输出版本漂移时发布被拒。
	if _, err := svc.PublishVerification(ctx, PublishVerificationInput{
		VerificationID: v.ID, OutputVersion: "v9.9",
	}); CodeOf(err) != ErrCodeConflict {
		t.Fatalf("publish with drifted output version must conflict, got %v", err)
	}

	// 发布幂等：重复发布返回同一结论。
	pub := publishVerified(t, ctx, svc, v)
	if pub.Status != VerificationPublished || pub.PublishedAt == nil {
		t.Fatalf("published record wrong: %+v", pub)
	}
	pub2, err := svc.PublishVerification(ctx, PublishVerificationInput{
		VerificationID: v.ID, OutputVersion: "v1.0",
	})
	if err != nil || pub2.ID != pub.ID {
		t.Fatalf("idempotent publish failed: %v %s", err, pub2.ID)
	}
	// 已发布后拒绝再写块结果。
	_, err = svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v.ID, Results: []BlockResult{
		{File: "data/b.db", Index: 0, ObservedDigest: "late", Success: false},
	}})
	mustCode(t, err, ErrCodeConflict)
}

// 发布后重跑不会静默替换：新范围作为新校验记录保存，已发布结果保持可用。
func TestVerification_RerunCreatesNewRecordNotReplace(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")

	v1 := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", sampleManifest(), verifyTargetTime)
	publishVerified(t, ctx, svc, v1)

	status, err := svc.GetRestoreOutputStatus(ctx, task.ID)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if !status.Available || status.Published == nil || status.Published.Verification.ID != v1.ID {
		t.Fatalf("published output must be available: %+v", status)
	}
	if status.DatasetID != "ds" || status.TargetEnvironment != "env-a" {
		t.Fatalf("status must show restore source: %+v", status)
	}

	// 重跑产生不同清单（一个块摘要变化）：必须是新记录，旧发布不被覆盖。
	newManifest := []ManifestFile{
		{Name: "data/a.db", Size: 20, Digest: "file-a", BlockDigests: []string{"a0", "CHANGED"}},
		{Name: "data/b.db", Size: 10, Digest: "file-b", BlockDigests: []string{"b0"}},
	}
	v2 := createVerificationOrFail(t, ctx, svc, task, "req-2", "v1.1", newManifest, verifyTargetTime)
	if v2.ID == v1.ID {
		t.Fatal("rerun must create a new verification record")
	}
	records, _ := svc.ListVerifications(ctx, task.ID)
	if len(records) != 2 {
		t.Fatalf("expected 2 verification records, got %d", len(records))
	}
	// 新记录尚未发布：查询明确指出差异块；已发布结果仍是当前可用结论。
	_, err = svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v2.ID, Results: []BlockResult{
		{File: "data/a.db", Index: 1, ObservedDigest: "ACTUALLY-a1", Success: true, Detail: "diff"},
	}})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	view2, _ := svc.GetVerification(ctx, v2.ID)
	if view2.Available || len(view2.Diffs) != 1 || view2.Diffs[0].File != "data/a.db" {
		t.Fatalf("new record must stay unavailable and pinpoint diff: %+v", view2.Diffs)
	}
	status, _ = svc.GetRestoreOutputStatus(ctx, task.ID)
	if !status.Available || status.Published.Verification.ID != v1.ID {
		t.Fatalf("published result must not be silently replaced: %+v", status)
	}
}

// 恢复任务重启后复用已通过块，但失败块与输出版本必须重新确认。
func TestVerification_RestartReusesPassedBlocksOnly(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	manifest := sampleManifest()

	// 第一次校验：a0/a1 通过，b0 失败后放弃（保持未发布）。通过重新执行恢复
	// 模拟重启：这里直接用同一已完成任务创建新校验（重跑指向相同恢复计划）。
	v1 := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", manifest, verifyTargetTime)
	if _, err := svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v1.ID, Results: []BlockResult{
		{File: "data/a.db", Index: 0, ObservedDigest: "a0", Success: true},
		{File: "data/a.db", Index: 1, ObservedDigest: "a1", Success: true},
		{File: "data/b.db", Index: 0, ObservedDigest: "bad", Success: false, Detail: "io error"},
	}}); err != nil {
		t.Fatalf("verify: %v", err)
	}

	// 新输出版本必须显式重新确认：版本不同即冲突，不能复用 v1 的在途校验。
	_, err := svc.CreateVerification(ctx, CreateVerificationInput{
		RequestKey: "req-1b", RestoreTaskID: task.ID, TargetTime: verifyTargetTime,
		OutputVersion: "v1.0", Manifest: manifest,
	})
	mustCode(t, err, ErrCodeConflict)

	// v1 无法完成（b0 持续失败的场景下放弃），重启以新请求、新版本创建。
	v2, err := svc.CreateVerification(ctx, CreateVerificationInput{
		RequestKey: "req-2", RestoreTaskID: task.ID, TargetTime: verifyTargetTime,
		OutputVersion: "v1.1", Manifest: manifest, Restart: true,
	})
	if err != nil {
		t.Fatalf("restart verification: %v", err)
	}
	// 旧记录已被取代：不能发布。
	if _, err := svc.PublishVerification(ctx, PublishVerificationInput{
		VerificationID: v1.ID, OutputVersion: "v1.0",
	}); CodeOf(err) != ErrCodeConflict {
		t.Fatalf("superseded verification must not publish, got %v", err)
	}
	statuses := map[string]VerificationBlockStatus{}
	for _, b := range v2.Blocks {
		statuses[b.File+"#"+itoa(b.Index)] = b.Status
	}
	if statuses["data/a.db#0"] != BlockPassed || statuses["data/a.db#1"] != BlockPassed {
		t.Fatalf("passed blocks must be reused: %+v", statuses)
	}
	if statuses["data/b.db#0"] != BlockPending {
		t.Fatalf("previously failed block must be re-confirmed (pending), got %s", statuses["data/b.db#0"])
	}

	// 清单变化：期望摘要变化的块不得复用旧结论。
	changedManifest := []ManifestFile{
		{Name: "data/a.db", Size: 20, Digest: "file-a", BlockDigests: []string{"a0", "a1-new"}},
		{Name: "data/b.db", Size: 10, Digest: "file-b", BlockDigests: []string{"b0"}},
	}
	v3, err := svc.CreateVerification(ctx, CreateVerificationInput{
		RequestKey: "req-3", RestoreTaskID: task.ID, TargetTime: verifyTargetTime,
		OutputVersion: "v1.2", Manifest: changedManifest, Restart: true,
	})
	if err != nil {
		t.Fatalf("restart with changed manifest: %v", err)
	}
	for _, b := range v3.Blocks {
		if b.File == "data/a.db" && b.Index == 1 {
			if b.Status != BlockPending {
				t.Fatalf("block with changed expected digest must not be reused: %s", b.Status)
			}
		}
		if b.File == "data/a.db" && b.Index == 0 && b.Status != BlockPassed {
			t.Fatalf("unchanged passed block should be reused: %s", b.Status)
		}
	}
}

func itoa(i int) string { return fmt.Sprintf("%d", i) }

// 发布竞态：同一未发布校验在并发发布下最多一次生效，结论不被撕裂；
// 并发创建第二个未发布校验也必须只有一个赢家。
func TestVerification_ConcurrentPublishAndCreate(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	v := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", sampleManifest(), verifyTargetTime)
	if _, err := svc.VerifyBlocks(ctx, VerifyBlocksInput{
		VerificationID: v.ID, Results: allGoodResults(v.Manifest),
	}); err != nil {
		t.Fatalf("verify: %v", err)
	}

	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = svc.PublishVerification(ctx, PublishVerificationInput{
				VerificationID: v.ID, OutputVersion: "v1.0",
			})
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			t.Fatalf("idempotent concurrent publish must all succeed, got %v", e)
		}
	}
	records, _ := svc.ListVerifications(ctx, task.ID)
	published := 0
	for _, r := range records {
		if r.Available {
			published++
		}
	}
	if published != 1 || len(records) != 1 {
		t.Fatalf("exactly one published record, got published=%d total=%d", published, len(records))
	}

	// 并发创建新校验：只有一个能拿到“未发布”名额。
	const m = 8
	createErrs := make([]error, m)
	wg.Add(m)
	for i := 0; i < m; i++ {
		go func(i int) {
			defer wg.Done()
			_, createErrs[i] = svc.CreateVerification(ctx, CreateVerificationInput{
				RequestKey: fmt.Sprintf("race-%d", i), RestoreTaskID: task.ID,
				TargetTime: verifyTargetTime, OutputVersion: "v2.0", Manifest: sampleManifest(),
			})
		}(i)
	}
	wg.Wait()
	conflicts, created := 0, 0
	for _, e := range createErrs {
		switch CodeOf(e) {
		case "":
			created++
		case ErrCodeConflict:
			conflicts++
		default:
			t.Fatalf("unexpected error %v", e)
		}
	}
	if created != 1 || conflicts != m-1 {
		t.Fatalf("exactly one concurrent create must win: created=%d conflicts=%d", created, conflicts)
	}
}

// 未发布校验冻结的源链快照受保留清理保护；发布后（恢复任务已 succeeded 不活跃）
// 若快照已被压缩替代则可被正常回收，校验记录本身仍保留结论。
func TestVerification_ProtectsChainWhileUnpublished(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	v := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", sampleManifest(), verifyTargetTime)

	run, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 0}})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	for _, d := range run.Decisions {
		if d.Action == RetentionDeleted {
			t.Fatalf("snapshot %s deleted while verification %s is unpublished", d.SnapshotID, v.ID)
		}
		found := false
		for _, r := range d.Reasons {
			if r == ReasonActiveVerification {
				found = true
			}
		}
		if !found {
			t.Fatalf("retention decision %s missing active_verification reason: %v", d.SnapshotID, d.Reasons)
		}
	}
}

// FileStore 持久化：校验记录（范围、块结论、发布状态）重启后完整恢复。
func TestVerification_FileStoreRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	ctx := context.Background()

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc := NewService(store)
	ids := registerChain(t, ctx, svc, "ds", 1)
	task := finishRestore(t, ctx, svc, ids[1], "env-a", "w1")
	v := createVerificationOrFail(t, ctx, svc, task, "req-1", "v1.0", sampleManifest(), verifyTargetTime)
	if _, err := svc.VerifyBlocks(ctx, VerifyBlocksInput{VerificationID: v.ID, Results: []BlockResult{
		{File: "data/a.db", Index: 0, ObservedDigest: "a0", Success: true},
	}}); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	svc2 := NewService(reopened)
	view, err := svc2.GetVerification(ctx, v.ID)
	if err != nil {
		t.Fatalf("get after restart: %v", err)
	}
	if view.Passed != 1 || view.Failed != 0 || view.Pending != 2 || view.Available {
		t.Fatalf("block state not restored: %+v", view)
	}
	if view.Verification.OutputVersion != "v1.0" || view.Verification.ManifestDigest == "" ||
		len(view.Verification.Plan) != len(task.Chain) {
		t.Fatalf("frozen scope not restored: %+v", view.Verification)
	}
	// 同键重放返回原结论。
	again := createVerificationOrFail(t, ctx, svc2, task, "req-1", "v1.0", sampleManifest(), verifyTargetTime)
	if again.ID != v.ID {
		t.Fatalf("idempotency not durable: %s vs %s", again.ID, v.ID)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened: %v", err)
	}
}
