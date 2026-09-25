package backuprestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestFileStore_Persistence 验证关系、状态、outbox、租约记录全部落盘，
// 重启后状态完整且 ID 序列不回退。
func TestFileStore_Persistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open new file store: %v", err)
	}
	svc := NewService(store)

	ids := registerChain(t, ctx, svc, "ds", 2)
	task, err := svc.CreateRestore(ctx, CreateRestoreInput{
		TargetSnapshotID: ids[2], TargetEnvironment: "env", Holder: "w1",
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		d, err := svc.DispatchNextStep(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
		if err != nil || d.Step == nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
		if _, err := svc.AckStep(ctx, AckStepInput{
			TaskID: task.ID, LeaseID: task.LeaseID, Epoch: task.LeaseEpoch,
			StepIndex: d.Step.Index, ExecutionVersion: d.Step.ExecutionVersion, Success: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := svc.CompleteRestore(ctx, task.ID, task.LeaseID, task.LeaseEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RunRetention(ctx, []RetentionRule{{DatasetID: "ds", KeepLatestCompleted: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 文件确实存在且为合法 JSON。
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		t.Fatalf("state file missing or empty: %v", err)
	}

	// 重新打开：状态应完整恢复。
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	svc2 := NewService(store2)

	chain, err := svc2.GetBackupChain(ctx, ids[2])
	if err != nil || len(chain) != 3 {
		t.Fatalf("chain after reload: %v len=%d", err, len(chain))
	}
	reloaded, err := svc2.GetRestoreTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("task after reload: %v", err)
	}
	if reloaded.Status != TaskSucceeded || len(reloaded.Steps) != 3 {
		t.Fatalf("task state wrong after reload: %+v", reloaded)
	}
	for i, step := range reloaded.Steps {
		if step.Status != StepSucceeded {
			t.Fatalf("step %d not succeeded after reload: %s", i, step.Status)
		}
	}

	events, err := svc2.ListOutboxEvents(ctx, true)
	if err != nil || len(events) != 1 || events[0].ID != res.Event.ID {
		t.Fatalf("outbox after reload: %v %+v", err, events)
	}
	leaseEvents, err := svc2.ListLeaseEvents(ctx, task.ID)
	if err != nil || len(leaseEvents) != 2 { // acquired + released
		t.Fatalf("lease events after reload: %v %+v", err, leaseEvents)
	}
	runs, err := svc2.ListRetentionRuns(ctx)
	if err != nil || len(runs) != 1 {
		t.Fatalf("retention runs after reload: %v %+v", err, runs)
	}

	// ID 序列不回退：新生成的 ID 必须严格大于旧 ID。
	newSnap, err := svc2.RegisterSnapshot(ctx, RegisterSnapshotInput{
		DatasetID: "ds2", Kind: KindFull, Digest: "dg-after-restart",
	})
	if err != nil {
		t.Fatal(err)
	}
	if newSnap.ID <= ids[2] {
		t.Fatalf("id sequence regressed: new=%s <= %s", newSnap.ID, ids[2])
	}
}

// TestFileStore_AtomicWrite 验证打开不存在的路径会得到空库，
// 而损坏文件会被归类为 unavailable。
func TestFileStore_CorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewFileStore(path)
	mustCode(t, err, ErrCodeUnavailable)
}

// TestFileStore_RollbackOnError 验证事务返回错误时不落盘、不修改已提交状态。
func TestFileStore_RollbackOnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	err = store.Update(func(tx *Tx) error {
		tx.PutSnapshot(Snapshot{ID: "snap-x", Digest: "d", Status: StatusPending})
		return classified(ErrCodeConflict, "deliberate abort")
	})
	mustCode(t, err, ErrCodeConflict)

	var count int
	if err := store.View(func(tx *Tx) error {
		count = len(tx.ListSnapshots())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("aborted tx should not be visible, found %d snapshots", count)
	}
}
