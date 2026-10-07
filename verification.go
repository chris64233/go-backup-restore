package backuprestore

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// 恢复校验：恢复产物在发布（目录可用）前必须通过全部数据块摘要校验
// ---------------------------------------------------------------------------

// RestoreVerificationFileInput 描述恢复产物清单中的一个文件及其数据块期望摘要。
// BlockDigests 每个元素对应一个数据块，长度即该文件的数据块数量。
type RestoreVerificationFileInput struct {
	Path         string
	Size         int64
	Digest       string
	BlockDigests []string
}

// CreateRestoreVerificationInput 创建校验记录的入参。
type CreateRestoreVerificationInput struct {
	IdempotencyKey string // 校验请求号：相同请求重复提交返回原结论
	TaskID         string // 恢复计划：恢复任务（其冻结的备份链即为恢复来源）
	TargetTime     time.Time
	OutputVersion  string // 输出版本，与恢复计划/清单一起冻结
	Files          []RestoreVerificationFileInput
}

// inputSignature 归一化请求内容，用于幂等冲突判定：同一请求号但恢复计划、
// 目标时间、输出版本或文件清单任何一处不同，都必须报 conflict。
func inputSignature(in CreateRestoreVerificationInput) string {
	key := fmt.Sprintf("task=%s\ntarget=%s\nversion=%s\n",
		in.TaskID, in.TargetTime.UTC().Format(time.RFC3339Nano), in.OutputVersion)
	for _, f := range in.Files {
		key += fmt.Sprintf("%s:%d:%s:%d\n", f.Path, f.Size, f.Digest, len(f.BlockDigests))
		for _, d := range f.BlockDigests {
			key += "  " + d + "\n"
		}
	}
	return key
}

// signatureOf 根据已冻结记录重建其请求签名，与新请求做一致性比较。
func signatureOf(v *RestoreVerification) string {
	key := fmt.Sprintf("task=%s\ntarget=%s\nversion=%s\n",
		v.TaskID, v.TargetTime.UTC().Format(time.RFC3339Nano), v.OutputVersion)
	for _, f := range v.Manifest {
		key += fmt.Sprintf("%s:%d:%s:%d\n", f.Path, f.Size, f.Digest, f.Blocks)
		for bi := 0; bi < f.Blocks; bi++ {
			key += "  " + v.Blocks[fileBlockOffset(v.Manifest, f.Index)+bi].ExpectedDigest + "\n"
		}
	}
	return key
}

// CreateRestoreVerification 创建一条恢复校验记录，在单个事务内：
//  1. 幂等判定：相同请求号且恢复计划/目标时间/输出版本/清单一致 -> 返回原记录；
//     请求号相同但任何一项不同 -> conflict，绝不悄悄改变正在校验的范围；
//  2. 冻结恢复计划（复制恢复任务的冻结备份链）、目标时间、输出版本与文件清单，
//     此后源备份链如何变化都不影响这条记录的校验范围；
//  3. 按清单初始化全部数据块为 pending。
func (s *Service) CreateRestoreVerification(ctx context.Context, in CreateRestoreVerificationInput) (*RestoreVerification, error) {
	if in.IdempotencyKey == "" || in.TaskID == "" || in.OutputVersion == "" {
		return nil, classified(ErrCodeInvalidArgument,
			"idempotency key, task id and output version are required")
	}
	if in.TargetTime.IsZero() {
		return nil, classified(ErrCodeInvalidArgument, "target time is required")
	}
	if len(in.Files) == 0 {
		return nil, classified(ErrCodeInvalidArgument, "at least one manifest file is required")
	}
	seenPath := map[string]bool{}
	totalBlocks := 0
	for fi, f := range in.Files {
		if f.Path == "" {
			return nil, classified(ErrCodeInvalidArgument, "file %d: path is required", fi)
		}
		if seenPath[f.Path] {
			return nil, classified(ErrCodeInvalidArgument, "duplicate manifest file path %q", f.Path)
		}
		seenPath[f.Path] = true
		if f.Digest == "" {
			return nil, classified(ErrCodeInvalidArgument, "file %q: digest is required", f.Path)
		}
		if len(f.BlockDigests) == 0 {
			return nil, classified(ErrCodeInvalidArgument, "file %q: at least one block digest is required", f.Path)
		}
		for bi, d := range f.BlockDigests {
			if d == "" {
				return nil, classified(ErrCodeInvalidArgument, "file %q block %d: expected digest is empty", f.Path, bi)
			}
		}
		totalBlocks += len(f.BlockDigests)
	}

	var out *RestoreVerification
	err := s.store.Update(func(tx *Tx) error {
		if existing, ok := tx.FindVerificationByKey(in.IdempotencyKey); ok {
			if signatureOf(existing) != inputSignature(in) {
				return classified(ErrCodeConflict,
					"verification request %s already submitted as %s with a different restore plan, target time, output version or manifest",
					in.IdempotencyKey, existing.ID)
			}
			out = cloneVerification(existing)
			return nil
		}

		task, ok := tx.GetTask(in.TaskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", in.TaskID)
		}
		if task.Status != TaskSucceeded {
			return classified(ErrCodeConflict,
				"restore task %s is %s, only a completed restore can be verified", task.ID, task.Status)
		}

		now := s.timeNow()
		frozen := make([]FrozenSnapshot, len(task.Chain))
		copy(frozen, task.Chain)
		files := make([]RestoreManifestFile, len(in.Files))
		blocks := make([]RestoreBlock, 0, totalBlocks)
		for fi, f := range in.Files {
			files[fi] = RestoreManifestFile{
				Index: fi, Path: f.Path, Size: f.Size, Digest: f.Digest,
				Blocks: len(f.BlockDigests),
			}
			for bi, expected := range f.BlockDigests {
				blocks = append(blocks, RestoreBlock{
					FileIndex: fi, BlockIndex: bi,
					ExpectedDigest: expected, Status: BlockPending,
				})
			}
		}
		v := RestoreVerification{
			ID:                tx.NewID("verify"),
			IdempotencyKey:    in.IdempotencyKey,
			TaskID:            task.ID,
			DatasetID:         task.DatasetID,
			TargetEnvironment: task.TargetEnvironment,
			RestoreChain:      frozen,
			TargetTime:        in.TargetTime.UTC(),
			OutputVersion:     in.OutputVersion,
			Manifest:          files,
			Blocks:            blocks,
			Status:            VerificationRunning,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		tx.PutVerification(v)
		out = cloneVerification(&v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PublishRestoreVerificationInput 发布校验结论的入参。
// OutputVersion 必须再次与冻结版本一致（发布前的最后确认）。
type PublishRestoreVerificationInput struct {
	VerificationID string
	OutputVersion  string
}

// PublishRestoreVerificationResult 返回发布后的校验记录与恢复任务。
type PublishRestoreVerificationResult struct {
	Verification *RestoreVerification
	Task         *RestoreTask
}

// PublishRestoreVerification 在单个事务内原子发布：
//  1. 校验记录仍为 running，且发布前重新确认输出版本与冻结版本一致；
//  2. 校验冻结的源备份链仍存在且摘要未变（源链变化给出明确冲突，不静默改范围）；
//  3. 只有全部数据块 matched 才允许发布；存在 pending/failed 块时保持不可用，
//     错误中指出具体文件与数据块；
//  4. 每个恢复任务至多一条已发布校验记录（存储层唯一约束兜底）：
//     后来的重跑即使全部通过也只能保存为新的校验记录，不能静默替换已发布结果。
//
// 发布成功后恢复任务才标记 Available=true；对已发布记录重复发布幂等返回原结论。
func (s *Service) PublishRestoreVerification(ctx context.Context, in PublishRestoreVerificationInput) (*PublishRestoreVerificationResult, error) {
	if in.VerificationID == "" || in.OutputVersion == "" {
		return nil, classified(ErrCodeInvalidArgument, "verification id and output version are required")
	}
	res := &PublishRestoreVerificationResult{}
	err := s.store.Update(func(tx *Tx) error {
		v, ok := tx.GetVerification(in.VerificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", in.VerificationID)
		}
		if v.Status == VerificationPublished {
			// 幂等：已发布记录返回原结论，绝不产生第二份发布。
			task, _ := tx.GetTask(v.TaskID)
			res.Verification = cloneVerification(v)
			if task != nil {
				res.Task = cloneTask(task)
			}
			return nil
		}
		if in.OutputVersion != v.OutputVersion {
			return classified(ErrCodeConflict,
				"output version changed for verification %s: frozen %s, publish declares %s",
				v.ID, v.OutputVersion, in.OutputVersion)
		}
		if bad := firstUnmatchedBlock(v); bad != nil {
			mf := v.Manifest[bad.FileIndex]
			return classified(ErrCodeConflict,
				"cannot publish verification %s: block file=%q index=%d is %s%s",
				v.ID, mf.Path, bad.BlockIndex, bad.Status, mismatchTail(bad, mf.Path))
		}
		// 重新确认冻结的恢复来源没有被悄悄改动。
		for _, f := range v.RestoreChain {
			snap, ok := tx.GetSnapshot(f.SnapshotID)
			if !ok {
				return classified(ErrCodeConflict, "source snapshot %s no longer exists", f.SnapshotID)
			}
			if snap.Digest != f.Digest {
				return classified(ErrCodeConflict,
					"source snapshot %s digest changed since verification freeze", f.SnapshotID)
			}
		}
		task, ok := tx.GetTask(v.TaskID)
		if !ok {
			return classified(ErrCodeConflict, "restore task %s no longer exists", v.TaskID)
		}
		if publishedID, exists := tx.PublishedVerificationForTask(task.ID); exists {
			return classified(ErrCodeConflict,
				"restore task %s already has published verification %s; a new diff must be saved as a new verification record, not silently replaced",
				task.ID, publishedID)
		}

		now := s.timeNow()
		v.Status = VerificationPublished
		v.PublishedAt = &now
		v.UpdatedAt = now
		tx.PutVerification(*v)
		if err := tx.MarkVerificationPublished(task.ID, v.ID); err != nil {
			return err
		}
		task.Available = true
		task.PublishedVerificationID = v.ID
		tx.PutTask(*task)

		res.Verification = cloneVerification(v)
		res.Task = cloneTask(task)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// firstUnmatchedBlock 返回第一个未通过（pending/failed）的数据块。
func firstUnmatchedBlock(v *RestoreVerification) *RestoreBlock {
	for i := range v.Blocks {
		if v.Blocks[i].Status != BlockMatched {
			return &v.Blocks[i]
		}
	}
	return nil
}

func mismatchTail(b *RestoreBlock, path string) string {
	switch b.Status {
	case BlockPending:
		return " (not verified yet)"
	case BlockFailed:
		if b.ObservedDigest != "" {
			return fmt.Sprintf(" (expected %s, observed %s)", b.ExpectedDigest, b.ObservedDigest)
		}
		return " (check failed; " + path + " not verified)"
	}
	return ""
}

// RestoreSource 说明恢复目录的来源：恢复任务与其冻结的备份链。
type RestoreSource struct {
	TaskID            string
	DatasetID         string
	TargetEnvironment string
	TargetSnapshotID  string
	FrozenChain       []FrozenSnapshot
}

// BlockSummary 汇总数据块校验进度。
type BlockSummary struct {
	Total      int
	Pending    int
	Matched    int
	Failed     int
	Mismatches []BlockMismatch // 当前摘要不一致（failed）的具体文件/数据块
}

// RestoreVerificationView 是校验记录的查询视图：恢复来源、数据块摘要差异
// 与最终可用状态；摘要不一致时精确指出具体文件或数据块。
type RestoreVerificationView struct {
	ID             string
	IdempotencyKey string
	Status         VerificationStatus
	Available      bool
	TargetTime     time.Time
	OutputVersion  string
	Source         RestoreSource
	Manifest       []RestoreManifestFile
	Blocks         BlockSummary
	CreatedAt      time.Time
	PublishedAt    *time.Time
}

// buildVerificationView 从冻结记录与恢复任务组装只读视图。
func buildVerificationView(v *RestoreVerification, task *RestoreTask) *RestoreVerificationView {
	view := &RestoreVerificationView{
		ID: v.ID, IdempotencyKey: v.IdempotencyKey, Status: v.Status,
		Available:  v.Status == VerificationPublished,
		TargetTime: v.TargetTime, OutputVersion: v.OutputVersion,
		Manifest:  append([]RestoreManifestFile(nil), v.Manifest...),
		CreatedAt: v.CreatedAt, PublishedAt: v.PublishedAt,
	}
	view.Source = RestoreSource{
		TaskID: v.TaskID, DatasetID: v.DatasetID,
		TargetEnvironment: v.TargetEnvironment,
		FrozenChain:       append([]FrozenSnapshot(nil), v.RestoreChain...),
	}
	if task != nil {
		view.Source.TargetSnapshotID = task.TargetSnapshotID
	}
	for _, b := range v.Blocks {
		view.Blocks.Total++
		switch b.Status {
		case BlockPending:
			view.Blocks.Pending++
		case BlockMatched:
			view.Blocks.Matched++
		case BlockFailed:
			view.Blocks.Failed++
			path := ""
			if b.FileIndex < len(v.Manifest) {
				path = v.Manifest[b.FileIndex].Path
			}
			view.Blocks.Mismatches = append(view.Blocks.Mismatches, BlockMismatch{
				FileIndex: b.FileIndex, Path: path, BlockIndex: b.BlockIndex,
				ExpectedDigest: b.ExpectedDigest, ObservedDigest: b.ObservedDigest,
				Detail: b.LastDetail,
			})
		}
	}
	return view
}

// GetRestoreVerification 查询单条校验记录（含恢复来源、摘要差异与可用状态）。
func (s *Service) GetRestoreVerification(ctx context.Context, verificationID string) (*RestoreVerificationView, error) {
	var out *RestoreVerificationView
	err := s.store.View(func(tx *Tx) error {
		v, ok := tx.GetVerification(verificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", verificationID)
		}
		task, _ := tx.GetTask(v.TaskID)
		out = buildVerificationView(v, task)
		return nil
	})
	return out, err
}

// ListRestoreVerifications 按创建顺序列出校验记录；taskID 为空时返回全部。
func (s *Service) ListRestoreVerifications(ctx context.Context, taskID string) ([]*RestoreVerificationView, error) {
	var out []*RestoreVerificationView
	err := s.store.View(func(tx *Tx) error {
		var list []*RestoreVerification
		if taskID == "" {
			list = tx.ListVerifications()
		} else {
			list = tx.ListVerificationsForTask(taskID)
		}
		for _, v := range list {
			task, _ := tx.GetTask(v.TaskID)
			out = append(out, buildVerificationView(v, task))
		}
		return nil
	})
	return out, err
}

// BlockVerificationResult 是一个数据块的校验回报。
// Success=false 表示该块读取/校验本身失败（ObservedDigest 可留空）；
// Success=true 时由服务端比对 ObservedDigest 与冻结的期望摘要决定是否通过。
type BlockVerificationResult struct {
	FileIndex      int
	BlockIndex     int
	Success        bool
	ObservedDigest string
	Detail         string
}

// ReportVerificationBlocksInput 分批回报数据块校验结果。
type ReportVerificationBlocksInput struct {
	VerificationID string
	Results        []BlockVerificationResult
}

// ReportVerificationBlocks 在单个事务内分批记录数据块校验结果：
//   - matched 块的结论不可回退：用不同摘要重报报 conflict，相同结果幂等；
//   - pending/failed 块允许重试，每次回报递增 Attempts；
//   - 一批内可部分成功部分失败：失败块保持 failed，记录继续保持 running，
//     只有后续重试让全部数据块通过才具备发布条件。
func (s *Service) ReportVerificationBlocks(ctx context.Context, in ReportVerificationBlocksInput) (*RestoreVerification, error) {
	if in.VerificationID == "" {
		return nil, classified(ErrCodeInvalidArgument, "verification id is required")
	}
	if len(in.Results) == 0 {
		return nil, classified(ErrCodeInvalidArgument, "at least one block result is required")
	}

	var out *RestoreVerification
	err := s.store.Update(func(tx *Tx) error {
		v, ok := tx.GetVerification(in.VerificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", in.VerificationID)
		}
		if v.Status != VerificationRunning {
			return classified(ErrCodeConflict, "verification %s is already %s", v.ID, v.Status)
		}
		for _, r := range in.Results {
			if r.FileIndex < 0 || r.FileIndex >= len(v.Manifest) {
				return classified(ErrCodeInvalidArgument,
					"file index %d out of range (manifest length %d)", r.FileIndex, len(v.Manifest))
			}
			mf := v.Manifest[r.FileIndex]
			if r.BlockIndex < 0 || r.BlockIndex >= mf.Blocks {
				return classified(ErrCodeInvalidArgument,
					"block index %d out of range for file %q (%d blocks)", r.BlockIndex, mf.Path, mf.Blocks)
			}
			offset := fileBlockOffset(v.Manifest, r.FileIndex)
			block := &v.Blocks[offset+r.BlockIndex]

			matched := r.Success && r.ObservedDigest == block.ExpectedDigest
			if block.Status == BlockMatched {
				// matched 不可回退：相同结论的迟到/重复回报幂等接受，其余拒绝。
				if matched {
					continue
				}
				return classified(ErrCodeConflict,
					"block file=%s index=%d already matched; a different result cannot replace it",
					mf.Path, r.BlockIndex)
			}
			block.Attempts++
			if matched {
				block.Status = BlockMatched
				block.ObservedDigest = r.ObservedDigest
				block.LastDetail = r.Detail
			} else {
				block.Status = BlockFailed
				block.ObservedDigest = r.ObservedDigest
				if r.Success {
					block.LastDetail = fmt.Sprintf("digest mismatch: expected %s, observed %s%s",
						block.ExpectedDigest, r.ObservedDigest, suffixDetail(r.Detail))
				} else {
					block.LastDetail = "block check failed" + suffixDetail(r.Detail)
				}
			}
		}
		v.UpdatedAt = s.timeNow()
		tx.PutVerification(*v)
		out = cloneVerification(v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func suffixDetail(detail string) string {
	if detail == "" {
		return ""
	}
	return ": " + detail
}

// RestartVerificationInput 恢复任务重启后复用校验上下文。
// OutputVersion 必须与创建时冻结的输出版本一致（重新确认），不一致报 conflict。
type RestartVerificationInput struct {
	VerificationID string
	OutputVersion  string
}

// RestartVerification 在恢复任务重启后复用已经通过（matched）的数据块，
// 但把失败块与未完成的 pending 块重新确认：失败块重置为 pending 等待重跑，
// matched 块保持不变。调用方必须重新确认输出版本，版本不一致直接 conflict，
// 从而避免重启后生成多份互相矛盾的目录。
func (s *Service) RestartVerification(ctx context.Context, in RestartVerificationInput) (*RestoreVerification, error) {
	if in.VerificationID == "" || in.OutputVersion == "" {
		return nil, classified(ErrCodeInvalidArgument, "verification id and output version are required")
	}
	var out *RestoreVerification
	err := s.store.Update(func(tx *Tx) error {
		v, ok := tx.GetVerification(in.VerificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", in.VerificationID)
		}
		if v.Status != VerificationRunning {
			return classified(ErrCodeConflict, "verification %s is already %s", v.ID, v.Status)
		}
		if in.OutputVersion != v.OutputVersion {
			return classified(ErrCodeConflict,
				"output version changed for verification %s: frozen %s, restart declares %s",
				v.ID, v.OutputVersion, in.OutputVersion)
		}
		for i := range v.Blocks {
			b := &v.Blocks[i]
			if b.Status == BlockMatched {
				continue // 已通过的块直接复用，不重复拷贝/校验
			}
			if b.Status == BlockFailed {
				b.Status = BlockPending
				b.ObservedDigest = ""
				b.LastDetail = "reset by restore restart"
			}
		}
		v.UpdatedAt = s.timeNow()
		tx.PutVerification(*v)
		out = cloneVerification(v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
