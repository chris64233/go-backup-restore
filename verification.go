package backuprestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

// 恢复校验：恢复结果在发布（目录标记为可用）前，必须逐块核对摘要；
// 校验范围（恢复计划、目标时间、输出版本、文件清单）在创建时冻结，
// 源备份链此后的变化不会悄悄改变正在校验的范围。

// CreateVerificationInput 是提交恢复校验请求的入参。
type CreateVerificationInput struct {
	RequestKey    string // 幂等键：同一请求重复提交返回原结论
	RestoreTaskID string
	TargetTime    time.Time // 目标恢复时间点
	OutputVersion string    // 输出版本标识，发布前必须再次确认
	OutputDir     string    // 恢复目录标识
	Manifest      []ManifestFile
	// Restart 显式声明这是一次恢复任务重启后的重新校验：
	// 会把同一任务当前未发布的旧校验置为 superseded（保留历史但禁止发布），
	// 并仅复用其已通过的块；普通重复创建不允许顶替在途校验。
	Restart bool
}

// manifestDigest 对冻结清单与范围计算规范化摘要，使“范围是否相同”可比较、可持久化。
func manifestDigest(plan []FrozenSnapshot, targetTime time.Time, outputVersion string, files []ManifestFile) string {
	h := sha256.New()
	fmt.Fprintf(h, "restore-verification/v1\n")
	fmt.Fprintf(h, "target_time=%s\n", targetTime.UTC().Format(time.RFC3339Nano))
	fmt.Fprintf(h, "output_version=%s\n", outputVersion)
	for _, p := range plan {
		fmt.Fprintf(h, "plan %d %s %s %s\n", p.Index, p.SnapshotID, p.Kind, p.Digest)
	}
	ordered := append([]ManifestFile(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	for _, f := range ordered {
		fmt.Fprintf(h, "file %s %d %s\n", f.Name, f.Size, f.Digest)
		for i, d := range f.BlockDigests {
			fmt.Fprintf(h, "block %s %d %s\n", f.Name, i, d)
		}
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func blockKey(file string, index int) string { return fmt.Sprintf("%s#%d", file, index) }

// CreateVerification 创建一条恢复校验记录：
//  1. 恢复任务必须存在且已成功（恢复步骤全部完成）；
//  2. 从恢复任务冻结恢复计划与链摘要，与目标时间、输出版本、清单一起锁定；
//  3. 请求键已存在：范围完全相同返回原记录（原结论），范围不同报 conflict；
//  4. 同一恢复任务只允许一个未发布的校验，避免多份互相矛盾的目录同时推进；
//  5. 恢复任务重启重跑：新记录复用“文件+块号+期望摘要”完全一致且已通过的块，
//     失败块/在途块与输出版本必须重新确认（新块从 pending 开始）。
//
// 已发布的结果永不被静默替换：即使清单与旧记录完全相同也创建新的校验记录。
func (s *Service) CreateVerification(ctx context.Context, in CreateVerificationInput) (*RestoreVerification, error) {
	if in.RequestKey == "" || in.RestoreTaskID == "" || in.OutputVersion == "" {
		return nil, classified(ErrCodeInvalidArgument,
			"request key, restore task id and output version are required")
	}
	if len(in.Manifest) == 0 {
		return nil, classified(ErrCodeInvalidArgument, "manifest must contain at least one file")
	}
	seen := map[string]bool{}
	for _, f := range in.Manifest {
		if f.Name == "" {
			return nil, classified(ErrCodeInvalidArgument, "manifest entry name is required")
		}
		if seen[f.Name] {
			return nil, classified(ErrCodeInvalidArgument, "duplicate manifest entry %s", f.Name)
		}
		seen[f.Name] = true
		if len(f.BlockDigests) == 0 {
			return nil, classified(ErrCodeInvalidArgument, "file %s requires at least one block digest", f.Name)
		}
		for _, d := range f.BlockDigests {
			if d == "" {
				return nil, classified(ErrCodeInvalidArgument, "file %s has empty block digest", f.Name)
			}
		}
	}

	var out *RestoreVerification
	err := s.store.Update(func(tx *Tx) error {
		task, ok := tx.GetTask(in.RestoreTaskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", in.RestoreTaskID)
		}
		target := in.TargetTime.UTC()
		plan := append([]FrozenSnapshot(nil), task.Chain...)
		wantDigest := manifestDigest(plan, target, in.OutputVersion, in.Manifest)

		if existing, ok := tx.FindVerificationByRequest(in.RequestKey); ok {
			if existing.RestoreTaskID != in.RestoreTaskID ||
				!existing.TargetTime.Equal(target) ||
				existing.OutputVersion != in.OutputVersion ||
				existing.ManifestDigest != wantDigest {
				return classified(ErrCodeConflict,
					"verification request key %s already used by %s with different restore plan, target time, output version or manifest",
					in.RequestKey, existing.ID)
			}
			out = cloneVerification(existing)
			return nil
		}

		if task.Status != TaskSucceeded {
			return classified(ErrCodeConflict,
				"restore task %s is %s, only succeeded restores can be verified", task.ID, task.Status)
		}
		// 同一恢复计划同时只允许一个未发布的校验：普通创建显式冲突；
		// 显式重启才把旧校验置为 superseded（保留历史但禁止发布），绝不静默覆盖。
		var prior *RestoreVerification
		if p, ok := tx.LatestVerificationByTask(task.ID); ok && p.Active() {
			if !in.Restart {
				return classified(ErrCodeConflict,
					"restore task %s already has an unpublished verification %s; set Restart to supersede it",
					task.ID, p.ID)
			}
			prior = p
		}

		now := s.timeNow()
		frozenFiles := append([]ManifestFile(nil), in.Manifest...)
		for i := range frozenFiles {
			frozenFiles[i].BlockDigests = append([]string(nil), frozenFiles[i].BlockDigests...)
		}

		v := RestoreVerification{
			ID:                tx.NewID("verify"),
			RequestKey:        in.RequestKey,
			RestoreTaskID:     task.ID,
			DatasetID:         task.DatasetID,
			TargetEnvironment: task.TargetEnvironment,
			TargetSnapshotID:  task.TargetSnapshotID,
			Plan:              plan,
			TargetTime:        target,
			OutputVersion:     in.OutputVersion,
			OutputDir:         in.OutputDir,
			Manifest:          frozenFiles,
			ManifestDigest:    wantDigest,
			Status:            VerificationVerifying,
			CreatedAt:         now,
		}

		// 恢复任务重启：仅复用“同一文件、同一块号、同一期望摘要”的已通过块；
		// 失败块与新块一律重新校验；输出版本作为新范围的一部分重新确认。
		priorID := ""
		if prior != nil {
			priorID = prior.ID
			passed := map[string]VerificationBlock{}
			for _, b := range prior.Blocks {
				if b.Status == BlockPassed {
					passed[blockKey(b.File, b.Index)] = b
				}
			}
			for _, f := range v.Manifest {
				for i, d := range f.BlockDigests {
					b := VerificationBlock{File: f.Name, Index: i, ExpectedDigest: d, Status: BlockPending}
					if old, ok := passed[blockKey(f.Name, i)]; ok && old.ExpectedDigest == d {
						b.Status = BlockPassed
						b.ObservedDigest = old.ObservedDigest
						b.Attempts = old.Attempts
						b.LastDetail = "reused from verification " + priorID
						t := now
						b.UpdatedAt = &t
					}
					v.Blocks = append(v.Blocks, b)
				}
			}
		} else {
			for _, f := range v.Manifest {
				for i, d := range f.BlockDigests {
					v.Blocks = append(v.Blocks, VerificationBlock{
						File: f.Name, Index: i, ExpectedDigest: d, Status: BlockPending,
					})
				}
			}
		}
		t2 := now
		v.UpdatedAt = &t2
		if prior != nil {
			prior.Status = VerificationSuperseded
			prior.UpdatedAt = &now
			tx.PutVerification(*prior)
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

// BlockResult 是一个数据块的实际校验结果。
type BlockResult struct {
	File           string
	Index          int
	ObservedDigest string
	Success        bool
	Detail         string
}

// VerifyBlocksInput 是分批提交数据块校验结果的入参，一批可以跨文件。
type VerifyBlocksInput struct {
	VerificationID string
	Results        []BlockResult
}

// VerifyBlocks 分批确认数据块摘要：
//   - 实际摘要等于冻结期望（或显式成功且摘要匹配）-> passed，一经通过不可回退；
//   - 摘要不一致或显式失败 -> failed，记录实际摘要，允许后续重试；
//   - 已发布记录拒绝再写，保证发布结论不被迟到结果改变。
//
// 部分成功只更新本批块，其余保持原状：全部摘要通过之前目录始终不可用。
func (s *Service) VerifyBlocks(ctx context.Context, in VerifyBlocksInput) (*RestoreVerification, error) {
	if len(in.Results) == 0 {
		return nil, classified(ErrCodeInvalidArgument, "at least one block result is required")
	}
	for _, r := range in.Results {
		if r.File == "" {
			return nil, classified(ErrCodeInvalidArgument, "block result file is required")
		}
	}
	var out *RestoreVerification
	err := s.store.Update(func(tx *Tx) error {
		v, ok := tx.GetVerification(in.VerificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", in.VerificationID)
		}
		if !v.Active() {
			return classified(ErrCodeConflict, "verification %s is already %s", v.ID, v.Status)
		}
		index := make(map[string]int, len(v.Blocks))
		for i := range v.Blocks {
			index[blockKey(v.Blocks[i].File, v.Blocks[i].Index)] = i
		}
		now := s.timeNow()
		for _, r := range in.Results {
			pos, exists := index[blockKey(r.File, r.Index)]
			if !exists {
				return classified(ErrCodeNotFound,
					"block %s index %d is not part of verification %s frozen manifest", r.File, r.Index, v.ID)
			}
			b := &v.Blocks[pos]
			if b.Status == BlockPassed {
				continue // 已通过的块不可回退
			}
			b.Attempts++
			b.ObservedDigest = r.ObservedDigest
			if r.Success && r.ObservedDigest == b.ExpectedDigest {
				b.Status = BlockPassed
			} else {
				b.Status = BlockFailed
			}
			b.LastDetail = r.Detail
			t := now
			b.UpdatedAt = &t
		}
		t2 := now
		v.UpdatedAt = &t2
		tx.PutVerification(*v)
		out = cloneVerification(v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PublishVerificationInput 发布校验结果的入参。OutputVersion 必须等于创建时冻结的值，
// 是发布前对输出版本的最后一次重新确认。
type PublishVerificationInput struct {
	VerificationID string
	OutputVersion  string
}

// PublishVerification 在单个事务内发布：
//  1. 记录仍未发布（对已发布记录重复调用幂等返回原结论）；
//  2. 输出版本与冻结值一致（重新确认，漂移则要求重跑并新建校验）；
//  3. 恢复计划仍与冻结一致（源备份链变化不能影响已锁定范围）；
//  4. 全部数据块摘要通过：任何 failed/pending 块都拒绝发布，目录保持不可用。
//
// 发布只把该记录置为 published，绝不修改或删除任何历史记录。
func (s *Service) PublishVerification(ctx context.Context, in PublishVerificationInput) (*RestoreVerification, error) {
	var out *RestoreVerification
	err := s.store.Update(func(tx *Tx) error {
		v, ok := tx.GetVerification(in.VerificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", in.VerificationID)
		}
		if !v.Active() {
			if v.Status == VerificationPublished {
				out = cloneVerification(v) // 幂等：返回同一结论
				return nil
			}
			return classified(ErrCodeConflict,
				"verification %s is %s and can never be published", v.ID, v.Status)
		}
		if in.OutputVersion != v.OutputVersion {
			return classified(ErrCodeConflict,
				"output version drift for verification %s: frozen %s, got %s; create a new verification for the new output",
				v.ID, v.OutputVersion, in.OutputVersion)
		}
		task, ok := tx.GetTask(v.RestoreTaskID)
		if !ok {
			return classified(ErrCodeConflict, "restore task %s no longer exists", v.RestoreTaskID)
		}
		if task.Status != TaskSucceeded {
			return classified(ErrCodeConflict, "restore task %s is %s", task.ID, task.Status)
		}
		if len(task.Chain) != len(v.Plan) {
			return classified(ErrCodeConflict, "restore plan changed since verification %s was frozen", v.ID)
		}
		for i, p := range v.Plan {
			cur := task.Chain[i]
			if cur.SnapshotID != p.SnapshotID || cur.Digest != p.Digest {
				return classified(ErrCodeConflict,
					"restore plan changed since verification %s was frozen at step %d", v.ID, i)
			}
		}
		for _, b := range v.Blocks {
			if b.Status != BlockPassed {
				return classified(ErrCodeConflict,
					"verification %s cannot publish: block %s#%d is %s", v.ID, b.File, b.Index, b.Status)
			}
		}
		now := s.timeNow()
		v.Status = VerificationPublished
		v.PublishedAt = &now
		v.UpdatedAt = &now
		tx.PutVerification(*v)
		out = cloneVerification(v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// buildView 汇总校验视图：恢复来源、块计数、摘要差异定位与最终可用状态。
func buildView(v *RestoreVerification) *VerificationView {
	view := &VerificationView{Verification: v, Available: v.Available()}
	view.Total = len(v.Blocks)
	for _, b := range v.Blocks {
		switch b.Status {
		case BlockPassed:
			view.Passed++
		case BlockFailed:
			view.Failed++
			diff := BlockDigestDiff{
				File: b.File, Index: b.Index,
				ExpectedDigest: b.ExpectedDigest,
				ObservedDigest: b.ObservedDigest,
				Detail:         b.LastDetail,
			}
			if b.ObservedDigest == "" {
				diff.Detail = "block check failed without an observed digest"
			} else if b.ObservedDigest != b.ExpectedDigest {
				diff.Detail = fmt.Sprintf("digest mismatch on %s block %d", b.File, b.Index)
			}
			view.Diffs = append(view.Diffs, diff)
		case BlockPending:
			view.Pending++
		}
	}
	return view
}

// GetVerification 查询单条校验记录的完整视图（含来源、块差异与可用状态）。
func (s *Service) GetVerification(ctx context.Context, verificationID string) (*VerificationView, error) {
	var out *VerificationView
	err := s.store.View(func(tx *Tx) error {
		v, ok := tx.GetVerification(verificationID)
		if !ok {
			return classified(ErrCodeNotFound, "verification %s not found", verificationID)
		}
		out = buildView(cloneVerification(v))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListVerifications 按恢复任务列出全部校验记录（含已发布历史与新差异记录），
// restoreTaskID 为空时列出全部，按创建时间排序。
func (s *Service) ListVerifications(ctx context.Context, restoreTaskID string) ([]*VerificationView, error) {
	var out []*VerificationView
	err := s.store.View(func(tx *Tx) error {
		for _, v := range tx.ListVerifications() {
			if restoreTaskID != "" && v.RestoreTaskID != restoreTaskID {
				continue
			}
			out = append(out, buildView(cloneVerification(v)))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RestoreOutputStatus 是恢复目录对外可用性的查询结果。
// Published 是当前已发布（可用）的校验记录；Records 是该恢复任务的全部校验记录，
// 因此重跑产生的新差异记录可被显式看到，而不会静默顶替已发布结果。
type RestoreOutputStatus struct {
	RestoreTaskID     string
	DatasetID         string
	TargetEnvironment string
	OutputDir         string
	Available         bool
	Published         *VerificationView
	Records           []*VerificationView
}

// GetRestoreOutputStatus 汇总一个恢复任务对应目录的最终可用状态与恢复来源。
func (s *Service) GetRestoreOutputStatus(ctx context.Context, restoreTaskID string) (*RestoreOutputStatus, error) {
	var out *RestoreOutputStatus
	err := s.store.View(func(tx *Tx) error {
		task, ok := tx.GetTask(restoreTaskID)
		if !ok {
			return classified(ErrCodeNotFound, "restore task %s not found", restoreTaskID)
		}
		st := &RestoreOutputStatus{
			RestoreTaskID:     task.ID,
			DatasetID:         task.DatasetID,
			TargetEnvironment: task.TargetEnvironment,
		}
		for _, v := range tx.ListVerificationsByTask(task.ID) {
			view := buildView(cloneVerification(v))
			st.Records = append(st.Records, view)
			if st.OutputDir == "" {
				st.OutputDir = v.OutputDir
			}
			if v.Available() {
				st.Available = true
				st.Published = view
			}
		}
		out = st
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
