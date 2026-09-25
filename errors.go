package backuprestore

import "errors"

// 错误分类。所有公开 API 返回的错误都可以通过 errors.Is 与下列哨兵错误比较，
// 便于调用方按类别处理（重试、拒绝、告警等）。
var (
	// ErrNotFound：快照、任务、租约等实体不存在（或已被删除）。
	ErrNotFound = errors.New("backuprestore: not found")

	// ErrInvalidInput：请求参数不合法（缺字段、类型错误等）。
	ErrInvalidInput = errors.New("backuprestore: invalid input")

	// ErrInvalidTransition：状态机不允许的迁移（重复完成、回退、失败快照转完成等）。
	ErrInvalidTransition = errors.New("backuprestore: invalid state transition")

	// ErrSnapshotNotCompleted：增量快照的父或恢复目标不是已完成快照。
	ErrSnapshotNotCompleted = errors.New("backuprestore: snapshot not completed")

	// ErrSnapshotFailed：试图使用失败快照（恢复目标或父快照）。
	ErrSnapshotFailed = errors.New("backuprestore: snapshot failed")

	// ErrChainCycle：父指针沿链回溯出现环。
	ErrChainCycle = errors.New("backuprestore: parent chain has a cycle")

	// ErrCrossDataset：增量快照与其父不属于同一数据集。
	ErrCrossDataset = errors.New("backuprestore: cross-dataset parent")

	// ErrDigestConflict：同一快照 ID 被以不同摘要重复登记（摘要不可变）。
	ErrDigestConflict = errors.New("backuprestore: digest conflict")

	// ErrLeaseConflict：目标环境上已存在另一个有效恢复租约。
	ErrLeaseConflict = errors.New("backuprestore: lease conflict")

	// ErrStaleLease：回执携带的租约/栅栏令牌已过期（旧租约的迟到回执）。
	ErrStaleLease = errors.New("backuprestore: stale lease")

	// ErrVersionMismatch：回执的执行版本与当前步骤执行版本不一致。
	ErrVersionMismatch = errors.New("backuprestore: execution version mismatch")

	// ErrStepOrder：试图回执非当前待执行步骤（步骤必须沿链有序推进）。
	ErrStepOrder = errors.New("backuprestore: step out of order")

	// ErrJobFinished：任务已是终态（成功/被接管/取消），不再接受任何回执。
	ErrJobFinished = errors.New("backuprestore: job already finished")
)
