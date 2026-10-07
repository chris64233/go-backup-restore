package backuprestore

import "time"

// SnapshotKind 区分完整快照与增量快照。
type SnapshotKind string

const (
	// KindFull 是一条备份链的根，不允许声明父快照。
	KindFull SnapshotKind = "full"
	// KindIncremental 增量快照必须挂在同一数据集、已完成的父快照之后。
	KindIncremental SnapshotKind = "incremental"
)

// SnapshotStatus 是快照的完成状态。登记后为 pending，
// 只能一次性迁移到 completed 或 failed，终态不可变。
type SnapshotStatus string

const (
	StatusPending   SnapshotStatus = "pending"
	StatusCompleted SnapshotStatus = "completed"
	StatusFailed    SnapshotStatus = "failed"
)

// Snapshot 是不可变的快照记录。Digest 与 ParentID 一经登记永不修改；
// Status 只允许 pending -> completed|failed 一次迁移。
type Snapshot struct {
	ID            string
	DatasetID     string
	Kind          SnapshotKind
	ParentID      string
	Digest        string
	Status        SnapshotStatus
	FailureReason string
	CreatedAt     time.Time
	CompletedAt   *time.Time
}

// FrozenSnapshot 是恢复任务创建时冻结下来的链节点。
// 即使后续保留清理运行，活动恢复引用的快照也不允许被删除。
type FrozenSnapshot struct {
	Index      int
	SnapshotID string
	Digest     string
	Kind       SnapshotKind
}

// TaskStatus 恢复任务状态机：pending -> running -> succeeded。
// 失败的步骤允许重试，因此任务没有 failed 终态。
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskSucceeded TaskStatus = "succeeded"
)

func (t TaskStatus) active() bool { return t == TaskPending || t == TaskRunning }

// StepStatus 单个恢复步骤的状态。
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepSucceeded StepStatus = "succeeded"
	StepFailed    StepStatus = "failed"
)

// RestoreStep 描述链上的一个有序恢复步骤。
// ExecutionVersion 在每次（重新）派发时单调递增，回执必须携带当前版本，
// 从而丢弃上一次尝试的迟到回执。
type RestoreStep struct {
	Index            int
	SnapshotID       string
	Digest           string
	Status           StepStatus
	ExecutionVersion int32
	Attempts         int32
	StartedAt        *time.Time
	UpdatedAt        *time.Time
	LastDetail       string
}

// RestoreTask 是一次恢复：链在创建时冻结，租约按 epoch 递增接管。
type RestoreTask struct {
	ID                string
	DatasetID         string
	TargetEnvironment string
	TargetSnapshotID  string
	Chain             []FrozenSnapshot
	Steps             []RestoreStep
	Status            TaskStatus
	LeaseID           string
	LeaseEpoch        int64
	LeaseHolder       string
	CreatedAt         time.Time
	StartedAt         *time.Time
	CompletedAt       *time.Time
}

// Active 表示任务是否仍占用目标环境与链上快照。
func (t RestoreTask) Active() bool { return t.Status.active() }

// LeaseEvent 记录租约获取、接管与释放，便于审计接管序列。
type LeaseEvent struct {
	At      time.Time
	TaskID  string
	LeaseID string
	Epoch   int64
	Holder  string
	Action  string // acquired | taken_over | released
	Reason  string
}

const (
	LeaseAcquired  = "acquired"
	LeaseTakenOver = "taken_over"
	LeaseReleased  = "released"
)

// OutboxEvent 是任务完成时写出的通知。同一任务只会写出一条。
type OutboxEvent struct {
	ID                string
	TaskID            string
	Type              string
	DatasetID         string
	TargetEnvironment string
	TargetSnapshotID  string
	CreatedAt         time.Time
	DeliveredAt       *time.Time
}

const OutboxRestoreCompleted = "restore_completed"

// CompactionTask 是一次链压缩：把一段已完成、连续的快照链合成为新的完整快照。
// 创建时冻结起点、终点、链上每个快照的摘要以及数据集版本，此后执行针对的
// 永远是这份冻结视图。执行租约按 epoch 递增接管，语义与恢复任务一致：
// 旧租约持有者的回执不能推进新接管者的任务。
type CompactionTask struct {
	ID             string
	IdempotencyKey string // 任务号：相同键重复创建返回同一任务，保证幂等
	DatasetID      string
	FromSnapshotID string // 起点，必须为完整快照（链根）
	ToSnapshotID   string // 终点
	Chain          []FrozenSnapshot
	DatasetVersion int64  // 创建时冻结的数据集版本（每次登记/发布递增）
	ExpectedDigest string // 由冻结链推导的内容摘要，发布前必须校验通过
	Steps          []RestoreStep
	Status         TaskStatus
	LeaseID        string
	LeaseEpoch     int64
	LeaseHolder    string
	NewSnapshotID  string // 发布成功后指向新的完整快照
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
}

// Active 表示压缩任务是否仍冻结其链并占用执行租约。
func (t CompactionTask) Active() bool { return t.Status.active() }

// Replacement 记录一个原快照被哪个新完整快照替代（发布时写入，永不修改）。
type Replacement struct {
	OriginalSnapshotID string
	NewSnapshotID      string
	CompactionTaskID   string
	DatasetID          string
	PublishedAt        time.Time
}

// RetentionRule 表达按数据集保留最近 N 个已完成快照的策略。
// KeepLatestCompleted <= 0 表示不按数量保护任何快照。
type RetentionRule struct {
	DatasetID           string
	KeepLatestCompleted int
}

// RetentionDecisionAction 保留清理对单个快照的决定。
type RetentionDecisionAction string

const (
	RetentionDeleted  RetentionDecisionAction = "deleted"
	RetentionRetained RetentionDecisionAction = "retained"
)

// 保留/删除原因码，写入决策记录，解释每个决定的依据。
const (
	ReasonActiveRestore        = "active_restore"
	ReasonActiveCompaction     = "active_compaction"
	ReasonActiveVerification   = "active_verification"
	ReasonRetentionPolicy      = "retention_policy"
	ReasonIncompleteSnapshot   = "incomplete_snapshot"
	ReasonAncestorIncomplete   = "ancestor_of_incomplete_snapshot"
	ReasonAncestorRetained     = "ancestor_of_retained_snapshot"
	ReasonSnapshotInProgress   = "snapshot_in_progress"
	ReasonExpiredUnreferenced  = "expired_and_unreferenced"
	ReasonFailedUnreferenced   = "failed_and_unreferenced"
	ReasonReplacedByCompaction = "replaced_by_compaction"
)

// RetentionDecision 是一次保留运行中对单个快照的决定与原因。
type RetentionDecision struct {
	SnapshotID string
	DatasetID  string
	Action     RetentionDecisionAction
	Reasons    []string
}

// RetentionRun 是一次从一致快照作出的保留决定的完整记录。
type RetentionRun struct {
	ID        string
	DecidedAt time.Time
	Rules     []RetentionRule
	Decisions []RetentionDecision
}

// ---------------------------------------------------------------------------
// 恢复校验
// ---------------------------------------------------------------------------

// ManifestFile 是一次恢复输出清单中的单个文件：文件级摘要与分块摘要都在校验前冻结，
// 此后源备份链的任何变化都不会改变正在校验的范围。
type ManifestFile struct {
	Name         string
	Size         int64
	Digest       string   // 文件级期望摘要
	BlockDigests []string // 每个数据块的期望摘要（按块序）
}

// VerificationStatus 校验记录状态机：
// verifying -> published（全部摘要通过并发布，目录可用）；
// verifying 本身也可以长期停留在部分成功状态（failed 块可重试），
// 记录没有 failed 终态：部分成功永远不等于可用。
type VerificationStatus string

const (
	VerificationVerifying VerificationStatus = "verifying"
	VerificationPublished VerificationStatus = "published"
	// VerificationSuperseded 表示该未发布校验被一次显式重启取代：
	// 记录保留作为历史，但永远不能发布，避免多份互相矛盾的目录。
	VerificationSuperseded VerificationStatus = "superseded"
)

// VerificationBlockStatus 单个数据块的校验状态。
// passed 一经确认不可回退；failed 可以反复重试直到通过。
type VerificationBlockStatus string

const (
	BlockPending VerificationBlockStatus = "pending"
	BlockPassed  VerificationBlockStatus = "passed"
	BlockFailed  VerificationBlockStatus = "failed"
)

// VerificationBlock 是一个数据块的校验结论。ExpectedDigest 在创建时冻结；
// ObservedDigest 记录最近一次实际算出的摘要，摘要不一致时据此指出具体块。
type VerificationBlock struct {
	File           string
	Index          int
	ExpectedDigest string
	Status         VerificationBlockStatus
	ObservedDigest string
	Attempts       int32
	LastDetail     string
	UpdatedAt      *time.Time
}

// RestoreVerification 是一次恢复校验请求的不可变范围与逐步累积的校验结论。
// 范围（恢复计划、目标时间、输出版本、文件清单）在创建时冻结并参与幂等判定；
// 只有全部数据块摘要通过并再次确认输出版本后才发布，发布后不可被静默替换。
type RestoreVerification struct {
	ID                string
	RequestKey        string // 请求幂等键：相同键返回原结论，范围变化报冲突
	RestoreTaskID     string
	DatasetID         string
	TargetEnvironment string
	TargetSnapshotID  string
	Plan              []FrozenSnapshot // 创建时从恢复任务冻结的恢复计划（链 + 摘要）
	TargetTime        time.Time
	OutputVersion     string
	OutputDir         string
	Manifest          []ManifestFile
	ManifestDigest    string // 清单规范化摘要，用于检测重跑差异
	Blocks            []VerificationBlock
	Status            VerificationStatus
	CreatedAt         time.Time
	UpdatedAt         *time.Time
	PublishedAt       *time.Time
}

// Active 表示校验是否仍可能发布（占用恢复计划的引用）。
func (v RestoreVerification) Active() bool { return v.Status == VerificationVerifying }

// Available 表示恢复目录是否可对外使用：只有已发布的校验记录代表可用。
func (v RestoreVerification) Available() bool { return v.Status == VerificationPublished }

// BlockDigestDiff 是一个摘要不一致的数据块定位信息。
type BlockDigestDiff struct {
	File           string
	Index          int
	ExpectedDigest string
	ObservedDigest string
	Detail         string
}

// VerificationView 是校验查询视图：恢复来源、块摘要差异与最终可用状态。
type VerificationView struct {
	Verification *RestoreVerification
	Total        int
	Passed       int
	Failed       int
	Pending      int
	Diffs        []BlockDigestDiff
	Available    bool
}
