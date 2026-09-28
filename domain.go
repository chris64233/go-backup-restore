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
//
// 时间点恢复会沿压缩替代关系构造执行计划：一个压缩产物节点会替代一段原链，
// 因此用 Source 记录该节点的来历（原始备份 or 压缩任务产物），
// 用 ReplacedSnapshots 记录被它折叠掉的原链快照 ID，使整份计划可审计、可复现。
type FrozenSnapshot struct {
	Index      int
	SnapshotID string
	Digest     string
	Kind       SnapshotKind
	// Source 标记节点来历："original" 为原始备份；"compaction" 为压缩任务合成产物。
	Source string
	// CompactionTaskID 在 Source == "compaction" 时指向产出该快照的压缩任务。
	CompactionTaskID string
	// ReplacedSnapshots 是该节点在计划中折叠替代的原链快照（有序，含被压缩段全部节点）。
	// 原始备份节点该列表为空。它们也被冻结引用，任务结束前不得回收。
	ReplacedSnapshots []string
}

const (
	// FrozenSourceOriginal 表示冻结节点是直接登记的原始备份快照。
	FrozenSourceOriginal = "original"
	// FrozenSourceCompaction 表示冻结节点是链压缩合成出的完整快照。
	FrozenSourceCompaction = "compaction"
)

// TaskStatus 恢复/压缩任务状态机：pending -> running -> succeeded | canceled。
// 失败的步骤允许重试，因此任务没有 failed 终态；取消只能发生一次，
// 与完成、回执、派发并发时由可序列化事务保证最终只剩一个终态。
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskSucceeded TaskStatus = "succeeded"
	TaskCanceled  TaskStatus = "canceled"
)

func (t TaskStatus) active() bool { return t == TaskPending || t == TaskRunning }

// RestoreMode 区分恢复任务如何确定其目标。
type RestoreMode string

const (
	// RestoreModeSnapshot 直接指定目标快照（CreateRestore）。
	RestoreModeSnapshot RestoreMode = "snapshot"
	// RestoreModePointInTime 只给定目标时间，由系统选出时间点快照（CreatePointInTimeRestore）。
	RestoreModePointInTime RestoreMode = "point_in_time"
)

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

// RestoreTask 是一次恢复：执行计划在创建时冻结，租约按 epoch 递增接管。
type RestoreTask struct {
	ID                string
	DatasetID         string
	TargetEnvironment string
	// Mode 决定目标如何确定：直接给快照 ID，或只给目标时间。
	Mode             RestoreMode
	TargetSnapshotID string
	// TargetTime 是用户请求恢复到的时间点（UTC）。两种模式都会记录：
	// point_in_time 为用户给定值；snapshot 模式为目标快照的完成时间。
	TargetTime time.Time
	// SelectedSnapshotID 是按时间点规则实际选中的快照；snapshot 模式等于 TargetSnapshotID。
	SelectedSnapshotID string
	// FrozenAt 记录执行计划冻结的时刻，供审计“这份计划看到的是哪个瞬间的世界”。
	FrozenAt    time.Time
	Chain       []FrozenSnapshot
	Steps       []RestoreStep
	Status      TaskStatus
	LeaseID     string
	LeaseEpoch  int64
	LeaseHolder string
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	// CanceledAt 在取消成功时写入；CancelReason 记录取消原因。
	CanceledAt   *time.Time
	CancelReason string
}

// Active 表示任务是否仍占用目标环境与链上快照。
func (t RestoreTask) Active() bool { return t.Status.active() }

// PlanNodeView 是执行计划中单个节点的只读视图，回答“这一步实际恢复的是哪份数据”。
type PlanNodeView struct {
	Index             int
	SnapshotID        string
	Digest            string
	Kind              SnapshotKind
	Source            string
	CompactionTaskID  string
	ReplacedSnapshots []string // 压缩产物节点折叠的原链快照；原始备份为空
}

// StepFailureView 标记一个未成功步骤的当前位置与最近一次失败信息。
type StepFailureView struct {
	Index      int
	SnapshotID string
	Status     StepStatus
	Attempts   int32
	LastDetail string
}

// RestoreProgress 是恢复任务的可观测进度：实际使用的备份链（含压缩溯源）、
// 步骤计数、首个失败位置，以及全部未成功步骤。用于“查到实际使用的备份链和失败位置”。
type RestoreProgress struct {
	TaskID    string
	Status    TaskStatus
	DatasetID string
	Mode      RestoreMode
	// TargetTime 是请求恢复到的时间；SelectedSnapshotID 是实际选中的时间点快照。
	TargetTime         time.Time
	SelectedSnapshotID string
	Plan               []PlanNodeView
	Total              int
	Pending            int
	Running            int
	Succeeded          int
	Failed             int
	// CompletedSteps 是已成功（绝不重复应用）的步骤数。
	CompletedSteps int
	// FirstFailed 是第一个未成功的步骤（恢复将从这里继续）；全部成功或尚未派发时为 nil。
	FirstFailed *StepFailureView
	// Failures 列出全部未成功步骤（pending/failed/running）。
	Failures []StepFailureView
}

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
