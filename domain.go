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

// CompactionStep 是压缩任务的一个有序执行步骤：把链上第 i 个快照的
// 内容合并进 staging。语义与 RestoreStep 相同——ExecutionVersion 在每次
// （重新）派发时递增，回执必须携带当前版本，旧租约/旧版本的迟到回执被丢弃。
type CompactionStep struct {
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

// CompactionTask 是一次链压缩：把 [StartSnapshotID, EndSnapshotID] 这段
// 连续且全部已完成的快照链合成为一个新的完整快照。
// 创建时冻结起点、终点、链上每个快照的摘要以及数据集版本；
// 此后链内容不可变，冻结视图即执行依据。
type CompactionTask struct {
	ID              string
	IdempotencyKey  string // 任务号：重复创建按此键幂等返回同一任务
	DatasetID       string
	DatasetVersion  int64 // 创建时的数据集版本（每次快照登记递增）
	StartSnapshotID string
	EndSnapshotID   string
	Chain           []FrozenSnapshot
	ExpectedDigest  string // 由冻结链确定的内容摘要，发布时校验
	Steps           []CompactionStep
	Status          TaskStatus
	LeaseID         string
	LeaseEpoch      int64
	LeaseHolder     string
	// 发布结果：新完整快照 ID 与每个原快照到新快照的替代关系。
	NewSnapshotID string
	Replacements  map[string]string
	CreatedAt     time.Time
	StartedAt     *time.Time
	CompletedAt   *time.Time
}

// Active 表示压缩任务是否仍占用链上快照（保留清理不得回收）。
func (t CompactionTask) Active() bool { return t.Status.active() }

// CompactionProgress 是压缩进度的只读视图。
type CompactionProgress struct {
	TaskID         string
	Status         TaskStatus
	TotalSteps     int
	SucceededSteps int
	RunningSteps   int
	FailedSteps    int
	PendingSteps   int
	Attempts       int32
	NewSnapshotID  string // 发布后才有值
}

// CompactionChains 是新旧链查询的结果：Old 为创建时冻结的原链，
// New 为发布后新完整快照所在的链（发布前为空）。
type CompactionChains struct {
	Old []FrozenSnapshot
	New []*Snapshot
}

// SnapshotReferences 列出一个快照当前被哪些活动引用阻断回收。
type SnapshotReferences struct {
	SnapshotID           string
	ActiveRestoreTaskIDs []string // 冻结链包含该快照的有效恢复任务
	ActiveCompactionIDs  []string // 冻结链包含该快照的有效压缩任务
	PendingChildIDs      []string // 引用该快照的未完成子快照
}

// Blocked 表示是否仍存在阻断回收的引用。
func (r SnapshotReferences) Blocked() bool {
	return len(r.ActiveRestoreTaskIDs) > 0 || len(r.ActiveCompactionIDs) > 0 || len(r.PendingChildIDs) > 0
}

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

// OutboxCompactionCompleted 是压缩任务发布新完整快照时写出的通知类型。
const OutboxCompactionCompleted = "compaction_completed"

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
	ReasonActiveRestore       = "active_restore"
	ReasonActiveCompaction    = "active_compaction"
	ReasonRetentionPolicy     = "retention_policy"
	ReasonIncompleteSnapshot  = "incomplete_snapshot"
	ReasonAncestorIncomplete  = "ancestor_of_incomplete_snapshot"
	ReasonAncestorRetained    = "ancestor_of_retained_snapshot"
	ReasonSnapshotInProgress  = "snapshot_in_progress"
	ReasonExpiredUnreferenced = "expired_and_unreferenced"
	ReasonFailedUnreferenced  = "failed_and_unreferenced"
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
