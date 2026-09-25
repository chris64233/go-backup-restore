package backuprestore

import "time"

// SnapshotKind 区分完整快照与增量快照。
type SnapshotKind string

const (
	// KindFull 完整快照，是一条增量链的根，不允许有父快照。
	KindFull SnapshotKind = "full"
	// KindIncremental 增量快照，必须挂在同一数据集已完成的祖先之后。
	KindIncremental SnapshotKind = "incremental"
)

// SnapshotStatus 是快照的完成状态。
type SnapshotStatus string

const (
	// SnapshotPending 快照已登记但数据尚未完成，不能用于恢复，也不能作为新增量的父。
	SnapshotPending SnapshotStatus = "pending"
	// SnapshotCompleted 快照已完成且摘要不可变，可用于恢复与挂载。
	SnapshotCompleted SnapshotStatus = "completed"
	// SnapshotFailed 快照失败，不能恢复、不能挂载，可被保留清理删除。
	SnapshotFailed SnapshotStatus = "failed"
)

// Snapshot 是不可变的快照记录。
//
// 摘要（Digest）、父指针（ParentID）在登记时确定，此后不可修改；
// 唯一允许迁移的是完成状态：pending -> completed / failed，且只能迁移一次。
type Snapshot struct {
	ID          string         `json:"id"`
	DatasetID   string         `json:"dataset_id"`
	Kind        SnapshotKind   `json:"kind"`
	ParentID    string         `json:"parent_id,omitempty"`
	Digest      string         `json:"digest"`
	Status      SnapshotStatus `json:"status"`
	Sequence    uint64         `json:"sequence"`
	FailReason  string         `json:"fail_reason,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	CompletedAt time.Time      `json:"completed_at,omitempty"`

	// 删除标记由保留清理写入；记录保留用于审计，活动查询默认不返回。
	Deleted      bool      `json:"deleted,omitempty"`
	DeleteReason string    `json:"delete_reason,omitempty"`
	DeletedAt    time.Time `json:"deleted_at,omitempty"`
}

// FrozenSnapshot 是恢复任务创建时冻结下来的链节点。
// 即使之后源快照记录发生任何变化（事实上摘要不可变），恢复仍以冻结内容为准。
type FrozenSnapshot struct {
	Index      int          `json:"index"`
	SnapshotID string       `json:"snapshot_id"`
	Kind       SnapshotKind `json:"kind"`
	Digest     string       `json:"digest"`
}

// RestoreStatus 是恢复任务状态。
type RestoreStatus string

const (
	// RestoreRunning 任务进行中，步骤可回执、可重试，租约有效。
	RestoreRunning RestoreStatus = "running"
	// RestoreSucceeded 全部步骤成功，任务终态，只允许写出一次通知。
	RestoreSucceeded RestoreStatus = "succeeded"
	// RestoreSuperseded 租约被接管者抢走，旧任务终态；其迟到回执一律拒绝。
	RestoreSuperseded RestoreStatus = "superseded"
	// RestoreCanceled 持有当前租约者主动取消，任务终态。
	RestoreCanceled RestoreStatus = "canceled"
)

// StepStatus 是恢复步骤状态。
type StepStatus string

const (
	// StepPending 步骤尚未拿到成功回执（含等待重试）。
	StepPending StepStatus = "pending"
	// StepFailed 最近一次执行失败，允许在当前租约下重试，重试会抬升执行版本。
	StepFailed StepStatus = "failed"
	// StepSucceeded 步骤已成功，不可重复推进。
	StepSucceeded StepStatus = "succeeded"
)

// RestoreStep 是冻结链上的一个有序恢复步骤。
type RestoreStep struct {
	Index            int        `json:"index"`
	SnapshotID       string     `json:"snapshot_id"`
	Status           StepStatus `json:"status"`
	ExecutionVersion uint64     `json:"execution_version"`
	LastError        string     `json:"last_error,omitempty"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// Lease 是目标环境上的恢复租约。同一目标环境同一时刻至多一个有效租约；
// Fencing 是单调递增的栅栏令牌，接管后旧租约的回执会因令牌过旧被拒绝。
type Lease struct {
	ID         string    `json:"id"`
	TargetEnv  string    `json:"target_env"`
	JobID      string    `json:"job_id"`
	Fencing    uint64    `json:"fencing"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// RestoreJob 是恢复任务：链在创建时冻结，租约与步骤执行版本用于回执校验。
type RestoreJob struct {
	ID               string           `json:"id"`
	TargetEnv        string           `json:"target_env"`
	DatasetID        string           `json:"dataset_id"`
	TargetSnapshotID string           `json:"target_snapshot_id"`
	Chain            []FrozenSnapshot `json:"chain"`
	Steps            []*RestoreStep   `json:"steps"`
	Status           RestoreStatus    `json:"status"`
	LeaseID          string           `json:"lease_id"`
	Fencing          uint64           `json:"fencing"`
	CreatedAt        time.Time        `json:"created_at"`
	FinishedAt       time.Time        `json:"finished_at,omitempty"`
}

// OutboxMessage 是任务完成后写出一次的通知。
type OutboxMessage struct {
	ID               string    `json:"id"`
	JobID            string    `json:"job_id"`
	TargetEnv        string    `json:"target_env"`
	DatasetID        string    `json:"dataset_id"`
	TargetSnapshotID string    `json:"target_snapshot_id"`
	Payload          string    `json:"payload"`
	Delivered        bool      `json:"delivered"`
	CreatedAt        time.Time `json:"created_at"`
	DeliveredAt      time.Time `json:"delivered_at,omitempty"`
}

// RetentionDecision 是保留清理对单个快照做出的决定。
type RetentionDecision string

const (
	// Retain 保留快照。
	Retain RetentionDecision = "retain"
	// Delete 删除快照。
	Delete RetentionDecision = "delete"
)

// 保留/删除原因，全部为枚举常量，便于审计与测试断言。
const (
	// ReasonActiveRestore：快照仍处于某个有效恢复任务的冻结链上。
	ReasonActiveRestore = "active-restore-chain"
	// ReasonRetentionPolicy：快照是保留策略选中的头部或是其祖先。
	ReasonRetentionPolicy = "retention-policy"
	// ReasonIncompleteSnapshot：快照自身尚未完成。
	ReasonIncompleteSnapshot = "snapshot-incomplete"
	// ReasonIncompleteChild：快照被未完成的子快照（沿祖先链）引用。
	ReasonIncompleteChild = "incomplete-child-reference"
	// ReasonPolicyExpired：完整/已完成快照不在保留集合内。
	ReasonPolicyExpired = "expired-by-policy"
	// ReasonNoPolicy：数据集未配置保留策略，安全默认保留全部已完成快照。
	ReasonNoPolicy = "no-retention-policy"
	// ReasonFailedSnapshot：失败快照，无恢复价值且不可能有后代。
	ReasonFailedSnapshot = "failed-snapshot"
)

// CleanupRecord 是一次保留清理中对某个快照的决定与原因，只追加、不修改。
type CleanupRecord struct {
	ID         string            `json:"id"`
	DatasetID  string            `json:"dataset_id"`
	SnapshotID string            `json:"snapshot_id"`
	Decision   RetentionDecision `json:"decision"`
	Reason     string            `json:"reason"`
	Detail     string            `json:"detail,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
}

// RetentionPolicy 描述一个数据集的保留策略：保留最近 KeepLatest 个已完成快照
// （自动保留它们的全部祖先）。0 表示不按策略保留任何已完成快照。
type RetentionPolicy struct {
	DatasetID  string    `json:"dataset_id"`
	KeepLatest int       `json:"keep_latest"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// RetentionReport 汇总一次清理的结果。
type RetentionReport struct {
	DatasetID    string
	Decisions    []CleanupRecord
	DeletedCount int
}
