package backuprestore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// state 是存储的完整根对象。Update 事务在它的深拷贝上执行，
// 提交成功后整体替换，因此每个事务都工作在一个可序列化的一致快照上：
// 保留清理看到的引用关系，绝不会是快照登记/恢复创建执行到一半的中间状态。
type state struct {
	Snapshots     map[string]*Snapshot    `json:"snapshots"`
	Tasks         map[string]*RestoreTask `json:"tasks"`
	Outbox        map[string]*OutboxEvent `json:"outbox"`
	OutboxByTask  map[string]string       `json:"outbox_by_task"`
	LeaseEvents   []LeaseEvent            `json:"lease_events"`
	RetentionRuns []*RetentionRun         `json:"retention_runs"`
	IDSeq         map[string]int64        `json:"id_seq"`
}

func newState() *state {
	return &state{
		Snapshots:    map[string]*Snapshot{},
		Tasks:        map[string]*RestoreTask{},
		Outbox:       map[string]*OutboxEvent{},
		OutboxByTask: map[string]string{},
		IDSeq:        map[string]int64{},
	}
}

func (s *state) clone() *state {
	c := newState()
	for k, v := range s.Snapshots {
		c.Snapshots[k] = cloneSnapshot(v)
	}
	for k, v := range s.Tasks {
		c.Tasks[k] = cloneTask(v)
	}
	for k, v := range s.Outbox {
		cv := *v
		c.Outbox[k] = &cv
	}
	for k, v := range s.OutboxByTask {
		c.OutboxByTask[k] = v
	}
	c.LeaseEvents = append(c.LeaseEvents, s.LeaseEvents...)
	for _, r := range s.RetentionRuns {
		c.RetentionRuns = append(c.RetentionRuns, cloneRetentionRun(r))
	}
	for k, v := range s.IDSeq {
		c.IDSeq[k] = v
	}
	return c
}

func cloneSnapshot(s *Snapshot) *Snapshot {
	if s == nil {
		return nil
	}
	c := *s
	if s.CompletedAt != nil {
		t := *s.CompletedAt
		c.CompletedAt = &t
	}
	return &c
}

func cloneTask(t *RestoreTask) *RestoreTask {
	if t == nil {
		return nil
	}
	c := *t
	c.Chain = append([]FrozenSnapshot(nil), t.Chain...)
	c.Steps = make([]RestoreStep, len(t.Steps))
	for i, st := range t.Steps {
		cs := st
		if st.StartedAt != nil {
			tt := *st.StartedAt
			cs.StartedAt = &tt
		}
		if st.UpdatedAt != nil {
			tt := *st.UpdatedAt
			cs.UpdatedAt = &tt
		}
		c.Steps[i] = cs
	}
	if t.StartedAt != nil {
		tt := *t.StartedAt
		c.StartedAt = &tt
	}
	if t.CompletedAt != nil {
		tt := *t.CompletedAt
		c.CompletedAt = &tt
	}
	return &c
}

func cloneRetentionRun(r *RetentionRun) *RetentionRun {
	if r == nil {
		return nil
	}
	c := *r
	c.Rules = append([]RetentionRule(nil), r.Rules...)
	c.Decisions = make([]RetentionDecision, len(r.Decisions))
	for i, d := range r.Decisions {
		cd := d
		cd.Reasons = append([]string(nil), d.Reasons...)
		c.Decisions[i] = cd
	}
	return &c
}

// Tx 是一个可序列化事务。读方法返回内部拷贝的指针，可直接读取；
// 写方法以值传入，由事务负责存入拷贝。事务返回错误则全部修改丢弃。
type Tx struct {
	st *state
}

// ---------- 读方法 ----------

func (tx *Tx) GetSnapshot(id string) (*Snapshot, bool) {
	s, ok := tx.st.Snapshots[id]
	return s, ok
}

func sortedSnapshots(in map[string]*Snapshot) []*Snapshot {
	out := make([]*Snapshot, 0, len(in))
	for _, s := range in {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ListSnapshots 按创建时间列出全部快照。
func (tx *Tx) ListSnapshots() []*Snapshot { return sortedSnapshots(tx.st.Snapshots) }

// ListSnapshotsByDataset 列出指定数据集的快照，按创建时间排序。
func (tx *Tx) ListSnapshotsByDataset(datasetID string) []*Snapshot {
	out := make([]*Snapshot, 0)
	for _, s := range sortedSnapshots(tx.st.Snapshots) {
		if s.DatasetID == datasetID {
			out = append(out, s)
		}
	}
	return out
}

// ListChildren 列出直接挂在 parentID 之下的快照（用于“未完成子快照”保护）。
func (tx *Tx) ListChildren(parentID string) []*Snapshot {
	out := make([]*Snapshot, 0)
	for _, s := range sortedSnapshots(tx.st.Snapshots) {
		if s.ParentID == parentID {
			out = append(out, s)
		}
	}
	return out
}

func (tx *Tx) GetTask(id string) (*RestoreTask, bool) {
	t, ok := tx.st.Tasks[id]
	return t, ok
}

// FindActiveTaskByEnv 返回目标环境上当前仍有效的恢复任务（含 pending/running）。
func (tx *Tx) FindActiveTaskByEnv(targetEnv string) (*RestoreTask, bool) {
	var found *RestoreTask
	for _, t := range tx.st.Tasks {
		if t.TargetEnvironment == targetEnv && t.Active() {
			if found == nil || t.CreatedAt.After(found.CreatedAt) {
				found = t
			}
		}
	}
	return found, found != nil
}

// ListTasks 按创建时间列出全部恢复任务。
func (tx *Tx) ListTasks() []*RestoreTask {
	out := make([]*RestoreTask, 0, len(tx.st.Tasks))
	for _, t := range tx.st.Tasks {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (tx *Tx) GetOutboxEvent(id string) (*OutboxEvent, bool) {
	e, ok := tx.st.Outbox[id]
	return e, ok
}

func (tx *Tx) GetOutboxByTask(taskID string) (*OutboxEvent, bool) {
	id, ok := tx.st.OutboxByTask[taskID]
	if !ok {
		return nil, false
	}
	return tx.st.Outbox[id], true
}

// ListOutbox 按创建时间列出全部 outbox 事件。
func (tx *Tx) ListOutbox() []*OutboxEvent {
	out := make([]*OutboxEvent, 0, len(tx.st.Outbox))
	for _, e := range tx.st.Outbox {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ListLeaseEvents 按发生顺序返回租约审计记录。
func (tx *Tx) ListLeaseEvents() []LeaseEvent {
	return append([]LeaseEvent(nil), tx.st.LeaseEvents...)
}

// ListRetentionRuns 按决策时间列出全部保留运行记录。
func (tx *Tx) ListRetentionRuns() []*RetentionRun {
	out := make([]*RetentionRun, 0, len(tx.st.RetentionRuns))
	for _, r := range tx.st.RetentionRuns {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].DecidedAt.Equal(out[j].DecidedAt) {
			return out[i].DecidedAt.Before(out[j].DecidedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ---------- 写方法 ----------

func (tx *Tx) PutSnapshot(s Snapshot) { tx.st.Snapshots[s.ID] = &s }

func (tx *Tx) DeleteSnapshot(id string) {
	delete(tx.st.Snapshots, id)
}

func (tx *Tx) PutTask(t RestoreTask) { tx.st.Tasks[t.ID] = &t }

// AddOutboxEvent 写入通知事件；每个任务最多一条，重复写入返回 conflict，
// 使“任务完成只能写出一次 outbox”在存储层也有兜底保证。
func (tx *Tx) AddOutboxEvent(e OutboxEvent) error {
	if existing, ok := tx.st.OutboxByTask[e.TaskID]; ok {
		return classified(ErrCodeConflict, "outbox event for task %s already exists: %s", e.TaskID, existing)
	}
	tx.st.Outbox[e.ID] = &e
	tx.st.OutboxByTask[e.TaskID] = e.ID
	return nil
}

// MarkOutboxDelivered 由投递方在成功投递后调用（at-least-once 投递的幂等记账）。
func (tx *Tx) MarkOutboxDelivered(eventID string, at time.Time) error {
	e, ok := tx.st.Outbox[eventID]
	if !ok {
		return classified(ErrCodeNotFound, "outbox event %s not found", eventID)
	}
	if e.DeliveredAt == nil {
		t := at
		e.DeliveredAt = &t
	}
	return nil
}

func (tx *Tx) AddLeaseEvent(e LeaseEvent) { tx.st.LeaseEvents = append(tx.st.LeaseEvents, e) }

func (tx *Tx) AddRetentionRun(r RetentionRun) { tx.st.RetentionRuns = append(tx.st.RetentionRuns, &r) }

// NewID 生成持久化的单调递增 ID（重启不回退）。
func (tx *Tx) NewID(kind string) string {
	n := tx.st.IDSeq[kind] + 1
	tx.st.IDSeq[kind] = n
	return fmt.Sprintf("%s-%06d", kind, n)
}

// Store 是事务式持久化接口。实现必须保证 Update 串行执行、
// 且每个事务看到的是某个一致时刻的完整状态。
type Store interface {
	Update(fn func(tx *Tx) error) error
	View(fn func(tx *Tx) error) error
	Close() error
}

// memStore 是进程内串行化存储，所有 Update/View 在同一把锁上排队，
// 天然给出可串行化隔离。它也被 FileStore 复用作内存状态。
type memStore struct {
	mu   sync.RWMutex
	root *state
}

func newMemStore() *memStore { return &memStore{root: newState()} }

// NewMemoryStore 创建纯内存存储（进程结束即丢失，适合测试）。
func NewMemoryStore() Store { return newMemStore() }

func (m *memStore) Update(fn func(tx *Tx) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w := m.root.clone()
	if err := fn(&Tx{st: w}); err != nil {
		return err // 丢弃工作拷贝
	}
	m.root = w
	return nil
}

func (m *memStore) View(fn func(tx *Tx) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fn(&Tx{st: m.root}) // 读锁内只读使用
}

func (m *memStore) Close() error { return nil }

// FileStore 在每次成功提交后把状态原子落盘（临时文件 + rename），
// 重启时从同一文件恢复，关系/状态/outbox/租约记录全部持久化。
type FileStore struct {
	path  string
	mu    sync.Mutex
	inner *memStore
}

// NewFileStore 打开 path 上的持久化存储；文件不存在则初始化为空。
func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{path: path, inner: newMemStore()}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(data) > 0 {
			loaded := newState()
			if err := json.Unmarshal(data, loaded); err != nil {
				return nil, wrapErr(ErrCodeUnavailable, err, "load state from %s", path)
			}
			if loaded.Snapshots == nil {
				loaded = newState()
			}
			fs.inner.root = loaded
		}
	case os.IsNotExist(err):
		// 空库
	default:
		return nil, wrapErr(ErrCodeUnavailable, err, "read state file %s", path)
	}
	return fs, nil
}

func (f *FileStore) Update(fn func(tx *Tx) error) error {
	// 与落盘串在同一临界区：提交与持久化原子发生，调用方返回成功即已 durable。
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.inner.Update(func(tx *Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		return f.persist(tx.st)
	})
	return err
}

func (f *FileStore) View(fn func(tx *Tx) error) error { return f.inner.View(fn) }

func (f *FileStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.persist(f.inner.root)
}

func (f *FileStore) persist(s *state) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return wrapErr(ErrCodeUnavailable, err, "marshal state")
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return wrapErr(ErrCodeUnavailable, err, "create temp file in %s", dir)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return wrapErr(ErrCodeUnavailable, err, "write temp file")
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return wrapErr(ErrCodeUnavailable, err, "fsync temp file")
	}
	if err := tmp.Close(); err != nil {
		return wrapErr(ErrCodeUnavailable, err, "close temp file")
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return wrapErr(ErrCodeUnavailable, err, "rename temp file to %s", f.path)
	}
	return nil
}
