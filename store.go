package backuprestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// state 是服务的全部持久化状态：快照关系、任务/租约、outbox 与清理记录。
// 所有修改都必须发生在 Store.Update 的事务回调内。
type state struct {
	Snapshots      map[string]*Snapshot        `json:"snapshots"`
	Jobs           map[string]*RestoreJob      `json:"jobs"`
	Leases         map[string]*Lease           `json:"leases"` // key: targetEnv
	Outbox         map[string]*OutboxMessage   `json:"outbox"`
	CleanupRecords []*CleanupRecord            `json:"cleanup_records"`
	Policies       map[string]*RetentionPolicy `json:"policies"`
	FencingSeq     map[string]uint64           `json:"fencing_seq"`  // key: targetEnv，单调递增
	SnapshotSeq    map[string]uint64           `json:"snapshot_seq"` // key: datasetID，快照序号单调递增
}

func newState() *state {
	return &state{
		Snapshots:   make(map[string]*Snapshot),
		Jobs:        make(map[string]*RestoreJob),
		Leases:      make(map[string]*Lease),
		Outbox:      make(map[string]*OutboxMessage),
		Policies:    make(map[string]*RetentionPolicy),
		FencingSeq:  make(map[string]uint64),
		SnapshotSeq: make(map[string]uint64),
	}
}

// Store 是持久化抽象。Update 提供可串行化的读-改-写事务：
// 回调内对 state 的修改要么全部生效，要么（返回错误时）全部丢弃。
type Store interface {
	// Update 在写锁下执行 fn；fn 返回 nil 时提交修改。
	Update(ctx context.Context, fn func(*state) error) error
	// View 在读锁下执行 fn；fn 不得修改 state。
	View(ctx context.Context, fn func(*state) error) error
}

// MemoryStore 是基于内存的 Store 实现，适合测试与嵌入。
type MemoryStore struct {
	mu sync.RWMutex
	st *state
}

// NewMemoryStore 创建一个空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{st: newState()}
}

func (m *MemoryStore) Update(_ context.Context, fn func(*state) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := cloneState(m.st)
	if err := fn(m.st); err != nil {
		// 回调失败：恢复事务前快照，保证原子回滚。
		m.st = snapshot
		return err
	}
	return nil
}

func cloneState(st *state) *state {
	data, err := json.Marshal(st)
	if err != nil {
		panic(fmt.Sprintf("backuprestore: clone state: %v", err))
	}
	var cp state
	if err := json.Unmarshal(data, &cp); err != nil {
		panic(fmt.Sprintf("backuprestore: clone state: %v", err))
	}
	return &cp
}

func (m *MemoryStore) View(_ context.Context, fn func(*state) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fn(m.st)
}

// FileStore 将状态以 JSON 原子落盘（写临时文件后 rename），
// 进程重启后状态（关系、任务、租约、outbox、清理记录）完整恢复。
type FileStore struct {
	MemoryStore
	path string
}

// NewFileStore 打开（必要时创建）path 指向的 JSON 状态文件。
func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{path: path}
	data, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		fs.MemoryStore.st = newState()
	case err != nil:
		return nil, fmt.Errorf("backuprestore: read store file: %w", err)
	case len(data) == 0:
		fs.MemoryStore.st = newState()
	default:
		st := newState()
		if err := json.Unmarshal(data, st); err != nil {
			return nil, fmt.Errorf("backuprestore: decode store file: %w", err)
		}
		// 兼容旧文件缺字段的情况。
		if st.Snapshots == nil {
			st.Snapshots = make(map[string]*Snapshot)
		}
		if st.Jobs == nil {
			st.Jobs = make(map[string]*RestoreJob)
		}
		if st.Leases == nil {
			st.Leases = make(map[string]*Lease)
		}
		if st.Outbox == nil {
			st.Outbox = make(map[string]*OutboxMessage)
		}
		if st.Policies == nil {
			st.Policies = make(map[string]*RetentionPolicy)
		}
		if st.FencingSeq == nil {
			st.FencingSeq = make(map[string]uint64)
		}
		if st.SnapshotSeq == nil {
			st.SnapshotSeq = make(map[string]uint64)
		}
		fs.MemoryStore.st = st
	}
	return fs, nil
}

// Update 提交事务后将整体状态原子写盘；回调失败时内存状态一并回滚。
func (f *FileStore) Update(ctx context.Context, fn func(*state) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := cloneState(f.st)
	if err := fn(f.st); err != nil {
		f.st = snapshot
		return err
	}
	return f.persistLocked()
}

func (f *FileStore) persistLocked() error {
	data, err := json.MarshalIndent(f.st, "", "  ")
	if err != nil {
		return fmt.Errorf("backuprestore: encode state: %w", err)
	}
	if dir := filepath.Dir(f.path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("backuprestore: create store dir: %w", err)
		}
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("backuprestore: write store file: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("backuprestore: commit store file: %w", err)
	}
	return nil
}
