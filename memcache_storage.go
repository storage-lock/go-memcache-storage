package memcache_storage

import (
	"context"
	"encoding/json"
	"fmt"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	ntp_time_provider "github.com/storage-lock/go-ntp-time-provider"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/bradfitz/gomemcache/memcache"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ------------------------------------------------ ---------------------------------------------------------------------

// 存储在 memcache value 中的分隔符
const valueSeparator = "\x00"

// MemcacheStorage 基于 Memcache 的存储引擎实现
//
// Memcache 通过 Add（key 不存在才写入）和 CompareAndSwap（CasID 匹配才写入）
// 提供原生 CAS 支持。这是不同于 Redis Lua 和 etcd Txn 的第三种 CAS 范式。
//
// 注意：Memcache 的 Delete 不支持条件删除，因此 DeleteWithVersion 先用 CAS 写入
// 一个删除标记（空 JSON），再 Delete key。CAS 保证只有版本匹配的持有者能删除。
type MemcacheStorage struct {
	options *MemcacheStorageOptions
	client  *memcache.Client

	timeProvider storage.TimeProvider

	// knownLockIds 进程内维护的已知锁 ID 集合，用于支持 List 操作
	// Memcache 不支持 key 枚举，因此需要进程内记录已创建的锁 ID
	// 注意：这是进程内状态，多进程部署时不一致，但 List 仅用于运维查看，不参与互斥性
	knownLockIds   map[string]struct{}
	knownLockIdsMu sync.RWMutex
}

var _ storage.Storage = &MemcacheStorage{}

// NewMemcacheStorage 创建一个基于 Memcache 的存储引擎
func NewMemcacheStorage(ctx context.Context, options *MemcacheStorageOptions) (*MemcacheStorage, error) {

	if err := options.Check(); err != nil {
		return nil, err
	}

	s := &MemcacheStorage{
		options:      options,
		knownLockIds: make(map[string]struct{}),
	}

	err := s.Init(ctx)
	if err != nil {
		return nil, err
	}

	return s, nil
}

const MemcacheStorageName = "memcache-storage"

func (x *MemcacheStorage) GetName() string {
	return MemcacheStorageName
}

// Capabilities 声明 Memcache 存储支持的能力
// Memcache 通过 Add + CompareAndSwap 提供 CAS，通过 NTP 提供可靠时间
// 注意：Memcache 的 Delete 不支持条件删除，但通过 CAS 写删除标记+Delete 实现
// AtomicDelete 语义（CAS 保证只有版本匹配的持有者能触发删除），声明此能力
func (x *MemcacheStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
		storage.CapabilityAtomicDelete,
	}
}

func (x *MemcacheStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	// 探活幂等：确保连接可用
	if err := client.Ping(); err != nil {
		return err
	}
	x.client = client

	// Memcache 没有时间 API，优先使用 Options 中注入的时间源，否则默认使用 NTP
	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = ntp_time_provider.NewNTPTimeProvider(nil)
	}

	return nil
}

// lockKey 返回 Memcache 中存储锁信息的 key
func (x *MemcacheStorage) lockKey(lockId string) string {
	return fmt.Sprintf("%s:lock:%s", x.options.KeyPrefix, lockId)
}

// encodeValue 将 version 和 json 编码为 memcache value
func encodeValue(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + valueSeparator + jsonStr)
}

// decodeValue 从 memcache value 解码 version 和 json
func decodeValue(data []byte) (storage.Version, string, error) {
	parts := strings.SplitN(string(data), valueSeparator, 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid memcache value format")
	}
	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return storage.Version(version), parts[1], nil
}

// CreateWithVersion 原子创建锁记录
// Memcache Add 命令：key 不存在时才写入，否则返回 ErrNotStored
func (x *MemcacheStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)
	value := encodeValue(version, lockInformation.ToJsonString())

	err := x.client.Add(&memcache.Item{
		Key:   key,
		Value: value,
	})
	if err != nil {
		if err == memcache.ErrNotStored {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 记录已创建的锁 ID（用于 List 操作）
	x.knownLockIdsMu.Lock()
	x.knownLockIds[lockId] = struct{}{}
	x.knownLockIdsMu.Unlock()

	return nil
}

// UpdateWithVersion 原子更新锁记录
// Memcache CAS 流程：Get 读 CasID → 检查 version → CompareAndSwap 写入
// CompareAndSwap 保证：如果 Get 后无人修改，CasID 匹配则写入成功
func (x *MemcacheStorage) UpdateWithVersion(ctx context.Context, lockId string, exceptedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)

	// 获取当前值和 CasID
	item, err := x.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return storage_lock.ErrLockNotFound
		}
		return err
	}

	// 解析当前版本号
	currentVersion, _, err := decodeValue(item.Value)
	if err != nil {
		return err
	}

	// 检查版本是否匹配
	if currentVersion != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// CAS 写入新值
	newValue := encodeValue(newVersion, lockInformation.ToJsonString())
	item.Value = newValue
	err = x.client.CompareAndSwap(item)
	if err != nil {
		if err == memcache.ErrCASConflict {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion 原子删除锁记录
// Memcache Delete 不支持条件删除，因此先用 CAS 写入删除标记确保版本匹配，
// 然后 Delete key。CAS 保证只有版本匹配的持有者能触发删除。
func (x *MemcacheStorage) DeleteWithVersion(ctx context.Context, lockId string, exceptedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.lockKey(lockId)

	// 获取当前值和 CasID
	item, err := x.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 解析当前版本号
	currentVersion, _, err := decodeValue(item.Value)
	if err != nil {
		return err
	}

	// 检查版本是否匹配
	if currentVersion != exceptedVersion {
		return storage_lock.ErrVersionMiss
	}

	// CAS 写入空值（删除标记），确保版本匹配后才删除
	item.Value = []byte("")
	err = x.client.CompareAndSwap(item)
	if err != nil {
		if err == memcache.ErrCASConflict {
			return storage_lock.ErrVersionMiss
		}
		return err
	}

	// 删除 key（CAS 已保证只有持有者能走到这里）
	_ = x.client.Delete(key)

	// 从已知锁 ID 集合中移除
	x.knownLockIdsMu.Lock()
	delete(x.knownLockIds, lockId)
	x.knownLockIdsMu.Unlock()

	return nil
}

// Get 获取锁信息
func (x *MemcacheStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.lockKey(lockId)
	item, err := x.client.Get(key)
	if err != nil {
		if err == memcache.ErrCacheMiss {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}

	// 空值表示已删除
	if len(item.Value) == 0 {
		return "", storage_lock.ErrLockNotFound
	}

	_, jsonStr, err := decodeValue(item.Value)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

// GetTime 获取时间
// Memcache 没有时间 API，使用 NTP 作为可靠时间源
func (x *MemcacheStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

// Close 关闭存储引擎
// 连接由 ConnectionManager 管理，Storage 只清空引用
func (x *MemcacheStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

// List 列出当前所有的锁信息
// Memcache 不支持 key 列表/扫描操作，因此通过进程内 knownLockIds 集合枚举
// 注意：这是进程内状态，多进程部署时不一致，但 List 仅用于运维查看，不参与互斥性
func (x *MemcacheStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	x.knownLockIdsMu.RLock()
	ids := make([]string, 0, len(x.knownLockIds))
	for id := range x.knownLockIds {
		ids = append(ids, id)
	}
	x.knownLockIdsMu.RUnlock()

	slice := make([]*storage.LockInformation, 0, len(ids))
	for _, lockId := range ids {
		key := x.lockKey(lockId)
		item, err := x.client.Get(key)
		if err != nil {
			if err == memcache.ErrCacheMiss {
				// key 已不存在（可能被其他进程删除），清理本地记录
				x.knownLockIdsMu.Lock()
				delete(x.knownLockIds, lockId)
				x.knownLockIdsMu.Unlock()
				continue
			}
			return nil, err
		}
		if len(item.Value) == 0 {
			// 空值表示已删除
			x.knownLockIdsMu.Lock()
			delete(x.knownLockIds, lockId)
			x.knownLockIdsMu.Unlock()
			continue
		}
		_, jsonStr, err := decodeValue(item.Value)
		if err != nil {
			continue
		}
		lockInfo := &storage.LockInformation{}
		err = json.Unmarshal([]byte(jsonStr), lockInfo)
		if err != nil {
			continue
		}
		slice = append(slice, lockInfo)
	}
	return go_iterator.FromSlice(slice), nil
}

// ------------------------------------------------ ---------------------------------------------------------------------
