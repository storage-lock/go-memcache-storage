package memcache_storage

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/storage-lock/go-storage"
)

// MemcacheStorageOptions Memcache 的存储选项
type MemcacheStorageOptions struct {

	// 获取连接
	ConnectionManager storage.ConnectionManager[*memcache.Client]

	// KeyPrefix Memcache 中 key 的前缀，用于隔离不同的锁实例
	// 实际的 key 格式为：<KeyPrefix>:lock:<lockId>
	KeyPrefix string

	// TimeProvider 可选的时间源，默认使用 NTP
	// 在无法访问 NTP 服务器的环境（如无外网的容器）中，可注入本地时间源
	TimeProvider storage.TimeProvider
}

func NewMemcacheStorageOptions() *MemcacheStorageOptions {
	return &MemcacheStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func NewMemcacheStorageOptionsWithServers(servers []string) *MemcacheStorageOptions {
	return NewMemcacheStorageOptions().SetConnectionManager(NewMemcacheConnectionManagerFromServers(servers))
}

func (x *MemcacheStorageOptions) SetKeyPrefix(keyPrefix string) *MemcacheStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *MemcacheStorageOptions) SetConnectionManager(connectionManager storage.ConnectionManager[*memcache.Client]) *MemcacheStorageOptions {
	x.ConnectionManager = connectionManager
	return x
}

func (x *MemcacheStorageOptions) SetTimeProvider(timeProvider storage.TimeProvider) *MemcacheStorageOptions {
	x.TimeProvider = timeProvider
	return x
}

func (x *MemcacheStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}
