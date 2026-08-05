package memcache_storage

import (
	"context"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/storage-lock/go-storage"
	"sync"
)

// MemcacheConnectionManager 负责维护与 Memcache 的连接
type MemcacheConnectionManager struct {

	// Memcache 服务器地址列表
	Servers []string

	// initMu 保护下面的初始化状态。原用 sync.Once，Connect 首次失败后 err 永久缓存，
	// 后续 Take 永远返回同一 err 无法重试（漏洞4/liveness）。改为 mutex + initialized 标志：
	// 成功后缓存 client 不再重复 Connect；失败不缓存 err，下次 Take 可重试。
	initMu      sync.Mutex
	initialized bool
	client      *memcache.Client
}

var _ storage.ConnectionManager[*memcache.Client] = &MemcacheConnectionManager{}

// NewMemcacheConnectionManagerFromServers 从 Memcache 服务器地址列表创建连接管理器
func NewMemcacheConnectionManagerFromServers(servers []string) *MemcacheConnectionManager {
	return &MemcacheConnectionManager{
		Servers: servers,
	}
}

// NewMemcacheConnectionManagerFromClient 复用已经存在的 Memcache 客户端，从其创建连接管理器
func NewMemcacheConnectionManagerFromClient(client *memcache.Client) *MemcacheConnectionManager {
	return &MemcacheConnectionManager{
		client:      client,
		initialized: true, // 外部预设 client，Take 不再 Connect
	}
}

const MemcacheConnectionManagerName = "memcache-connection-manager"

func (x *MemcacheConnectionManager) Name() string {
	return MemcacheConnectionManagerName
}

// Take 获取 Memcache 客户端。
//
// 漏洞4 修复：原 sync.Once 在连接失败时永久缓存 err，无法重试。
// 改为 mutex + initialized：成功后缓存 client 不重复 Connect；失败不缓存 err 可重试。
func (x *MemcacheConnectionManager) Take(ctx context.Context) (*memcache.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	if x.initialized {
		return x.client, nil
	}
	client := memcache.New(x.Servers...)
	// 探活：确保连接可用
	if err := client.Ping(); err != nil {
		// 连接失败不缓存，下次 Take 可重试
		return nil, err
	}
	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *MemcacheConnectionManager) Return(ctx context.Context, connection *memcache.Client) error {
	return nil
}

func (x *MemcacheConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	client := x.client
	x.initMu.Unlock()
	if client != nil {
		return client.Close()
	}
	return nil
}
