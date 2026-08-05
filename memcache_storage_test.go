package memcache_storage

import (
	"context"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"testing"
	"time"
)

// localTimeProvider 本地时间源，用于无 NTP 访问的测试环境
type localTimeProvider struct{}

func (p *localTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

var _ storage.TimeProvider = &localTimeProvider{}

var memcachedCmd *exec.Cmd

func TestMain(m *testing.M) {
	envName := "STORAGE_LOCK_MEMCACHE_SERVERS"
	servers := os.Getenv(envName)
	if servers != "" {
		os.Exit(m.Run())
	}

	// 尝试启动临时 memcached
	memcachePort := "11311"
	memcachedCmd = exec.Command("memcached",
		"-p", memcachePort,
		"-d",
	)
	if err := memcachedCmd.Run(); err != nil {
		// 无法启动 memcached，跳过测试
		os.Exit(0)
	}

	// 等待 memcached 就绪
	time.Sleep(time.Millisecond * 500)
	os.Setenv(envName, "localhost:"+memcachePort)
	exitCode := m.Run()

	os.Exit(exitCode)
}

func TestMemcacheStorage(t *testing.T) {
	envName := "STORAGE_LOCK_MEMCACHE_SERVERS"
	servers := os.Getenv(envName)
	if servers == "" {
		t.Skip("STORAGE_LOCK_MEMCACHE_SERVERS not set and memcached not available")
	}

	connectionManager := NewMemcacheConnectionManagerFromServers([]string{servers})
	s, err := NewMemcacheStorage(context.Background(), &MemcacheStorageOptions{
		ConnectionManager: connectionManager,
		KeyPrefix:         storage_test_helper.TestTableName,
		TimeProvider:      &localTimeProvider{},
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
