package cacheman_test

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dcarbone/go-cacheman"
)

var (
	atomicVal uint64
)

func basicRebuildAction(_ interface{}) (interface{}, error) {
	return atomic.AddUint64(&atomicVal, 1), nil
}

func defaultConfig() *cacheman.Config {
	return &cacheman.Config{
		RebuildAction:   basicRebuildAction,
		Backend:         new(sync.Map),
		IdleTimeout:     cacheman.DefaultManagerIdleTimeout,
		TimeoutBehavior: cacheman.TimeoutBehaviorNoAction,
		Logger:          log.New(os.Stdout, "", log.LstdFlags|log.Lmsgprefix),
	}
}

func basicCacheMan(t *testing.T, mu ...func(*cacheman.Config)) *cacheman.CacheMan {
	if m, err := cacheman.New(defaultConfig(), mu...); err != nil {
		t.Fatalf("Error constructing CacheMan: %v", err)
		return nil
	} else {
		return m
	}
}

func TestCacheMan(t *testing.T) {
	t.Run("test-init-sane", func(t *testing.T) {
		_ = basicCacheMan(t)
	})

}
