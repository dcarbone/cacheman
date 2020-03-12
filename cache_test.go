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

	t.Run("get-key", func(t *testing.T) {
		m := basicCacheMan(t)

		v, err := m.Get("test")
		if err != nil {
			t.Logf("Error getting initial key: %v", err)
			t.Fail()
		} else if i, ok := v.(uint64); !ok {
			t.Logf("Expected v to be uint64, saw %T", v)
			t.Fail()
		} else if i != 1 {
			t.Logf("Expected i to be 1, saw %d", i)
			t.Fail()
		}
	})
}
