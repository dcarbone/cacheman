package cacheman_test

import (
	"log"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dcarbone/go-cacheman"
)

type expirableMap struct {
	*sync.Map
}

func newExpirableMap() *expirableMap {
	m := new(expirableMap)
	m.Map = new(sync.Map)
	return m
}

func (m *expirableMap) StoreFor(key, value interface{}, ttl time.Duration) {
	m.Store(key, value)
	go func() {
		<-time.After(ttl)
		m.Delete(key)
	}()
}

func (m *expirableMap) StoreUntil(key, value interface{}, deadline time.Time) {
	m.Store(key, value)
	go func() {
		<-time.After(deadline.Sub(time.Now()))
		m.Delete(key)
	}()
}

func basicRebuildAction(ttl time.Duration) cacheman.RebuildActionFunc {
	var atomicVal uint64
	return func(_ interface{}) (interface{}, time.Duration, error) {
		return atomic.AddUint64(&atomicVal, 1), ttl, nil
	}
}

func defaultConfig() *cacheman.Config {
	return &cacheman.Config{
		RebuildAction:   basicRebuildAction(0),
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

func testEquals(t *testing.T, m *cacheman.CacheMan, key, expected interface{}) {
	if v, err := m.Get(key); err != nil {
		t.Logf("Error getting key \"%v\": %v", key, err)
		t.Fail()
	} else if reflect.TypeOf(v) != reflect.TypeOf(expected) {
		t.Logf("Key \"%v\" value type mismatch: expected=%T; actual=%T", key, expected, v)
		t.Fail()
	} else if expected != v {
		t.Logf("Key \"%v\" value mismatch: expected=%v; acutal=%v", key, expected, v)
		t.Fail()
	}
}

func TestCacheMan(t *testing.T) {
	t.Run("test-init-sane", func(t *testing.T) {
		t.Parallel()

		_ = basicCacheMan(t)
	})

	t.Run("get-key", func(t *testing.T) {
		t.Parallel()

		m := basicCacheMan(t)
		testEquals(t, m, "test", uint64(1))
	})

	t.Run("get-key-again", func(t *testing.T) {
		t.Parallel()

		m := basicCacheMan(t)

		testEquals(t, m, "test", uint64(1))

		if t.Failed() {
			return
		}

		testEquals(t, m, "test", uint64(1))
	})

	t.Run("manager-timeout", func(t *testing.T) {
		t.Parallel()

		m := basicCacheMan(t, func(config *cacheman.Config) {
			config.IdleTimeout = time.Second
		})

		testEquals(t, m, "test", uint64(1))
		testEquals(t, m, "test", uint64(1))

		if t.Failed() {
			return
		}

		time.Sleep(2 * time.Second)

		testEquals(t, m, "test", uint64(1))
		testEquals(t, m, "test", uint64(1))
	})

	t.Run("manager-timeout-delete", func(t *testing.T) {
		t.Parallel()

		m := basicCacheMan(t, func(config *cacheman.Config) {
			config.IdleTimeout = time.Second
			config.TimeoutBehavior = cacheman.TimeoutBehaviorDelete
		})

		testEquals(t, m, "test", uint64(1))
		testEquals(t, m, "test", uint64(1))

		if t.Failed() {
			return
		}

		time.Sleep(2 * time.Second)

		testEquals(t, m, "test", uint64(2))
		testEquals(t, m, "test", uint64(2))
	})

	t.Run("expirable-backend", func(t *testing.T) {
		t.Parallel()

		m := basicCacheMan(t, func(config *cacheman.Config) {
			config.RebuildAction = basicRebuildAction(time.Second)
			config.Backend = newExpirableMap()
		})

		testEquals(t, m, "test", uint64(1))
		testEquals(t, m, "test", uint64(1))

		time.Sleep(2 * time.Second)

		testEquals(t, m, "test", uint64(2))
		testEquals(t, m, "test", uint64(2))
	})
}
