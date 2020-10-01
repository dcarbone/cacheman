package cacheman_test

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dcarbone/go-cacheman/v2"
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

func basicRebuildAction(t *testing.T, ttl time.Duration) cacheman.RebuildActionFunc {
	type store struct {
		v uint64
	}
	st := new(store)
	return func(_ interface{}) (interface{}, time.Duration, error) {
		t.Logf("rebuilding, current=%d", atomic.LoadUint64(&st.v))
		return atomic.AddUint64(&st.v, 1), ttl, nil
	}
}

func defaultConfig(t *testing.T) *cacheman.Config {
	return &cacheman.Config{
		RebuildAction: basicRebuildAction(t, 0),
		Backend:       new(sync.Map),
	}
}

func basicCacheMan(t *testing.T, mu ...func(*cacheman.Config)) *cacheman.CacheMan {
	if m, err := cacheman.New(defaultConfig(t), mu...); err != nil {
		t.Fatalf("Error constructing CacheMan: %v", err)
		return nil
	} else {
		return m
	}
}

func testEquals(t *testing.T, m *cacheman.CacheMan, key, expected interface{}) <-chan struct{} {
	done := make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if v, err := m.Load(key); err != nil {
			t.Logf("Error getting key \"%v\": %v", key, err)
			t.Fail()
		} else if reflect.TypeOf(v) != reflect.TypeOf(expected) {
			t.Logf("Key \"%v\" value type mismatch: expected=%T; actual=%T", key, expected, v)
			t.Fail()
		} else if expected != v {
			t.Logf("Key \"%v\" value mismatch: expected=%v; acutal=%v", key, expected, v)
			t.Fail()
		}
	}()
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
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

	t.Run("expirable-backend", func(t *testing.T) {
		t.Parallel()

		m := basicCacheMan(t, func(config *cacheman.Config) {
			config.RebuildAction = basicRebuildAction(t, time.Second)
			config.Backend = newExpirableMap()
		})

		testEquals(t, m, "test", uint64(1))
		testEquals(t, m, "test", uint64(1))

		time.Sleep(2 * time.Second)

		testEquals(t, m, "test", uint64(2))
		testEquals(t, m, "test", uint64(2))
	})

	t.Run("concurrent-access", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if f, err := os.Create(fmt.Sprintf("./profile/mem_%s.prof", time.Now().Format(time.RFC3339))); err == nil {
				defer func() { _ = f.Close() }()
				runtime.GC()
				_ = pprof.WriteHeapProfile(f)
			}
		}()

		m := basicCacheMan(t)

		stop := time.NewTimer(20 * time.Second)
		for i := 0; i < 1000; i++ {
			go func() {
				for {
					select {
					case <-stop.C:
						return
					case <-time.After(time.Second):
						t.Log("load test took longer than 1 second to complete")
						t.Fail()
					case <-testEquals(t, m, "test", uint64(1)):
					}
				}
			}()
		}
		<-stop.C
	})
}
