package cacheman

import (
	"errors"
	"log"
	"sync"
	"time"
)

type (
	RebuildActionFunc func(key interface{}) (data interface{}, ttl time.Duration, err error)

	Backend interface {
		Store(key, value interface{})
		Load(key interface{}) (value interface{}, ok bool)
		Delete(key interface{})
	}

	DeadlineBackend interface {
		Backend
		StoreUntil(key, value interface{}, deadline time.Time)
	}

	TTLBackend interface {
		Backend
		StoreFor(key, value interface{}, ttl time.Duration)
	}
)

type Config struct {
	// RebuildAction [required]
	//
	// This func will be called when a running manager for a key is unable to load a value from the configured backend
	RebuildAction RebuildActionFunc

	// Backend [optional]
	//
	// This is used to actually store the data.  If one is not provided, a new sync.Map instance is used.
	Backend Backend
}

func buildConfig(inc *Config, mu ...func(*Config)) *Config {
	actual := new(Config)

	if inc == nil {
		inc = new(Config)
	}

	for _, fn := range mu {
		fn(inc)
	}

	actual.RebuildAction = inc.RebuildAction
	actual.Backend = inc.Backend

	return actual
}

type CacheMan struct {
	mu sync.RWMutex
	be Backend

	rebuildAction RebuildActionFunc
}

func New(c *Config, mutators ...func(*Config)) (*CacheMan, error) {

	config := buildConfig(c, mutators...)

	if config.RebuildAction == nil {
		return nil, errors.New("RebuildAction cannot be nil")
	}

	cm := new(CacheMan)

	cm.rebuildAction = config.RebuildAction

	if config.Backend == nil {
		cm.be = new(sync.Map)
	} else {
		cm.be = config.Backend
	}

	return cm, nil
}

// Load will fetch the requested key from the cache, calling the refresh func for this cache if the key is not found
func (cm *CacheMan) Load(key interface{}) (interface{}, error) {
	cm.mu.RLock()
	if v, ok := cm.be.Load(key); ok {
		log.Printf("key %v found", key)
		cm.mu.RUnlock()
		return v, nil
	}
	cm.mu.RUnlock()
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.storeAndLoad(key)
}

func (cm *CacheMan) storeAndLoad(key interface{}) (interface{}, error) {
	var (
		v   interface{}
		ok  bool
		ttl time.Duration
		err error
	)

	// test for another routine getting here before i did
	if v, ok = cm.be.Load(key); ok {
		return v, nil
	}

	// execute rebuild
	if v, ttl, err = cm.rebuildAction(key); err != nil {
		return nil, err
	}

	if dbe, ok := cm.be.(DeadlineBackend); ok {
		dbe.StoreUntil(key, v, time.Now().Add(ttl))
	} else if tbe, ok := cm.be.(TTLBackend); ok {
		tbe.StoreFor(key, v, ttl)
	} else {
		cm.be.Store(key, v)
	}

	return v, nil
}
