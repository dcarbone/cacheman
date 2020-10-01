package cacheman

import (
	"errors"
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

	storeActionFunc func(key interface{}, rebuildAction RebuildActionFunc, be Backend) (interface{}, error)
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
	mu sync.Mutex
	be Backend

	rebuildAction RebuildActionFunc
	storeAction   storeActionFunc
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

	if _, ok := cm.be.(DeadlineBackend); ok {
		cm.storeAction = storeAndLoadDeadline
	} else if _, ok := cm.be.(TTLBackend); ok {
		cm.storeAction = storeAndLoadTTL
	} else {
		cm.storeAction = storeAndLoad
	}

	return cm, nil
}

// Load will fetch the requested key from the cache, calling the refresh func for this cache if the key is not found
func (cm *CacheMan) Load(key interface{}) (interface{}, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	var (
		v   interface{}
		ok  bool
		err error
	)
	v, ok = cm.be.Load(key)
	if !ok {
		v, err = cm.storeAction(key, cm.rebuildAction, cm.be)
	}

	return v, err
}

func storeAndLoad(key interface{}, rebuildAction RebuildActionFunc, be Backend) (interface{}, error) {
	var (
		v   interface{}
		err error
	)
	if v, _, err = rebuildAction(key); err != nil {
		return nil, err
	}
	be.Store(key, v)
	return v, nil
}

func storeAndLoadDeadline(key interface{}, rebuildAction RebuildActionFunc, be Backend) (interface{}, error) {
	var (
		v   interface{}
		ttl time.Duration
		err error
	)
	if v, ttl, err = rebuildAction(key); err != nil {
		return nil, err
	}
	be.(DeadlineBackend).StoreUntil(key, v, time.Now().Add(ttl))
	return v, nil
}

func storeAndLoadTTL(key interface{}, rebuildAction RebuildActionFunc, be Backend) (interface{}, error) {
	var (
		v   interface{}
		ttl time.Duration
		err error
	)
	if v, ttl, err = rebuildAction(key); err != nil {
		return nil, err
	}
	be.(TTLBackend).StoreFor(key, v, ttl)
	return v, nil
}
