package cacheman

import (
	"errors"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

const (
	DefaultManagerIdleTimeout = time.Hour
)

var (
	LogPrefix                 = "-> cacheman "
	KeyManagerLogPrefixFormat = "--> cacheman-km-%s "
)

type response struct {
	data interface{}
	err  error
}

type request struct {
	key  interface{}
	resp chan *response
}

type RebuildActionFunc func(key interface{}) (data interface{}, err error)

type Backend interface {
	Store(key, value interface{})
	Load(key interface{}) (value interface{}, ok bool)
	Delete(key interface{})
}

type DeadlineBackend interface {
	Backend
	StoreUntil(deadline time.Time, key, value interface{})
}

type Config struct {
	// RebuildAction [required]
	//
	// This func will be called when a running manager for a key is unable to load a value from the configured backend
	RebuildAction RebuildActionFunc

	// Backend [optional]
	//
	// This is used to actually store the data.  If one is not provided, a new sync.Map instance is used.
	Backend Backend

	// IdleTimeout [optional]
	//
	// Amount of time for this manager and key to exist past last fetch request
	IdleTimeout time.Duration

	// TimeoutBehavior [optional]
	//
	// Behavior of manager timeout
	TimeoutBehavior TimeoutBehavior

	// Logger [optional]
	//
	// If you want logging, define this.
	Logger *log.Logger
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
	actual.IdleTimeout = inc.IdleTimeout
	actual.TimeoutBehavior = inc.TimeoutBehavior
	actual.Logger = inc.Logger

	return actual
}

type CacheMan struct {
	mu  sync.RWMutex
	log *log.Logger
	be  Backend

	requests chan *request
	cleanup  chan interface{}

	rebuildAction RebuildActionFunc

	managers    map[interface{}]*keyManager
	idleTTL     time.Duration
	ttlBehavior TimeoutBehavior
}

func New(c *Config, mutators ...func(*Config)) (*CacheMan, error) {

	config := buildConfig(c, mutators...)

	if config.RebuildAction == nil {
		return nil, errors.New("RebuildAction cannot be nil")
	}

	cm := new(CacheMan)

	cm.rebuildAction = config.RebuildAction
	cm.ttlBehavior = config.TimeoutBehavior
	cm.requests = make(chan *request, 100)
	cm.cleanup = make(chan interface{}, 100)
	cm.managers = make(map[interface{}]*keyManager)

	if config.Logger != nil {
		cm.log = log.New(config.Logger.Writer(), LogPrefix, config.Logger.Flags())
	} else {
		cm.log = log.New(ioutil.Discard, LogPrefix, log.Lmsgprefix|log.LstdFlags)
	}

	if config.Backend == nil {
		cm.be = new(sync.Map)
	} else {
		cm.be = config.Backend
	}

	if config.IdleTimeout > 0 {
		cm.idleTTL = config.IdleTimeout
	} else {
		cm.idleTTL = DefaultManagerIdleTimeout
	}

	go cm.run()

	return cm, nil
}

// Get will fetch the requested key from the cache, calling the refresh func for this cache if the key is not found
func (cm *CacheMan) Get(key interface{}) (interface{}, error) {
	resp := make(chan *response, 1)
	cm.requests <- &request{key: key, resp: resp}
	r := <-resp
	close(resp)
	return r.data, r.err
}

func (cm *CacheMan) Unmanage(key interface{}) {
	cm.cleanup <- key
}

func (cm *CacheMan) manage(key interface{}) *keyManager {
	m := newKeyManager(cm.log, key, cm.rebuildAction, cm.be, cm.idleTTL, cm.ttlBehavior)
	cm.managers[key] = m
	go func() {
		<-m.done
		cm.cleanup <- m.key
	}()
	return m
}

func (cm *CacheMan) run() {
	const hbInterval = time.Minute

	var (
		tick time.Time

		heartbeat = time.NewTimer(hbInterval)
	)

	for {
		select {
		case req := <-cm.requests:
			var (
				m  *keyManager
				ok bool
			)

			m, ok = cm.managers[req.key]
			if !ok || m.Closed() {
				cm.log.Printf("No active manager present for key \"%v\", creating...", req.key)
				m = cm.manage(req.key)
			}

			go requestFromManager(cm.log.Printf, m, req)

		case key := <-cm.cleanup:
			if m, ok := cm.managers[key]; ok && m.Closed() {
				cm.log.Printf("Cache key \"%v\" hasn't been hit for awhile, shutting it down...", key)
				delete(cm.managers, key)
			} else {
				cm.log.Printf("Received cleanup request for manager of \"%v\", but it either isn't in the map or has been superseded.  Moving on...", key)
			}

		case tick = <-heartbeat.C:
			cm.log.Printf("The time is %s and I'm currently holding on to %d key managers...", tick.Format(time.Kitchen), len(cm.managers))
			heartbeat.Reset(hbInterval)
		}
	}
}
