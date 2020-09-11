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
	KeyManagerLogPrefixFormat = "--> cacheman-km-%v "
)

type action uint8

const (
	actionGet action = iota
	actionUnmanage
	actionCleanup
	actionKeyList
	actionFlush
	actionPurge
)

type response struct {
	data interface{}
	err  error
}

type request struct {
	action  action
	purgeFn func(key interface{}) bool
	key     interface{}
	resp    chan response
}

type RebuildActionFunc func(key interface{}) (data interface{}, ttl time.Duration, err error)
type ShutdownActionFunc func(key, currentValue interface{})

type Backend interface {
	Store(key, value interface{})
	Load(key interface{}) (value interface{}, ok bool)
	Delete(key interface{})
}

type DeadlineBackend interface {
	Backend
	StoreUntil(key, value interface{}, deadline time.Time)
}

type TTLBackend interface {
	Backend
	StoreFor(key, value interface{}, ttl time.Duration)
}

type Config struct {
	// RebuildAction [required]
	//
	// This func will be called when a running manager for a key is unable to load a value from the configured backend
	RebuildAction RebuildActionFunc

	// ShutdownAction [optional]
	//
	// This func will be called when a running manager shuts down
	ShutdownAction ShutdownActionFunc

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
	actual.ShutdownAction = inc.ShutdownAction
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

	rebuildAction  RebuildActionFunc
	shutdownAction ShutdownActionFunc

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
	cm.shutdownAction = config.ShutdownAction
	cm.ttlBehavior = config.TimeoutBehavior
	cm.requests = make(chan *request, 100)
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
	resp := make(chan response, 1)
	cm.requests <- &request{action: actionGet, key: key, resp: resp}
	r := <-resp
	close(resp)
	return r.data, r.err
}

// Unmanage
func (cm *CacheMan) Unmanage(key interface{}) {
	resp := make(chan response)
	cm.requests <- &request{action: actionUnmanage, key: key, resp: resp}
	<-resp
	close(resp)
}

func (cm *CacheMan) ActiveManagerKeys() []interface{} {
	resp := make(chan response)
	cm.requests <- &request{action: actionKeyList, resp: resp}
	r := <-resp
	close(resp)
	return r.data.([]interface{})
}

func (cm *CacheMan) Flush() {
	resp := make(chan response)
	cm.requests <- &request{action: actionFlush, resp: resp}
	<-resp
}

func (cm *CacheMan) Purge(fn func(key interface{}) bool) {
	resp := make(chan response)
	cm.requests <- &request{action: actionPurge, purgeFn: fn, resp: resp}
	<-resp
}

func (cm *CacheMan) manage(key interface{}) *keyManager {
	m := newKeyManager(cm.log, key, cm.rebuildAction, cm.shutdownAction, cm.be, cm.idleTTL, cm.ttlBehavior)
	cm.managers[key] = m
	go func() {
		<-m.Done()
		cm.requests <- &request{action: actionCleanup, key: key}
	}()
	return m
}

func (cm *CacheMan) doGet(req *request) {
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
}

func (cm *CacheMan) doUnmanage(req *request) {
	if m, ok := cm.managers[req.key]; ok && !m.Closed() {
		m.log.Printf("Received request to stop management of key \"%v\"...", req.key)
		m.Close()
	} else {
		m.log.Printf("Received request to stop management of key \"%v\", but it either isn't in the map or has been superseded", req.key)
	}
}

func (cm *CacheMan) doCleanup(req *request) {
	if m, ok := cm.managers[req.key]; ok && m.Closed() {
		cm.log.Printf("Cache key \"%v\" hasn't been hit for awhile, shutting it down...", req.key)
		delete(cm.managers, req.key)
	} else {
		cm.log.Printf("Received cleanup request for manager of \"%v\", but it either isn't in the map or has been superseded.  Moving on...", req.key)
	}
}

func (cm *CacheMan) doKeyList(req *request) {
	keys := make([]interface{}, 0)
	for k := range cm.managers {
		keys = append(keys, k)
	}
	req.resp <- response{data: keys}
}

func (cm *CacheMan) doFlush(req *request) {
	for _, m := range cm.managers {
		m.Close()
	}
	close(req.resp)
}

func (cm *CacheMan) doPurge(req *request) {
	if req.purgeFn == nil {
		cm.doFlush(req)
		return
	}
	for k, m := range cm.managers {
		go func(k interface{}, m *keyManager) {
			if req.purgeFn(k) {
				m.Close()
			}
		}(k, m)
	}
	close(req.resp)
}

func (cm *CacheMan) run() {
	const hbInterval = time.Minute

	var (
		req  *request
		tick time.Time

		heartbeat = time.NewTimer(hbInterval)
	)

	for {
		select {
		case req = <-cm.requests:
			switch req.action {
			case actionGet:
				cm.doGet(req)
			case actionUnmanage:
				cm.doUnmanage(req)
			case actionCleanup:
				cm.doCleanup(req)
			case actionKeyList:
				cm.doKeyList(req)
			case actionPurge:
				cm.doPurge(req)
			case actionFlush:
				cm.doFlush(req)
			}

		case tick = <-heartbeat.C:
			cm.log.Printf("The time is %s and I'm currently holding on to %d key managers...", tick.Format(time.Kitchen), len(cm.managers))
			heartbeat.Reset(hbInterval)
		}
	}
}
