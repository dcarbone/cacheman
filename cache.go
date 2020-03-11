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

type Config struct {
	// RebuildAction [required]
	//
	// This func will be called when a running manager for a key is unable to load a value from the configured backend
	RebuildAction RebuildActionFunc

	// Backend [optional]
	//
	// This is used to actually store the data.  If one is not provided, a new sync.Map instance is used.
	Backend Backend

	// ManagerTTL [optional]
	//
	// Amount of time a given key manager is to remain alive
	ManagerTTL time.Duration

	// Logger [optional]
	//
	// If you want logging, define this.
	Logger *log.Logger

	// Debug [optional]
	//
	// If true and if there is a logger, enables verbose logging
	Debug bool
}

type CacheMan struct {
	mu  sync.RWMutex
	log *log.Logger
	dbg bool
	act RebuildActionFunc
	req chan *request
	cln chan interface{}
	man map[interface{}]*KeyManager
	be  Backend
}

func New(config *Config) (*CacheMan, error) {
	var (
		c = new(CacheMan)
	)

	if config.RebuildAction == nil {
		return nil, errors.New("RebuildAction cannot be nil")
	}

	if config.Logger != nil {
		c.log = log.New(config.Logger.Writer(), LogPrefix, config.Logger.Flags())
	} else {
		c.log = log.New(ioutil.Discard, LogPrefix, log.Lmsgprefix|log.LstdFlags)
	}

	c.dbg = config.Debug
	c.act = config.RebuildAction
	c.req = make(chan *request, 100)
	c.cln = make(chan interface{}, 100)
	c.man = make(map[interface{}]*KeyManager)

	if config.Backend == nil {
		c.be = new(sync.Map)
	} else {
		c.be = config.Backend
	}

	go c.run()

	return c, nil
}

// Get will fetch the requested key from the cache, calling the refresh func for this cache if the key is not found
func (c *CacheMan) Get(key interface{}) (interface{}, error) {
	resp := make(chan *response, 1)
	defer close(resp)
	c.req <- &request{key: key, resp: resp}
	r := <-resp
	return r.data, r.err
}

func (c *CacheMan) manage(key interface{}) *KeyManager {
	c.man[key] = NewKeyManager(c.log, c.dbg, key, c.act, c.be, c.cln)
	return c.man[key]
}

func (c *CacheMan) logf(debug bool, f string, v ...interface{}) {
	if !debug || c.dbg {
		c.log.Printf(f, v...)
	}
}

func (c *CacheMan) run() {
	const hbInterval = time.Minute

	var (
		tick time.Time

		heartbeat = time.NewTimer(hbInterval)
	)

	for {
		select {
		case req := <-c.req:
			var (
				m  *KeyManager
				ok bool
			)

			if m, ok = c.man[req.key]; !ok {
				c.logf(true, "No manager present for key %q, creating...")
				m = c.manage(req.key)
			} else if m.Closed() {
				c.logf(true, "Manager for %q is still in map, but marked as closed.  Will replace...", req.key)
				m = c.manage(req.key)
			}

			go requestFromManager(c.logf, m, req)

		case key := <-c.cln:
			if m, ok := c.man[key]; ok && m.Closed() {
				c.logf(true, "Cache key %q hasn't been hit for awhile, shutting it down...", key)
				delete(c.man, key)
			} else {
				c.logf(true, "Received cleanup request for manager of %q, but it either isn't in the map or has been superseded.  Moving on...", key)
			}

		case tick = <-heartbeat.C:
			c.logf(true, "The time is %s and I'm currently holding on to %d key managers...", tick.Format(time.Kitchen), len(c.man))
			heartbeat.Reset(hbInterval)
		}
	}
}
