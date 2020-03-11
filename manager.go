package cacheman

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

var (
	ErrKeyMangerShutdowned = errors.New("key manager has been shutdown")
)

func IsManagerShutdownedError(err error) bool {
	for err != nil {
		if errors.Is(err, ErrKeyMangerShutdowned) {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

type KeyManagerConfig struct {
	// Key [required]
	//
	// Key this manager is responsible for managing
	Key interface{}

	// RebuildAction [required]
	//
	// This func will be called when a running manager for a key is unable to load a value from the configured backend
	RebuildAction RebuildActionFunc

	// Backend [required]
	//
	// Backend this manager is managing a key within
	Backend Backend

	// IdleTimeout [optional]
	//
	// Amount of time for this manager and key to exist past last fetch request
	IdleTimeout time.Duration

	// Logger [optional]
	//
	// If you want logging, define this.
	Logger *log.Logger
}

type KeyManager struct {
	mu sync.RWMutex

	log *log.Logger

	key interface{}
	act RebuildActionFunc
	be  Backend
	req chan *request

	accessCount  uint64
	lastAccessed time.Time

	idleTimeout time.Duration
	close       chan struct{}
	closed      bool

	done chan struct{}
}

func NewKeyManager(config *KeyManagerConfig) (*KeyManager, error) {
	if config.Key == nil {
		return nil, errors.New("key cannot be empty")
	}
	if config.RebuildAction == nil {
		return nil, errors.New("rebuild action cannot be empty")
	}
	if config.Backend == nil {
		return nil, errors.New("backend cannot be empty")
	}

	m := new(KeyManager)

	m.key = config.Key
	m.act = config.RebuildAction
	m.be = config.Backend
	m.req = make(chan *request, 100)
	m.close = make(chan struct{})
	m.done = make(chan struct{})

	if config.Logger == nil {
		m.log = log.New(ioutil.Discard, "", 0)
	} else {
		log.New(config.Logger.Writer(), fmt.Sprintf(KeyManagerLogPrefixFormat, config.Key), config.Logger.Flags())
	}

	if config.IdleTimeout > 0 {
		m.idleTimeout = config.IdleTimeout
	} else {
		m.idleTimeout = DefaultManagerIdleTimeout
	}

	go m.manage()

	return m, nil
}

func (m *KeyManager) Request(req *request) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		go respondToRequest(m.log.Printf, req, nil, ErrKeyMangerShutdowned)
		return
	}
	m.req <- req
}

func (m *KeyManager) AccessCount() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accessCount
}

func (m *KeyManager) LastAccessed() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastAccessed.UnixNano()
}

func (m *KeyManager) Closed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

func (m *KeyManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.mu.Unlock()

	close(m.close)
	<-m.done
}

func (m *KeyManager) logf(f string, v ...interface{}) {
	m.log.Printf(f, v...)
}

func (m *KeyManager) manage() {
	const (
		hbInterval = 5 * time.Minute
	)

	var (
		heartbeat = time.NewTimer(5 * time.Minute)
		lifespan  = time.NewTimer(m.idleTimeout)
	)

	m.log.Printf("Cache key manager for %v coming online.", m.key)

	defer func() {
		close(m.req)

		if len(m.req) > 0 {
			for req := range m.req {
				go respondToRequest(m.log.Printf, req, nil, ErrKeyMangerShutdowned)
			}
		}

		close(m.done)
	}()

	for {
		select {
		case req := <-m.req:
			var (
				v   interface{}
				ok  bool
				err error
			)

			m.mu.RLock()

			if v, ok = m.be.Load(req.key); !ok {
				if v, err = m.act(req.key); err == nil {
					m.be.Store(req.key, v)
				}
			}

			m.accessCount++
			m.lastAccessed = time.Now()

			m.mu.RUnlock()

			go respondToRequest(m.log.Printf, req, v, err)

			if !lifespan.Stop() && len(lifespan.C) > 0 {
				<-lifespan.C
			}

			lifespan.Reset(m.idleTimeout)

		case tick := <-heartbeat.C:
			m.log.Printf(
				"The time is %q.  I have served %d requests was last accessed at %q",
				tick.Format(time.Stamp),
				m.accessCount,
				m.lastAccessed.Format(time.Stamp),
			)

			heartbeat.Reset(hbInterval)

		case tick := <-lifespan.C:
			m.mu.Lock()
			m.closed = true
			m.mu.Unlock()
			m.be.Delete(m.key)
			m.log.Printf(
				"The time is %q.  I have not received a request in %q.  My last request was %q.  Shutting down...",
				tick.Format(time.Stamp),
				m.lastAccessed.Format(time.Stamp),
				m.idleTimeout,
			)
			return
		}
	}
}

func respondToRequest(logf func(string, ...interface{}), req *request, data interface{}, err error) {
	logf("Responding to request for key \"%v\" with: data=%T; err=%v", req.key, data, err)
	req.resp <- &response{data: data, err: err}
}

func requestFromManager(logf func(string, ...interface{}), m *KeyManager, req *request) {
	logf("Handing request for key \"%v\" off to its manager", req.key)
	m.Request(req)
}
