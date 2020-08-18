package cacheman

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

type TimeoutBehavior uint8

const (
	TimeoutBehaviorNoAction TimeoutBehavior = iota // no action is taken upon timeout
	TimeoutBehaviorDelete                          // the key being managed will be deleted upon timeout
)

func (t TimeoutBehavior) String() string {
	switch t {
	case TimeoutBehaviorNoAction:
		return "no-action"
	case TimeoutBehaviorDelete:
		return "delete"

	default:
		panic(fmt.Sprintf("Unknown key manager timeout behavior specified: %d", t))
	}
}

var (
	ErrKeyMangerClosed = errors.New("key manager has been shutdown")
)

func IsKeyManagerClosedError(err error) bool {
	for err != nil {
		if errors.Is(err, ErrKeyMangerClosed) {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

type keyManager struct {
	// this mutex is currently only used to lock the "closed" state
	mu  sync.Mutex
	log *log.Logger
	be  Backend

	key interface{}

	rebuildAction  RebuildActionFunc
	shutdownAction ShutdownActionFunc

	requests     chan *request
	accessCount  uint64
	lastAccessed time.Time
	rebuildCount uint64
	lastRebuilt  time.Time

	idleTTL     time.Duration
	ttlBehavior TimeoutBehavior
	close       chan struct{}
	closed      bool

	done chan struct{}
}

func newKeyManager(l *log.Logger, key interface{}, ract RebuildActionFunc, sact ShutdownActionFunc, be Backend, idleTTL time.Duration, ttlBehavior TimeoutBehavior) *keyManager {
	km := new(keyManager)

	km.key = key
	km.rebuildAction = ract
	km.shutdownAction = sact
	km.be = be
	km.idleTTL = idleTTL
	km.ttlBehavior = ttlBehavior
	km.requests = make(chan *request, 100)
	km.close = make(chan struct{}, 2) // needs to be buffered to prevent lock contention
	km.done = make(chan struct{})

	if l == nil {
		km.log = log.New(ioutil.Discard, "", 0)
	} else {
		km.log = log.New(l.Writer(), fmt.Sprintf(KeyManagerLogPrefixFormat, key), l.Flags())
	}

	go km.manage()

	return km
}

func (km *keyManager) Done() <-chan struct{} {
	km.mu.Lock()
	var d chan struct{}
	if km.done == nil {
		d = make(chan struct{})
		close(d)
	} else {
		d = km.done
	}
	km.mu.Unlock()
	return d
}

func (km *keyManager) Request(req *request) {
	km.mu.Lock()
	if km.closed {
		go respondToRequest(km.log.Printf, req, nil, ErrKeyMangerClosed)
	} else {
		km.requests <- req
	}
	km.mu.Unlock()
}

func (km *keyManager) Closed() bool {
	km.mu.Lock()
	defer km.mu.Unlock()
	return km.closed
}

func (km *keyManager) Close() {
	km.mu.Lock()
	if km.closed {
		km.mu.Unlock()
		return
	}
	km.closed = true
	km.close <- struct{}{}
	km.mu.Unlock()
}

func (km *keyManager) logf(f string, v ...interface{}) {
	km.log.Printf(f, v...)
}

func (km *keyManager) respond(req *request) {
	km.mu.Lock()
	defer km.mu.Unlock()

	var (
		v   interface{}
		ttl time.Duration
		ok  bool
		err error
	)

	km.accessCount++
	km.lastAccessed = time.Now()

	if km.closed {
		km.log.Printf("I am closed, will not rebuild.")
		err = ErrKeyMangerClosed
	} else if v, ok = km.be.Load(req.key); !ok {
		km.rebuildCount++
		km.lastRebuilt = time.Now()
		if v, ttl, err = km.rebuildAction(req.key); err == nil {
			if ttl > 0 {
				if tb, ok := km.be.(TTLBackend); ok {
					tb.StoreFor(req.key, v, ttl)
				} else if db, ok := km.be.(DeadlineBackend); ok {
					db.StoreUntil(req.key, v, time.Now().Add(ttl))
				} else {
					km.be.Store(req.key, v)
				}
			} else {
				km.be.Store(req.key, v)
			}
		}
	}

	go respondToRequest(km.log.Printf, req, v, err)
}

func (km *keyManager) shutdown() {
	km.mu.Lock()
	defer km.mu.Unlock()

	var (
		ml   = len(km.requests)
		v, _ = km.be.Load(km.key)
	)

	km.closed = true
	close(km.requests)

	km.log.Printf(
		"Shutting down. Final stats: unhandledRequests=%d; accessCount=%d; rebuildCount=%d; lastAccessed=%s; lastRebuilt=%s",
		ml,
		km.accessCount,
		km.rebuildCount,
		km.lastAccessed.Format(time.Stamp),
		km.lastRebuilt.Format(time.Stamp),
	)

	if ml > 0 {
		for req := range km.requests {
			go respondToRequest(km.log.Printf, req, v, ErrKeyMangerClosed)
		}
	}

	// delete value in preparation for next manager to come online and refresh it.
	if km.ttlBehavior == TimeoutBehaviorDelete {
		km.log.Printf("Timeout behavior is %q, deleting key...", km.ttlBehavior)
		km.be.Delete(km.key)
	}

	if km.shutdownAction != nil {
		go km.shutdownAction(km.key, v)
	}

	close(km.close)
	if len(km.close) > 0 {
		for range km.close {
		}
	}
	km.close = nil

	close(km.done)
	km.done = nil
}

func (km *keyManager) manage() {
	const (
		hbInterval = 5 * time.Minute
	)

	var (
		heartbeat = time.NewTimer(5 * time.Minute)
		lifespan  = time.NewTimer(km.idleTTL)
	)

	km.log.Printf("Cache key manager for %v coming online.", km.key)

	defer func() {
		if v := recover(); v != nil {
			km.log.Printf("Panic during key manager lifecycle: %v", v)
		}

		km.shutdown()

		if !lifespan.Stop() && len(lifespan.C) > 0 {
			<-lifespan.C
		}
		if !heartbeat.Stop() && len(heartbeat.C) > 0 {
			<-heartbeat.C
		}
	}()

	for {
		select {
		case req := <-km.requests:
			km.respond(req)

			if !lifespan.Stop() && len(lifespan.C) > 0 {
				<-lifespan.C
			}

			lifespan.Reset(km.idleTTL)

		case <-heartbeat.C:
			km.log.Printf(
				"Heartbeat stats: accessCount=%d; rebuildCount=%d; lastAccessed=%s; lastRebuilt=%s",
				km.accessCount,
				km.rebuildCount,
				km.lastAccessed.Format(time.Stamp),
				km.lastRebuilt.Format(time.Stamp),
			)

			heartbeat.Reset(hbInterval)

		case <-lifespan.C:
			km.log.Printf("I have not received a request in %q.  Shutting down...", km.idleTTL)
			km.Close()

		case <-km.close:
			km.log.Printf("The time is %q. I've been told to close.  Shutting down...", time.Now().Format(time.Stamp))
			return
		}
	}
}

func respondToRequest(logf func(string, ...interface{}), req *request, data interface{}, err error) {
	logf("Responding to request for key \"%v\" with: dataType=%T; err=%v", req.key, data, err)
	req.resp <- &response{data: data, err: err}
}

func requestFromManager(logf func(string, ...interface{}), m *keyManager, req *request) {
	logf("Handing request for key \"%v\" off to its manager", req.key)
	m.Request(req)
}
