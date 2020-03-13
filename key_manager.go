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

	rebuildAction RebuildActionFunc

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

func newKeyManager(l *log.Logger, key interface{}, act RebuildActionFunc, be Backend, idleTTL time.Duration, ttlBehavior TimeoutBehavior) *keyManager {
	km := new(keyManager)

	km.key = key
	km.rebuildAction = act
	km.be = be
	km.idleTTL = idleTTL
	km.ttlBehavior = ttlBehavior
	km.requests = make(chan *request, 100)
	km.close = make(chan struct{})
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
	if km.done == nil {
		km.done = make(chan struct{})
	}
	d := km.done
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
	c := km.closed
	km.mu.Unlock()
	return c
}

func (km *keyManager) Close() {
	km.mu.Lock()
	if km.closed {
		km.mu.Unlock()
		return
	}
	km.closed = true
	km.mu.Unlock()

	close(km.close)
	<-km.done
}

func (km *keyManager) logf(f string, v ...interface{}) {
	km.log.Printf(f, v...)
}

func (km *keyManager) shutdown() {
	close(km.requests)

	ml := len(km.requests)

	km.log.Printf(
		"Shutting down. Final stats: unhandledRequests=%d; accessCount=%d; rebuildCount=%d; lastAccessed=%s; lastRebuilt=%s",
		ml,
		km.accessCount,
		km.rebuildCount,
		km.lastAccessed.Format(time.Stamp),
		km.lastRebuilt.Format(time.Stamp),
	)

	if ml > 0 {
		// try to load current value to send to requests
		v, _ := km.be.Load(km.key)
		for req := range km.requests {
			go respondToRequest(km.log.Printf, req, v, ErrKeyMangerClosed)
		}
	}

	// delete value in preparation for next manager to come online and refresh it.
	if km.ttlBehavior == TimeoutBehaviorDelete {
		km.log.Printf("Timeout behavior is %q, deleting key...", km.ttlBehavior)
		km.be.Delete(km.key)
	}

	close(km.done)
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

	for {
		select {
		case req := <-km.requests:
			var (
				v   interface{}
				ttl time.Duration
				ok  bool
				err error
			)

			km.mu.Lock()

			km.accessCount++
			km.lastAccessed = time.Now()

			if v, ok = km.be.Load(req.key); !ok {
				if km.closed {
					km.log.Printf("Request received after being closed")
					err = ErrKeyMangerClosed
				} else {
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
			}

			km.mu.Unlock()

			go respondToRequest(km.log.Printf, req, v, err)

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
			km.mu.Lock()
			km.closed = true
			km.mu.Unlock()

			km.log.Printf("I have not received a request in %q.  Shutting down...", km.idleTTL)

			if !heartbeat.Stop() && len(heartbeat.C) > 0 {
				<-heartbeat.C
			}

			km.shutdown()
			return

		case <-km.close:
			km.log.Printf("The time is %q. I've been told to close.  Shutting down...", time.Now().Format(time.Stamp))

			if !lifespan.Stop() && len(lifespan.C) > 0 {
				<-lifespan.C
			}
			if !heartbeat.Stop() && len(heartbeat.C) > 0 {
				<-heartbeat.C
			}

			km.shutdown()
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
