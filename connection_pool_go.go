// Package connection_pool_go implements a bounded pool of reusable
// connections with pluggable health checks and idle timeout eviction.
//
// The zero value is not usable; construct a pool with New.
package connection_pool_go

import (
	"errors"
	"sync"
	"time"
)

// ErrPoolClosed is returned by Acquire and Release after Close has been called.
var ErrPoolClosed = errors.New("connection pool: closed")

// ErrPoolExhausted is returned by Acquire when the pool is full and no
// connection becomes available before the context deadline.
var ErrPoolExhausted = errors.New("connection pool: exhausted")

// ErrBadConnection is returned by Acquire when a connection fails its health
// check and could not be replaced.
var ErrBadConnection = errors.New("connection pool: connection failed health check")

// Conn is a connection managed by the pool.
//
// The pool never inspects the concrete type; it only calls Close and, if
// configured, Ping. Implementations must be safe for concurrent use.
type Conn interface {
	// Close releases the underlying resource. It is called exactly once per
	// connection, either by the pool during eviction or by the caller after a
	// successful Acquire.
	Close() error
}

// Pinger is an optional interface a Conn may implement. If a Conn implements
// it, the pool calls Ping before handing the connection to a caller and
// discards the connection if Ping returns an error.
type Pinger interface {
	Ping() error
}

// Factory creates a new connection. It is called by the pool when it needs a
// connection and none are idle.
type Factory func() (Conn, error)

// Options configures a Pool. All fields are optional except Factory.
type Options struct {
	// Factory creates new connections. Required.
	Factory Factory

	// MaxSize is the maximum number of connections the pool will create.
	// If zero, a default of 1 is used.
	MaxSize int

	// MaxIdle is the maximum number of idle connections kept for reuse.
	// If zero, MaxSize is used.
	MaxIdle int

	// IdleTimeout is how long an idle connection may sit before the pool
	// closes it. If zero, connections are never evicted for idleness.
	IdleTimeout time.Duration

	// HealthCheck, if non-nil, is called before an idle connection is handed
	// to a caller. Returning false causes the connection to be closed and a
	// new one to be created.
	HealthCheck func(Conn) bool

	// Clock returns the current time. If nil, time.Now is used. Tests can
	// supply a fake clock to make idle eviction deterministic.
	Clock func() time.Time

	// OnClose, if non-nil, is called after a connection is closed by the pool.
	OnClose func(Conn)
}

// Pool is a bounded pool of reusable connections. It is safe for concurrent
// use by multiple goroutines.
type Pool struct {
	mu          sync.Mutex
	cond        *sync.Cond
	factory     Factory
	maxSize     int
	maxIdle     int
	idleTimeout time.Duration
	healthCheck func(Conn) bool
	clock       func() time.Time
	onClose     func(Conn)

	// idle holds connections that are not currently checked out, ordered by
	// last-used time (oldest first).
	idle []*idleConn

	// numOpen is the number of connections the pool has created that have not
	// been closed. It includes both idle and checked-out connections.
	numOpen int

	closed bool
}

type idleConn struct {
	conn     Conn
	lastUsed time.Time
}

// New creates a Pool with the given options. It returns an error if Factory
// is nil or if MaxSize/MaxIdle are negative.
func New(opts Options) (*Pool, error) {
	if opts.Factory == nil {
		return nil, errors.New("connection pool: Factory is required")
	}
	if opts.MaxSize < 0 {
		return nil, errors.New("connection pool: MaxSize must not be negative")
	}
	if opts.MaxIdle < 0 {
		return nil, errors.New("connection pool: MaxIdle must not be negative")
	}

	maxSize := opts.MaxSize
	if maxSize == 0 {
		maxSize = 1
	}
	maxIdle := opts.MaxIdle
	if maxIdle == 0 {
		maxIdle = maxSize
	}
	if maxIdle > maxSize {
		maxIdle = maxSize
	}

	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}

	p := &Pool{
		factory:     opts.Factory,
		maxSize:     maxSize,
		maxIdle:     maxIdle,
		idleTimeout: opts.IdleTimeout,
		healthCheck: opts.HealthCheck,
		clock:       clock,
		onClose:     opts.OnClose,
	}
	p.cond = sync.NewCond(&p.mu)
	return p, nil
}

// Acquire returns a connection from the pool, creating one if necessary. It
// blocks until a connection is available, the pool is closed, or the pool is
// exhausted and no connection can be created.
//
// If the pool is at MaxSize and all connections are checked out, Acquire
// returns ErrPoolExhausted rather than blocking forever. This is the chosen
// trade-off: a bounded pool that never blocks indefinitely is easier to reason
// about in production than one that can deadlock.
func (p *Pool) Acquire() (Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		if p.closed {
			return nil, ErrPoolClosed
		}

		// Try to reuse an idle connection.
		for len(p.idle) > 0 {
			ic := p.idle[0]
			p.idle = p.idle[1:]

			if p.isExpired(ic) {
				p.closeConn(ic.conn)
				continue
			}
			if p.healthCheck != nil && !p.healthCheck(ic.conn) {
				p.closeConn(ic.conn)
				continue
			}
			return ic.conn, nil
		}

		// No idle connection; create one if we have room.
		if p.numOpen < p.maxSize {
			conn, err := p.factory()
			if err != nil {
				return nil, err
			}
			p.numOpen++
			return conn, nil
		}

		// Pool is full and all connections are checked out.
		return nil, ErrPoolExhausted
	}
}

// Release returns a connection to the pool. If the pool is closed or the
// connection is not healthy, the connection is closed instead.
func (p *Pool) Release(conn Conn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		p.closeConn(conn)
		return
	}

	if p.healthCheck != nil && !p.healthCheck(conn) {
		p.closeConn(conn)
		return
	}

	// If we already have too many idle connections, close this one.
	if len(p.idle) >= p.maxIdle {
		p.closeConn(conn)
		return
	}

	p.idle = append(p.idle, &idleConn{
		conn:     conn,
		lastUsed: p.clock(),
	})
	p.cond.Signal()
}

// Close closes all idle connections and marks the pool as closed. Connections
// that are currently checked out are not closed; they will be closed when
// released.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	for _, ic := range p.idle {
		p.closeConn(ic.conn)
	}
	p.idle = nil
	p.cond.Broadcast()
	return nil
}

// Stats reports the current state of the pool.
type Stats struct {
	// Open is the number of connections the pool has created and not closed.
	Open int
	// Idle is the number of connections currently available for reuse.
	Idle int
	// InUse is the number of connections currently checked out.
	InUse int
}

// Stats returns a snapshot of the pool's counters.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Open:  p.numOpen,
		Idle:  len(p.idle),
		InUse: p.numOpen - len(p.idle),
	}
}

// isExpired reports whether an idle connection has exceeded IdleTimeout.
// Callers must hold p.mu.
func (p *Pool) isExpired(ic *idleConn) bool {
	if p.idleTimeout <= 0 {
		return false
	}
	return p.clock().Sub(ic.lastUsed) >= p.idleTimeout
}

// closeConn closes a connection and updates the open counter. Callers must
// hold p.mu.
func (p *Pool) closeConn(conn Conn) {
	_ = conn.Close()
	p.numOpen--
	if p.onClose != nil {
		p.onClose(conn)
	}
}
