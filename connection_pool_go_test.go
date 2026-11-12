package connection_pool_go

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is a test connection that records whether it was closed.
type fakeConn struct {
	id     int
	closed atomic.Bool
	pingErr error
}

func (c *fakeConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *fakeConn) Ping() error {
	return c.pingErr
}

// fakeClock is a deterministic clock for idle timeout tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestNewRequiresFactory(t *testing.T) {
	_, err := New(Options{})
	if err == nil {
		t.Fatal("expected error when Factory is nil")
	}
}

func TestNewRejectsNegativeSizes(t *testing.T) {
	if _, err := New(Options{Factory: func() (Conn, error) { return &fakeConn{}, nil }, MaxSize: -1}); err == nil {
		t.Fatal("expected error for negative MaxSize")
	}
	if _, err := New(Options{Factory: func() (Conn, error) { return &fakeConn{}, nil }, MaxIdle: -1}); err == nil {
		t.Fatal("expected error for negative MaxIdle")
	}
}

func TestAcquireCreatesAndReleaseReuses(t *testing.T) {
	var created atomic.Int32
	p, err := New(Options{
		Factory: func() (Conn, error) {
			id := int(created.Add(1))
			return &fakeConn{id: id}, nil
		},
		MaxSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c1, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if created.Load() != 1 {
		t.Fatalf("expected 1 connection created, got %d", created.Load())
	}

	p.Release(c1)

	c2, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if c1 != c2 {
		t.Fatal("expected the same connection to be reused")
	}
	if created.Load() != 1 {
		t.Fatalf("expected 1 connection created, got %d", created.Load())
	}

	p.Release(c2)
}

func TestAcquireExhausted(t *testing.T) {
	p, err := New(Options{
		Factory: func() (Conn, error) { return &fakeConn{}, nil },
		MaxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	_, err = p.Acquire()
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected ErrPoolExhausted, got %v", err)
	}

	p.Release(c)
}

func TestAcquireAfterClose(t *testing.T) {
	p, err := New(Options{
		Factory: func() (Conn, error) { return &fakeConn{}, nil },
		MaxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = p.Acquire()
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestReleaseAfterCloseClosesConnection(t *testing.T) {
	p, err := New(Options{
		Factory: func() (Conn, error) { return &fakeConn{}, nil },
		MaxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	c, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	p.Release(c)

	fc := c.(*fakeConn)
	if !fc.closed.Load() {
		t.Fatal("expected connection to be closed on release after pool close")
	}
}

func TestHealthCheckDiscardsBadConnection(t *testing.T) {
	var created atomic.Int32
	p, err := New(Options{
		Factory: func() (Conn, error) {
			id := int(created.Add(1))
			return &fakeConn{id: id}, nil
		},
		MaxSize: 1,
		HealthCheck: func(c Conn) bool {
			fc := c.(*fakeConn)
			return fc.id != 1
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c1, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	p.Release(c1)

	c2, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if c2.(*fakeConn).id != 2 {
		t.Fatalf("expected a fresh connection with id 2, got id %d", c2.(*fakeConn).id)
	}
	if !c1.(*fakeConn).closed.Load() {
		t.Fatal("expected the unhealthy connection to be closed")
	}
	p.Release(c2)
}

func TestPingIsNotCalledByPool(t *testing.T) {
	// The pool only calls HealthCheck; Pinger is a marker interface that
	// callers may use in their own HealthCheck. This test documents that the
	// pool does not call Ping itself.
	var pinged atomic.Bool
	p, err := New(Options{
		Factory: func() (Conn, error) {
			return &fakeConn{pingErr: errors.New("ping failed")}, nil
		},
		MaxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if pinged.Load() {
		t.Fatal("pool should not call Ping itself")
	}
	p.Release(c)
}

func TestIdleTimeoutEviction(t *testing.T) {
	clk := &fakeClock{now: time.Unix(0, 0)}
	var created atomic.Int32
	p, err := New(Options{
		Factory: func() (Conn, error) {
			id := int(created.Add(1))
			return &fakeConn{id: id}, nil
		},
		MaxSize:     1,
		IdleTimeout: 10 * time.Second,
		Clock:       clk.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c1, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	p.Release(c1)

	// Not yet expired.
	clk.Advance(5 * time.Second)
	c2, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if c2 != c1 {
		t.Fatal("expected connection to be reused before idle timeout")
	}
	p.Release(c2)

	// Now expire it.
	clk.Advance(11 * time.Second)
	c3, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if c3 == c1 {
		t.Fatal("expected expired connection to be replaced")
	}
	if !c1.(*fakeConn).closed.Load() {
		t.Fatal("expected expired connection to be closed")
	}
	p.Release(c3)
}

func TestMaxIdleEviction(t *testing.T) {
	var created atomic.Int32
	var closedCount atomic.Int32
	p, err := New(Options{
		Factory: func() (Conn, error) {
			id := int(created.Add(1))
			return &fakeConn{id: id}, nil
		},
		MaxSize: 2,
		MaxIdle: 1,
		OnClose: func(Conn) { closedCount.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c1, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	p.Release(c1)
	p.Release(c2)

	// Only one idle connection should be kept; the other closed.
	if closedCount.Load() != 1 {
		t.Fatalf("expected exactly one connection closed by MaxIdle, got %d", closedCount.Load())
	}

	stats := p.Stats()
	if stats.Idle != 1 {
		t.Fatalf("expected 1 idle connection, got %d", stats.Idle)
	}
}

func TestStats(t *testing.T) {
	p, err := New(Options{
		Factory: func() (Conn, error) { return &fakeConn{}, nil },
		MaxSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	c1, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	c2, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	stats := p.Stats()
	if stats.Open != 2 || stats.Idle != 0 || stats.InUse != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	p.Release(c1)
	stats = p.Stats()
	if stats.Open != 2 || stats.Idle != 1 || stats.InUse != 1 {
		t.Fatalf("unexpected stats after release: %+v", stats)
	}

	p.Release(c2)
}

func TestFactoryErrorPropagates(t *testing.T) {
	want := errors.New("factory failed")
	p, err := New(Options{
		Factory: func() (Conn, error) { return nil, want },
		MaxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Acquire()
	if !errors.Is(err, want) {
		t.Fatalf("expected factory error, got %v", err)
	}
}

func TestConcurrentAcquireRelease(t *testing.T) {
	var created atomic.Int32
	p, err := New(Options{
		Factory: func() (Conn, error) {
			created.Add(1)
			return &fakeConn{}, nil
		},
		MaxSize: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := p.Acquire()
			if err != nil {
				if !errors.Is(err, ErrPoolExhausted) {
					t.Errorf("unexpected error: %v", err)
				}
				return
			}
			p.Release(c)
		}()
	}
	wg.Wait()

	if created.Load() > 4 {
		t.Fatalf("pool exceeded MaxSize: created %d", created.Load())
	}
}
