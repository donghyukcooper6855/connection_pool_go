# connection_pool_go

A bounded pool of reusable connections with health checks and idle timeout eviction.

```go
package main

import (
	"fmt"
	"time"

	pool "connection_pool_go"
)

type myConn struct{}

func (c *myConn) Close() error { return nil }

func main() {
	p, err := pool.New(pool.Options{
		Factory: func() (pool.Conn, error) {
			return &myConn{}, nil
		},
		MaxSize:     10,
		MaxIdle:     5,
		IdleTimeout: 30 * time.Second,
		HealthCheck: func(c pool.Conn) bool {
			return true
		},
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	conn, err := p.Acquire()
	if err != nil {
		panic(err)
	}
	defer p.Release(conn)

	fmt.Println("acquired a connection")
}
```

## Why this exists

Opening a fresh connection per operation is expensive, and an unbounded cache of
connections is a resource leak waiting to happen. This pool caps the number of
live connections at `MaxSize` and the number of idle connections at `MaxIdle`,
so memory and file descriptors stay bounded under load.

The trade-off is explicit: `Acquire` never blocks indefinitely. When the pool is
full and every connection is checked out, `Acquire` returns `ErrPoolExhausted`
rather than waiting for a connection to come back. Callers that want to wait can
retry with their own backoff; callers that want to shed load get a fast, clear
signal. A blocking pool would be easier to call but harder to reason about under
sustained overload.

## The awkward edge

Idle eviction only happens lazily, when `Acquire` looks at the idle list. There
is no background goroutine. A pool that is never used again will hold its idle
connections until `Close` is called. This keeps the library free of timers and
makes idle behaviour deterministic in tests, but it means you should call
`Close` when you are done with the pool.

`MaxIdle` is clamped to `MaxSize`; asking for more idle connections than the
pool can ever hold is treated as a mistake, not a request to grow the pool.
