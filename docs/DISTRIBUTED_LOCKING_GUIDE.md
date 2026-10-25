# Distributed Locking Guide

This guide shows how to ensure only one instance of a job runs across multiple application instances using external locking mechanisms.

## What go-cron Provides

go-cron ships the coordination interface and the wrappers on top of it, but no store:

- **`cron.Locker`**: one method, `TryLock(ctx, key, ttl) (release, ok, err)`. You implement it for the store your instances share (Redis, PostgreSQL, etcd, …). `cron.NewMemoryLocker` coordinates the schedulers of one process and serves as a test double.
- **`cron.SkipDuplicateRuns`**: runs each scheduled time of a job once across all instances.
- **`cron.SkipWhileLocked`**: lets jobs that use one resource run one at a time across all instances.

The stores stay out of the library, because each needs its own client dependency. See [ADR-025](adr/ADR-025-locker-interface.md), which extends [ADR-020](adr/ADR-020-feature-scope-boundary.md).

## The Problem

When running multiple instances of your application (for high availability or scaling), each instance has its own go-cron scheduler. Without coordination, the same job runs on every instance simultaneously:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Instance 1 │     │  Instance 2 │     │  Instance 3 │
│  go-cron    │     │  go-cron    │     │  go-cron    │
│             │     │             │     │             │
│  9:00 AM    │     │  9:00 AM    │     │  9:00 AM    │
│  daily-job  │     │  daily-job  │     │  daily-job  │
│  ──────────►│     │  ──────────►│     │  ──────────►│
│  RUNS!      │     │  RUNS!      │     │  RUNS!      │
└─────────────┘     └─────────────┘     └─────────────┘
        │                   │                   │
        └───────────────────┴───────────────────┘
                          │
                   All 3 run simultaneously!
                   (Usually not what you want)
```

## Solution: Reserve the Run in a Shared Store

Before running, each instance tries to reserve the run in a store all instances share. Only the instance that gets the reservation runs:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Instance 1 │     │  Instance 2 │     │  Instance 3 │
│             │     │             │     │             │
│  9:00 AM    │     │  9:00 AM    │     │  9:00 AM    │
│  TRY LOCK   │     │  TRY LOCK   │     │  TRY LOCK   │
│  ────────►  │     │  ────────►  │     │  ────────►  │
│  GOT IT! ✓  │     │  BLOCKED ✗  │     │  BLOCKED ✗  │
│  RUNS!      │     │  skip       │     │  skip       │
└─────────────┘     └─────────────┘     └─────────────┘
        │
        └── Only one instance runs the job
```

### Deduplication needs the scheduled time in the key

A lock named after the job and released after the run does **not** deduplicate. Example: the job runs for 1 second, and instance B's clock is 2 seconds behind instance A's. A takes the lock at 09:00:00, runs, and releases it at 09:00:01. B fires at 09:00:02 by A's clock, finds the lock free and runs the same 09:00 a second time.

Correct deduplication reserves a key made of the job name **and the scheduled time**, such as `daily-report@2026-10-26T09:00:00Z`, and does **not** release it after the run; the reservation expires through its TTL. Every instance that comes later for the same scheduled time finds it taken, however late its clock is. `cron.SkipDuplicateRuns` does exactly this; the scheduled time comes from `cron.RunInfoFromContext` ([ADR-023](adr/ADR-023-run-info-in-context.md)).

| Goal | Key | Released after the run | Wrapper |
|------|-----|------------------------|---------|
| Run each scheduled time once across instances | job name + scheduled time | No, expires via TTL | `SkipDuplicateRuns` |
| Never run jobs on one resource at the same time | resource name | Yes | `SkipWhileLocked` |

All instances must compute the same scheduled times: same spec, same time zone (`CRON_TZ=` or the same `WithLocation`). `@every` counts from each instance's own start and cannot be deduplicated; use a cron expression.

## Implementation Patterns

### Pattern 1: Redis Locker

A `cron.Locker` for Redis is one `SET NX PX` for `TryLock` and a compare-and-delete for `release`, so a release after expiry cannot remove a newer holder's key.

```go
package main

import (
    "context"
    "crypto/rand"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    cron "github.com/netresearch/go-cron"
)

// releaseScript deletes the key only if it still holds this caller's token.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0`)

// RedisLocker implements cron.Locker on a Redis instance all schedulers share.
type RedisLocker struct{ client *redis.Client }

func (l RedisLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (func(), bool, error) {
    token := rand.Text()
    ok, err := l.client.SetNX(ctx, key, token, ttl).Result()
    if err != nil || !ok {
        return nil, false, err
    }
    release := func() {
        _ = releaseScript.Run(context.Background(), l.client, []string{key}, token).Err()
    }
    return release, true, nil
}

func main() {
    locker := RedisLocker{client: redis.NewClient(&redis.Options{Addr: "localhost:6379"})}
    logger := cron.DefaultLogger

    c := cron.New(cron.WithChain(cron.Recover(logger)))

    // Each 09:00 runs on one instance only. The TTL must exceed the largest
    // clock difference between the instances.
    c.AddJob("0 9 * * *",
        cron.NewChain(
            cron.SkipDuplicateRuns(locker, "daily-report", 10*time.Minute, logger),
        ).Then(cron.FuncJob(dailyReport)),
        cron.WithName("daily-report"),
    )

    c.Start()
    select {} // Run forever
}

func dailyReport() {
    log.Println("Running daily report (only on one instance)")
}
```

The patterns below hold a lock for the duration of the run and release it afterwards. That serializes runs on a resource, like `SkipWhileLocked`, but it does not deduplicate a scheduled time (see above). To deduplicate with these stores, implement `cron.Locker` on them and use `SkipDuplicateRuns`.

### Pattern 2: PostgreSQL Advisory Locks

Use PostgreSQL's built-in advisory locks for coordination without additional infrastructure.

```go
package main

import (
    "context"
    "database/sql"
    "hash/fnv"
    "log"

    _ "github.com/lib/pq"
    cron "github.com/netresearch/go-cron"
)

// WithPgAdvisoryLock creates a wrapper using PostgreSQL advisory locks
func WithPgAdvisoryLock(db *sql.DB, lockName string) cron.JobWrapper {
    // Convert lock name to int64 for pg_advisory_lock
    lockID := hashStringToInt64(lockName)

    return func(job cron.Job) cron.Job {
        return cron.FuncJob(func() {
            // Try to acquire lock (non-blocking)
            var acquired bool
            err := db.QueryRow("SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
            if err != nil {
                log.Printf("Error checking advisory lock: %v", err)
                return
            }

            if !acquired {
                log.Printf("Could not acquire advisory lock %s, skipping", lockName)
                return
            }

            // Ensure we release the lock when done
            defer func() {
                _, err := db.Exec("SELECT pg_advisory_unlock($1)", lockID)
                if err != nil {
                    log.Printf("Error releasing advisory lock: %v", err)
                }
            }()

            job.Run()
        })
    }
}

func hashStringToInt64(s string) int64 {
    h := fnv.New64a()
    h.Write([]byte(s))
    return int64(h.Sum64())
}

func main() {
    db, _ := sql.Open("postgres", "postgres://localhost/mydb?sslmode=disable")

    c := cron.New()

    c.AddJob("*/5 * * * *",
        cron.NewChain(
            WithPgAdvisoryLock(db, "sync-inventory"),
        ).Then(cron.FuncJob(syncInventory)),
        cron.WithName("sync-inventory"),
    )

    c.Start()
    select {}
}
```

### Pattern 3: etcd Distributed Lock

Use etcd for coordination in Kubernetes or cloud-native environments.

```go
package main

import (
    "context"
    "log"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
    cron "github.com/netresearch/go-cron"
)

// WithEtcdLock creates a wrapper using etcd distributed locks
func WithEtcdLock(client *clientv3.Client, lockName string, ttl int) cron.JobWrapper {
    return func(job cron.Job) cron.Job {
        return cron.FuncJob(func() {
            // Create a session with TTL
            session, err := concurrency.NewSession(client,
                concurrency.WithTTL(ttl),
            )
            if err != nil {
                log.Printf("Failed to create etcd session: %v", err)
                return
            }
            defer session.Close()

            // Try to acquire the lock
            mutex := concurrency.NewMutex(session, "/cron-locks/"+lockName)

            ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
            defer cancel()

            if err := mutex.TryLock(ctx); err != nil {
                log.Printf("Could not acquire etcd lock %s, skipping: %v", lockName, err)
                return
            }
            defer mutex.Unlock(context.Background())

            job.Run()
        })
    }
}

func main() {
    client, _ := clientv3.New(clientv3.Config{
        Endpoints:   []string{"localhost:2379"},
        DialTimeout: 5 * time.Second,
    })
    defer client.Close()

    c := cron.New()

    c.AddJob("0 */6 * * *",
        cron.NewChain(
            WithEtcdLock(client, "cleanup-job", 600), // 10 min TTL
        ).Then(cron.FuncJob(cleanupJob)),
        cron.WithName("cleanup-job"),
    )

    c.Start()
    select {}
}
```

### Pattern 4: File-Based Lock (Single Host, Unix Only)

For multiple processes on the same host (not distributed), use file locks.

> **Note**: This pattern uses `syscall.Flock` which is Unix-specific (Linux, macOS, BSD).
> It will not compile on Windows. For Windows, use `LockFileEx` from the `golang.org/x/sys/windows` package.

```go
package main

import (
    "log"
    "os"
    "syscall"

    cron "github.com/netresearch/go-cron"
)

// WithFileLock creates a wrapper using filesystem locks (single host only)
func WithFileLock(lockPath string) cron.JobWrapper {
    return func(job cron.Job) cron.Job {
        return cron.FuncJob(func() {
            // Open or create lock file
            f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
            if err != nil {
                log.Printf("Failed to open lock file: %v", err)
                return
            }
            defer f.Close()

            // Try to acquire exclusive lock (non-blocking)
            err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
            if err != nil {
                log.Printf("Could not acquire file lock %s, skipping", lockPath)
                return
            }
            defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

            job.Run()
        })
    }
}

func main() {
    c := cron.New()

    c.AddJob("* * * * *",
        cron.NewChain(
            WithFileLock("/var/lock/my-cron-job.lock"),
        ).Then(cron.FuncJob(myJob)),
    )

    c.Start()
    select {}
}
```

## Combining with Other Wrappers

Chain the coordination wrapper with other job wrappers:

```go
c.AddJob("0 9 * * *",
    cron.NewChain(
        cron.Recover(cron.DefaultLogger),                                      // 1. Recover from panics
        cron.SkipDuplicateRuns(locker, "daily-report", 10*time.Minute, logger), // 2. Reserve this scheduled time
        cron.SkipIfStillRunning(cron.DefaultLogger),                           // 3. Skip if already running locally
        cron.RetryWithBackoff(logger, 3, time.Second, time.Minute, 2),         // 4. Retry the job itself
    ).Then(cron.FuncJob(dailyReport)),
    cron.WithName("daily-report"),
)
```

**Order matters!** The wrappers execute from outside in:
1. Recover catches any panic
2. The scheduled time is reserved
3. Local skip check (prevents concurrent local runs)
4. Retries run inside the reservation
5. Job runs

`SkipDuplicateRuns` must come before `RetryWithBackoff`, `RetryOnError` and `CircuitBreaker`: those drop the job context, and without the scheduled time from it the wrapper skips the run and logs an error.

## Lock TTL Considerations

Set lock TTL appropriately:

| Scenario | TTL Recommendation |
|----------|-------------------|
| `SkipDuplicateRuns` | Longer than the largest clock difference between instances; job duration does not matter |
| Quick job (<1 min) | 2-5 minutes |
| Medium job (1-10 min) | Job duration × 2 |
| Long job (>10 min) | Job duration + buffer, or use lock extension |
| Unknown duration | Use heartbeat/extension pattern |

### Lock Extension for Long Jobs

For jobs with unpredictable duration, extend the lock periodically. This example uses [redsync](https://github.com/go-redsync/redsync), which implements the Redlock algorithm; it serializes runs and does not deduplicate a scheduled time:

```go
func WithRedisLockExtending(rs *redsync.Redsync, lockName string, ttl time.Duration) cron.JobWrapper {
    return func(job cron.Job) cron.Job {
        return cron.FuncJob(func() {
            mutex := rs.NewMutex(lockName, redsync.WithExpiry(ttl))

            if err := mutex.Lock(); err != nil {
                return
            }

            // Start extension goroutine
            done := make(chan struct{})
            go func() {
                ticker := time.NewTicker(ttl / 2)
                defer ticker.Stop()
                for {
                    select {
                    case <-done:
                        return
                    case <-ticker.C:
                        if ok, err := mutex.Extend(); !ok || err != nil {
                            log.Printf("Failed to extend lock: %v", err)
                        }
                    }
                }
            }()

            defer func() {
                close(done)
                mutex.Unlock()
            }()

            job.Run()
        })
    }
}
```

## Leader Election Alternative

Instead of locking each job, elect a single leader to run all cron jobs:

```go
package main

import (
    "context"
    "log"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
    cron "github.com/netresearch/go-cron"
)

func main() {
    client, _ := clientv3.New(clientv3.Config{
        Endpoints: []string{"localhost:2379"},
    })

    session, _ := concurrency.NewSession(client, concurrency.WithTTL(10))
    election := concurrency.NewElection(session, "/cron-leader")

    // Campaign for leadership
    ctx := context.Background()
    if err := election.Campaign(ctx, "instance-1"); err != nil {
        log.Fatal(err)
    }

    log.Println("I am the leader, starting cron scheduler")

    // Only the leader runs the scheduler
    c := cron.New()
    c.AddFunc("0 9 * * *", dailyReport)
    c.AddFunc("*/5 * * * *", syncData)
    c.Start()

    // Resign leadership on shutdown
    defer election.Resign(context.Background())

    select {} // Run forever
}
```

## Testing Distributed Locks

Test lock behavior with FakeClock:

```go
func TestDistributedLockSkipsWhenLocked(t *testing.T) {
    // Simulate a lock that's already held
    mockLock := &MockDistributedLock{
        locked: true, // Already locked by another instance
    }

    var jobRan bool
    wrapper := WithMockLock(mockLock)
    wrappedJob := wrapper(cron.FuncJob(func() {
        jobRan = true
    }))

    wrappedJob.Run()

    if jobRan {
        t.Error("Job should have been skipped when lock unavailable")
    }
}

func TestDistributedLockRunsWhenAvailable(t *testing.T) {
    mockLock := &MockDistributedLock{
        locked: false, // Lock available
    }

    var jobRan bool
    wrapper := WithMockLock(mockLock)
    wrappedJob := wrapper(cron.FuncJob(func() {
        jobRan = true
    }))

    wrappedJob.Run()

    if !jobRan {
        t.Error("Job should have run when lock available")
    }
    if !mockLock.wasAcquired {
        t.Error("Lock should have been acquired")
    }
    if !mockLock.wasReleased {
        t.Error("Lock should have been released")
    }
}
```

## When to Use Which Backend

| Backend | Best For | Pros | Cons |
|---------|----------|------|------|
| **Redis** | Most deployments | Fast, `SET NX PX` is one atomic call | Requires Redis |
| **PostgreSQL** | Already using Postgres | No extra infra | Single point of failure |
| **etcd** | Kubernetes environments | Built for coordination | More complex setup |
| **Consul** | HashiCorp stack | Service mesh integration | Another service to run |
| **File locks** | Single host, multi-process | Simple, no external deps | Not distributed |

## Alternatives for Heavy Coordination Needs

If you need sophisticated distributed scheduling with built-in coordination:

| Tool | Description |
|------|-------------|
| [Dkron](https://dkron.io/) | Distributed cron with Raft consensus |
| [Temporal](https://temporal.io/) | Workflow orchestration with durable execution |
| [Kubernetes CronJob](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/) | K8s-native scheduled jobs |

## See Also

- [PERSISTENCE_GUIDE.md](PERSISTENCE_GUIDE.md) - Persist job state across restarts
- [OPERATIONS.md](OPERATIONS.md) - Production deployment patterns
- [COOKBOOK.md](COOKBOOK.md) - Common integration recipes
- [ADR-025](adr/ADR-025-locker-interface.md) - The `Locker` interface and its wrappers
- [ADR-020](adr/ADR-020-feature-scope-boundary.md) - Why the stores stay out of the library
