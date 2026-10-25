# ADR-025: Locker Interface for Coordination Between Instances

## Status
Accepted. Extends [ADR-020](ADR-020-feature-scope-boundary.md).

## Date
2026-10-25

## Context

[ADR-020](ADR-020-feature-scope-boundary.md) puts distributed locking out of
scope, because it needs a shared store such as Redis, etcd or a SQL database,
and points to [DISTRIBUTED_LOCKING_GUIDE.md](../DISTRIBUTED_LOCKING_GUIDE.md).
Every user therefore writes the coordination wrapper themselves.

The guide's patterns lock by job name and release the lock after the run. That
does not deduplicate: if instance A runs a one-second job at 09:00:00 and
instance B's clock is two seconds late, B takes the released lock at 09:00:02
by A's clock and runs the same scheduled time again. Correct deduplication
needs a key from the job and its **scheduled** time, and the reservation must
outlive the run. A wrapper could not know the scheduled time until
[ADR-023](ADR-023-run-info-in-context.md) put `RunInfo` into the job context.

Discussion [#431](https://github.com/netresearch/go-cron/discussions/431)
raised the case of several instances of one service that each run the same
jobs.

## Decision

go-cron defines the coordination interface and the wrappers on top of it. The
stores stay out of the library: go-cron keeps zero external dependencies.

```go
type Locker interface {
    TryLock(ctx context.Context, key string, ttl time.Duration) (release func(), ok bool, err error)
}

func NewMemoryLocker(clock Clock) *MemoryLocker
func SkipDuplicateRuns(locker Locker, name string, ttl time.Duration, logger Logger) JobWrapper
func SkipWhileLocked(locker Locker, key string, ttl time.Duration, logger Logger) JobWrapper
```

1. **One interface for both uses.** `TryLock` reserves a key until the TTL
   elapses or `release` is called. Deduplication never calls `release`;
   mutual exclusion of a resource releases after the run. `TryLock` does not
   wait, so every store can implement it with one atomic operation (`SET NX PX`
   in Redis, an insert with a unique key in SQL). A `release` after the
   reservation expired must not remove a newer holder's reservation.
2. **`MemoryLocker` is the only implementation in the library.** It
   coordinates the schedulers of one process and serves as the reference and
   test double. It takes a `Clock`, so TTL tests run on `FakeClock`.
3. **The dedup key is `name@<scheduled time in UTC, RFC 3339 with
   nanoseconds>`.** UTC makes instances in different zones produce one key for
   one instant. The name is a parameter, because `RunInfo` carries no entry
   name and a chain applies to every entry it wraps. The instances must share
   the spec, or they compute different scheduled times. A cron expression
   also needs the same time zone; an aligned interval does not, because its
   grid counts in absolute time. By
   default an `@every` schedule counts from each instance's own start, so
   instances never share its times and it cannot be deduplicated; the
   documentation says so.
   `@every_aligned`, or plain `@every` with the `AlignEvery` parse option
   ([ADR-026](ADR-026-aligned-intervals.md)), gives intervals a shared grid.
4. **Only `TriggerScheduled` and `TriggerCatchUp` reserve a key.** Both carry a
   time of the schedule, which every instance shares. Manual, dependency and
   immediate runs carry the time they were started, which no other instance
   shares; they run without a reservation.
5. **Fail closed.** When the locker returns an error, or the context carries
   no `RunInfo`, the run is skipped and an error logged. Running anyway would
   run every instance at once exactly when the store is down. A missing
   `RunInfo` means a wrapper before this one dropped the context
   (`RetryWithBackoff`, `RetryOnError` and `CircuitBreaker` do); the log
   message says so.
6. **At most once.** The reservation is taken before the run and never
   released, so a crash of the holder loses that scheduled time. That matches
   the rest of go-cron, which keeps no run state
   ([ADR-020](ADR-020-feature-scope-boundary.md)). At least once would need
   persisted run state and lease renewal.
7. **The TTL must exceed the largest clock difference between instances.**
   A longer TTL only keeps the key in the store longer.
8. **A skipped run is a skipped workflow result.** A job that returns without
   panicking counts as `ResultSuccess` in a workflow, so the dependents of a
   parent that this instance skipped would start here as well, and every
   instance would run them. The wrapper marks the run as skipped through the
   run context (`markRunSkipped`), and the workflow records `ResultSkipped`:
   dependents run only where the parent ran.
9. **`MemoryLocker` removes expired keys.** Deduplication never releases a
   key, so without removal the store would keep one key per scheduled time.
   `TryLock` removes the expired keys whenever the store has doubled since the
   last removal, which keeps the cost per call constant on average.
10. **Exclusion of a resource is its own wrapper.** `SkipWhileLocked(locker,
    key, ttl, logger)` reserves the resource name before each run and releases
    it when the run ends, also after a panic; a run that finds the key taken
    is skipped. It does not deduplicate a scheduled time, and its TTL must
    exceed the longest run. Across processes it only skips: waiting would need
    a blocking acquire that not every store has (alternative 3). Within one
    process, `ConcurrencyGroups` also offers the waiting variant.

## Consequences

### Positive
- Several instances of a service can share a schedule without each user
  writing and getting wrong the dedup wrapper.
- A store implementation is one method, and the wrapper logic is tested once,
  in the library.

### Negative
- `Locker`, `MemoryLocker` and `SkipDuplicateRuns` are public API and must stay
  stable.
- Users still write and operate the store implementation.

### Neutral
- ADR-020's boundary moves by one interface: the contract belongs to go-cron,
  the store does not.

## Alternatives Considered

### 1. Keep only the guide (ADR-020 unchanged)
Rejected: the guide's own pattern was wrong, and each user re-derives the key
and the TTL rules.

### 2. Ship store implementations (Redis, Postgres)
Rejected for the reasons in [ADR-020](ADR-020-feature-scope-boundary.md):
dependencies, and one module per store to maintain.

### 3. A blocking `Lock` in the interface
Rejected: not every store can block, and deduplication never waits. A caller
that wants to wait can retry `TryLock`.

## References

- [ADR-020](ADR-020-feature-scope-boundary.md): feature scope
- [ADR-023](ADR-023-run-info-in-context.md): `RunInfo` with the scheduled time
- `locker.go`, `locker_test.go`
