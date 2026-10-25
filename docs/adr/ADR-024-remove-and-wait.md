# ADR-024: Remove an Entry and Wait for Its Invocations

## Status
Accepted

## Date
2026-10-25

## Context

`Remove` and `RemoveByName` unschedule an entry and cancel its per-entry
context, but return before an invocation that is still running has ended.
Waiting afterwards is impossible: `WaitForJob` and `WaitForJobByName` look the
entry up first, and the removal has already deleted it from both indexes, so
both return at once. Waiting first is no alternative, because only the removal
cancels the context; the wait would last until the job ends on its own
([#428](https://github.com/netresearch/go-cron/issues/428)).

Ofelia's `Scheduler.RemoveJob` called `RemoveByName` and then
`WaitForJobByName`, documented as waiting, and never did. A configuration
reload that replaced a job therefore registered the replacement while the
canceled run was still stopping its container.

## Decision

Add two methods:

```go
func (c *Cron) RemoveAndWait(ctx context.Context, id EntryID) error
func (c *Cron) RemoveByNameAndWait(ctx context.Context, name string) error
```

The removal request returns the entry's `jobTracker`. The run loop takes the
tracker in the same step that removes the entry, so it covers every invocation
that started before the removal, and none can start after it: the entry is
gone from the heap and from the indexes. A lookup followed by a separate
removal would leave a window in which a fire could start an invocation the
caller never sees.

The wait happens in the caller's goroutine, outside the run loop and without
`runningMu`. A wait inside the run loop would stall every entry for the
duration of the job, the failure mode [ADR-022](ADR-022-drain-and-upsert.md)
rejects for the same reason. Holding `runningMu` would deadlock against a
run-once completion, which takes that lock to remove its entry.

The wait is bounded by `ctx`, because a job that ignores cancellation can run
for as long as it likes. When `ctx` ends first, the method returns `ctx.Err()`;
the entry is removed either way. `ErrEntryNotFound` reports an unknown entry,
so a caller can tell "nothing to remove" from "removed and drained".

`Remove`, `RemoveByName`, `WaitForJob` and `WaitForJobByName` keep their
behaviour.

## Consequences

### Positive
- A caller can replace an entry without the old and the new invocation
  overlapping, by waiting for the old one before adding the new entry.
- The run loop never blocks on a job.

### Negative
- The caller chooses the bound. A short bound returns while a job that ignores
  cancellation still runs.

### Neutral
- Purely additive. The internal remove request now replies with the tracker
  instead of an empty struct.

## Alternatives Considered

### 1. Make `WaitForJobByName` remember removed names
Rejected: the name can be reused by a new entry right after the removal, so a
wait by name would join the wrong entry's invocations.

### 2. Return a wait function from `Remove`
Rejected: it changes the signature of an existing method that callers of the
robfig/cron API depend on.

## References

- [#428](https://github.com/netresearch/go-cron/issues/428): removing an entry gives no way to wait for its running invocation
- [ADR-010](ADR-010-channel-synchronization.md): the run loop owns the entry state
- [ADR-022](ADR-022-drain-and-upsert.md): why a drain must not run inside the run loop
