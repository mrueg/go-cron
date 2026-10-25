# ADR-023: Run Information in the Job Context

## Status
Accepted

## Date
2026-10-25

## Context

The scheduler knows, for every invocation, the time the invocation was
scheduled for and what started it: a fire of the schedule, a call to
`TriggerEntry`, a catch-up from the entry's `MissedPolicy`, or a workflow parent
reaching its condition. A job sees none of it. The scheduled time reaches only
the `OnJobStart` observability hook, and the hooks run asynchronously
([ADR-003](ADR-003-async-observability.md)) and before the chain wrappers, so
they also fire for an invocation that a wrapper such as `SkipIfStillRunning`
then drops.

A caller that keeps its own schedule state needs both values in the code that
admits or drops the run. Ofelia's durable schedule state
([netresearch/ofelia ADR-005](https://github.com/netresearch/ofelia/blob/main/docs/adr/ADR-005-durable-schedule-state.md))
records the last scheduled time it evaluated, and a manual run, which arrives
with the time of the call as its scheduled time, must not move that record.
Without the kind it cannot tell the two apart.

The context already carries per-invocation data: an entry with dependents puts
its workflow execution ID there (`WorkflowExecutionID`).

## Decision

Every invocation's context carries a `RunInfo`:

```go
type RunInfo struct {
    ScheduledTime time.Time
    Trigger       TriggerKind
}

func RunInfoFromContext(ctx context.Context) (RunInfo, bool)
```

`TriggerKind` has five values, one per way the scheduler starts a job:
`TriggerScheduled`, `TriggerManual`, `TriggerCatchUp`, `TriggerDependency` and
`TriggerImmediate`. The last is the first run of an entry added
`WithRunImmediately`. It goes through the same path as a fire of the schedule,
but its time is the time the entry was added or the scheduler started, which
the schedule never produces; reported as `TriggerScheduled`, it would look like
a fire at that time and move a record such as Ofelia's to it. The zero value is
not a kind, so a `RunInfo` that was never set is
distinguishable from a real one, and `TriggerKind.Valid` follows the
defensive-enum pattern of `MissedPolicy`.

The value is set once, in `startJobWithExecution`, which every start path
reaches. Each call site passes its kind explicitly. `startWorkflowExecution`
takes the kind as a parameter instead of deriving it, because it is reached
from both the schedule fire and the manual trigger. The workflow execution ID
is layered on top of the same context, so a workflow job carries both.

A caller can also start an invocation with a `RunInfo` of its own:
`TriggerEntryWithInfo(id, info)` and `TriggerEntryWithInfoByName`. A caller that
keeps its own schedule state decides on its catch-ups itself, and needs the run
it starts to arrive as what it is, with the scheduled time it was started for.
Started through `TriggerEntry`, it would arrive as a manual run at the current
time, and the caller would have to keep side state to recognize it. The kind
must be valid (`ErrInvalidRunInfo` otherwise); a zero time means now.

The value travels in the context, not in a new interface method: the `Job` and
`JobWithContext` signatures stay unchanged
([ADR-005](ADR-005-decorator-pattern.md), [ADR-017](ADR-017-job-with-context.md)),
and the chain wrappers already pass the context on.

## Consequences

### Positive
- A job, or a wrapper in front of it, can record which scheduled time it ran
  for and whether a user started it, without hooks and without races against
  them.
- Purely additive: no signature changes, no new option.

### Negative
- One `context.WithValue` allocation per invocation.
- A `Job` that does not implement `JobWithContext` cannot read it. That is the
  same limit `WorkflowExecutionID` has.

### Neutral
- `OnJobStart` keeps its scheduled-time parameter; the hook and the context
  report the same value.

## Alternatives Considered

### 1. Pass the values through `OnJobStart` only
Rejected: the hook is asynchronous and runs before the wrappers, so it cannot
tell whether the invocation it reports actually ran.

### 2. Two kinds only (scheduled and manual)
Rejected: catch-ups and workflow children are started by the scheduler too.
Reporting them as scheduled would hide a catch-up's missed time behind the
schedule; reporting them as manual would be false.

## References

- [ADR-017](ADR-017-job-with-context.md): `JobWithContext` and the per-entry context
- `WorkflowExecutionID` in `workflow.go`: the existing per-invocation context value
- [netresearch/go-cron#428](https://github.com/netresearch/go-cron/issues/428): the companion removal change
