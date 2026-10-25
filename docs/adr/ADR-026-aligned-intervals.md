# ADR-026: Aligned Intervals

## Status
Accepted

## Date
2026-10-25

## Context

`@every <duration>` (`ConstantDelaySchedule`) counts from the time the
scheduler starts: the first run is the start time plus the interval, and each
next run is the previous one plus the interval. That has two consequences.

- **A restart moves the next run.** A scheduler that is restarted more often
  than the interval never runs the job: `@every 1h` in a process restarted
  every 50 minutes never fires.
- **Instances never share a scheduled time.** Two instances started a few
  seconds apart compute different times, so `SkipDuplicateRuns`
  ([ADR-025](ADR-025-locker-interface.md)) builds different keys and every
  instance runs.

Cron expressions do not have either problem, because their times depend only
on the clock. But a cron expression cannot state every interval: `@every 7m`
or `@every 90s` have no equivalent.

The specs are often written by the user of an application, not by its
developer: in Ofelia they are Docker labels or INI entries. A parser option
alone would let only the application choose.

## Decision

1. **`AlignedSchedule` fires at the multiples of its interval since the Unix
   epoch**, 1970-01-01 00:00:00 UTC. `Next(t)` is the first multiple after
   `t`, `Prev(t)` the last one before `t`; both return `t`'s location. The
   grid is absolute time, so time zones and DST do not shift it. Intervals
   are rounded like `Every` (at least one second, truncated to seconds) and
   follow the parser's minimum interval.
2. **Two new descriptors with a fixed meaning**, for the user:
   - `@every_aligned <duration>` parses to an `AlignedSchedule`;
   - `@every_start <duration>` parses to a `ConstantDelaySchedule`.
3. **`@every` keeps counting from the start by default**, as in robfig/cron.
   The `AlignEvery` parse option, for the application or its operator, makes
   a plain `@every` aligned. `@every_aligned` and `@every_start` are not
   affected by the option, so a spec that names one of them means the same on
   every parser.
4. **Intervals are exempt from the DST fall-back guard.** The guard
   ([ADR-016](ADR-016-dst-normalization.md)) skips a next time that shows the
   same wall-clock time as the previous run. One interval after 01:30 EDT is
   01:30 EST, a regular run, so the guard no longer applies to
   `ConstantDelaySchedule` or `AlignedSchedule`. This also fixes `@every 1h`,
   which lost that run before.
5. **No calendar units.** The duration is read by `time.ParseDuration`, whose
   largest unit is the hour. Calendar periods (days in local time, months,
   years) belong to `@daily`, `@monthly`, `@yearly` or a cron expression with
   `CRON_TZ=`, which already ignore restarts.

## Consequences

### Positive
- A job on `@every_aligned` runs on time across restarts, without persisted
  state.
- Instances share the scheduled times of `@every_aligned`, so
  `SkipDuplicateRuns` can deduplicate intervals.
- Users choose per job; operators can switch the default without editing
  every spec.

### Negative
- Two more descriptors to document and keep stable.
- With `AlignEvery`, the same `@every` spec means different schedules on
  differently configured parsers; `@every_aligned` and `@every_start` exist
  so a spec can be unambiguous.

### Neutral
- An aligned interval that does not divide a day, such as `@every_aligned 7m`,
  falls on different clock times each day. `@every_aligned 24h` runs at
  00:00 UTC, not at local midnight.

## Alternatives Considered

### 1. Make `@every` aligned for everyone
Rejected: it changes the meaning of existing specs and breaks the drop-in
compatibility with robfig/cron.

### 2. Parser option only
Rejected: only the application could choose, and users of an application
such as Ofelia could not pick the behavior per job.

### 3. Round the scheduled time in `SkipDuplicateRuns`
Rejected: the wrapper does not know the schedule, and it would not fix the
restart problem.

## References

- [ADR-015](ADR-015-zero-time-sentinel.md), [ADR-016](ADR-016-dst-normalization.md), [ADR-025](ADR-025-locker-interface.md)
- `aligned.go`, `aligned_test.go`, `parseDescriptor` in `parser.go`
- [Discussion #431](https://github.com/netresearch/go-cron/discussions/431)
