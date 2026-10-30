# ADR-027: DST Fall-Back by Rhythm

## Status
Accepted. Supersedes the fall-back part of
[ADR-016](ADR-016-dst-normalization.md); its spring-forward part stays.

## Date
2026-10-29

## Context

On a fall-back night the clock repeats a stretch of time, one hour in most
zones (01:00–01:59 in New York), 30 minutes on Lord Howe Island and two hours
on Antarctica/Troll. Every wall-clock time in that stretch occurs twice.

ADR-016 decided that a job on a cron expression runs once per wall-clock time.
The scheduler compared the next time only with the run just fired, which had
three consequences:

- **Fixed times ran twice** when a spec had several of them in the repeated
  hour: `0,30 1 * * *` ran 01:00 and 01:30 in both copies
  ([#446](https://github.com/netresearch/go-cron/issues/446)).
- **Hourly jobs lost a run.** `0 * * * *` ran 01:00 EDT and then 02:00 EST,
  two real hours apart.
- **A strict "once per wall-clock time" rule breaks frequent jobs.** Applied
  to every restricted hour field, `*/5 0-6 * * *` runs every 5 minutes through
  the first copy and then nothing for 65 minutes.

What a user states with a cron expression is how often a job runs in a span
of time: every 5 minutes, every hour, every two hours, once a day. The
repeated stretch is real time, so a job that runs every 5 minutes or every
hour must keep running in it, and a job that runs once a day or every two
hours must not run more often because of it.

## Decision

1. **A spec that runs in every stretch of the repeat length runs in both
   copies.** For a time `W` in the second copy, the scheduler checks whether
   the spec also matches `W` minus or plus the repeat length on a calendar
   without DST (`recursAcrossRepeat` in `cron.go`). If it does, the run in the
   second copy is a regular run: `*/5 0-6 * * *` stays 5 minutes apart in real
   time, `0 * * * *` and `@hourly` stay one hour apart.
2. **Any other spec runs each wall-clock time once that day.** The scheduler
   skips `W` in the second copy when the entry fired `W` in the first copy
   (the entry's record of first-copy runs) or when `W` repeats the run just
   fired (`isDSTFallBackDuplicate`). `0,30 1 * * *` and `*/20 1 * * *` run each
   time once; `0 */2 * * *` in Berlin runs 00:00, 02:00 CEST and 04:00 CET, so
   no two runs are less than two hours apart.
3. **A time the entry did not fire in the first copy still runs in the second
   copy**: the entry was added later, paused, or the process was asleep.
4. **Each member of a `UnionSchedule` is checked on its own**, in its own
   location. The second copy runs when any member that fires there runs.
5. **A `Schedule` that is not a `SpecSchedule`** keeps the comparison with the
   run just fired. **Intervals** (`@every`, `@every_start`, `@every_aligned`)
   stay exempt ([ADR-026](ADR-026-aligned-intervals.md)).
6. **`WithMissedPolicy` catch-ups follow the same rule**, and a catch-up that
   runs counts as a run of its time.

The rule is pinned by `TestDSTFallBack_RunsPerRhythm`,
`TestDSTFallBack_RunsPerRhythmOtherZones`, `TestDSTFallBack_UnionMemberRhythm`
and `TestRecursAcrossRepeat` in `dst_fallback_test.go`, and by
`TestDSTFallBackSweep` in `dst_fallback_sweep_test.go`, which checks 20
fall-back transitions of 2026 in 20 zones.

## Consequences

### Positive

- Jobs that run every few minutes or every hour keep their spacing through
  the repeated stretch; `0 * * * *` runs 25 times that day, every hour.
- Jobs at fixed times run once per time, as ADR-016 intended, also with
  several times in the repeated hour.
- Jobs every two or more hours keep at least their spacing.
- The decision depends only on the spec and the repeat length, not on the
  text of the spec: `0-23` and `*` behave the same.

### Negative

- A spec that matches every hour only inside a window runs hourly through both
  copies: `0 1,2 * * *` runs 01:00 EDT, 01:00 EST and 02:00 EST, three times
  that day.
- On Lord Howe Island the clock repeats 30 minutes. A rhythm that does not
  repeat every 30 minutes, such as `*/20 * * * *`, runs 01:40 once, so two
  runs are 50 minutes apart.
- The record of first-copy runs lives in memory: after a process restart
  inside the repeated stretch, fixed times of the first copy run again.

## Alternatives Considered

### 1. Once per wall-clock time for every spec

ADR-016 as written, extended by the record of first-copy runs. Rejected: it
gives `*/5 0-6 * * *` a 65-minute pause and takes a run from `0 * * * *`.

### 2. Vixie cron's rule: a `*` in the minute or hour field means "run both"

Rejected: it decides by how the spec is written. `*/20 1 * * *` would run
twice (#446), and `0 */2 * * *` in Berlin would run 02:00 CEST and 02:00 CET,
one hour apart.

### 3. Compare the time since the last run with the spec's own spacing

Run `W` in the second copy if the real time since the last run is at least the
gap the spec has after that run on a calendar without DST. Rejected: the result
depends on the run history. With `0,30 1 * * *` paused over 01:30 EDT, 01:00
EST comes one hour after 01:00 EDT and would run a second time.
