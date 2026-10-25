// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import "time"

// AlignedSchedule fires at the multiples of Interval since the Unix epoch,
// 1970-01-01 00:00:00 UTC. Every scheduler that runs it computes the same
// times, whenever it started, so a restart does not move the next run and
// several instances can deduplicate a run with SkipDuplicateRuns (ADR-026).
//
// ConstantDelaySchedule (@every) counts from the time the scheduler starts
// instead: restarted more often than its interval, it never runs.
//
// The grid is in absolute time, so time zones and DST do not shift it. An
// interval that does not divide a day evenly falls on different clock times
// each day; for runs at a fixed time of day, use a cron expression.
type AlignedSchedule struct {
	Interval time.Duration
}

// EveryAligned returns a schedule that fires at the multiples of d since the
// Unix epoch. Like Every, it rounds intervals below one second up to one
// second and truncates the rest to whole seconds.
func EveryAligned(d time.Duration) AlignedSchedule {
	return EveryAlignedWithMin(d, time.Second)
}

// EveryAlignedWithMin is EveryAligned with the minimum interval rules of
// EveryWithMin.
func EveryAlignedWithMin(d, minInterval time.Duration) AlignedSchedule {
	return AlignedSchedule{Interval: EveryWithMin(d, minInterval).Delay}
}

// Next returns the first multiple of Interval after t, in t's location. If
// Interval is zero or negative, it returns t plus one second, as
// ConstantDelaySchedule does, to keep the scheduler from spinning.
func (s AlignedSchedule) Next(t time.Time) time.Time {
	d := int64(s.Interval)
	if d <= 0 {
		return t.Add(time.Second)
	}
	return time.Unix(0, (floorDiv(t.UnixNano(), d)+1)*d).In(t.Location())
}

// Prev returns the last multiple of Interval before t, in t's location.
func (s AlignedSchedule) Prev(t time.Time) time.Time {
	d := int64(s.Interval)
	if d <= 0 {
		return t.Add(-time.Second)
	}
	return time.Unix(0, floorDiv(t.UnixNano()-1, d)*d).In(t.Location())
}

// floorDiv divides rounding toward negative infinity, so times before 1970
// fall on the same grid.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
