// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"math/bits"
	"time"
)

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
	if s.Interval <= 0 {
		return t.Add(time.Second)
	}
	return t.Add(s.Interval - gridOffset(t, s.Interval))
}

// Prev returns the last multiple of Interval before t, in t's location.
func (s AlignedSchedule) Prev(t time.Time) time.Time {
	if s.Interval <= 0 {
		return t.Add(-time.Second)
	}
	off := gridOffset(t, s.Interval)
	if off == 0 {
		off = s.Interval
	}
	return t.Add(-off)
}

// gridOffset returns how far t lies past the last multiple of d since the
// Unix epoch, in [0, d). It works on seconds and nanoseconds separately, so
// times outside the int64 nanosecond range of UnixNano (before 1678 or after
// 2262) still fall on the grid.
func gridOffset(t time.Time, d time.Duration) time.Duration {
	// The callers pass d > 0, floorMod returns a value in [0, d), Nanosecond
	// returns a value in [0, 1e9), and rem stays below d, so no conversion
	// below changes a value.
	n := uint64(d)                                 // #nosec G115 -- d > 0
	secRem := uint64(floorMod(t.Unix(), int64(d))) // #nosec G115 -- in [0, d)
	hi, lo := bits.Mul64(secRem, uint64(time.Second)%n)
	rem := bits.Rem64(hi, lo, n)
	rem = (rem + uint64(t.Nanosecond())%n) % n // #nosec G115 -- in [0, 1e9)
	return time.Duration(rem)                  // #nosec G115 -- below d
}

// floorMod returns a mod b in [0, b) for b > 0, also for negative a.
func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
