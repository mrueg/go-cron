// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAlignedScheduleNextPrev(t *testing.T) {
	t.Parallel()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	s := EveryAligned(time.Hour)
	for _, tc := range []struct {
		name       string
		at         time.Time
		next, prev time.Time
	}{
		{
			"inside an hour", time.Date(2026, 10, 26, 10, 17, 0, 0, time.UTC),
			time.Date(2026, 10, 26, 11, 0, 0, 0, time.UTC), time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC),
		},
		{
			"on the grid", time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 26, 11, 0, 0, 0, time.UTC), time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC),
		},
		{
			"before 1970", time.Date(1969, 12, 31, 23, 30, 0, 0, time.UTC),
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC),
		},
		{
			"with nanoseconds", time.Date(2026, 10, 26, 10, 17, 0, 500, time.UTC),
			time.Date(2026, 10, 26, 11, 0, 0, 0, time.UTC), time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC),
		},
		// Outside the int64 nanosecond range of UnixNano (1678-2262).
		{
			"year 3000", time.Date(3000, 6, 1, 10, 17, 0, 0, time.UTC),
			time.Date(3000, 6, 1, 11, 0, 0, 0, time.UTC), time.Date(3000, 6, 1, 10, 0, 0, 0, time.UTC),
		},
		{
			"year 1500", time.Date(1500, 6, 1, 10, 17, 0, 0, time.UTC),
			time.Date(1500, 6, 1, 11, 0, 0, 0, time.UTC), time.Date(1500, 6, 1, 10, 0, 0, 0, time.UTC),
		},
	} {
		if got := s.Next(tc.at); !got.Equal(tc.next) {
			t.Errorf("%s: Next = %v, want %v", tc.name, got, tc.next)
		}
		if got := s.Prev(tc.at); !got.Equal(tc.prev) {
			t.Errorf("%s: Prev = %v, want %v", tc.name, got, tc.prev)
		}
	}
	// The grid is absolute time; the result keeps the input's location.
	at := time.Date(2026, 10, 26, 6, 17, 0, 0, ny)
	if got := s.Next(at); got.Location() != ny || !got.Equal(time.Date(2026, 10, 26, 11, 0, 0, 0, time.UTC)) {
		t.Errorf("Next in New York = %v, want 11:00 UTC in America/New_York", got)
	}
}

// Two schedulers that start at different times inside one interval compute
// the same next run, which is what a restart and deduplication need.
func TestAlignedScheduleIgnoresStartTime(t *testing.T) {
	t.Parallel()
	s := EveryAligned(5 * time.Minute)
	a := s.Next(time.Date(2026, 10, 26, 10, 0, 3, 0, time.UTC))
	b := s.Next(time.Date(2026, 10, 26, 10, 4, 57, 0, time.UTC))
	if !a.Equal(b) || !a.Equal(time.Date(2026, 10, 26, 10, 5, 0, 0, time.UTC)) {
		t.Errorf("next runs = %v and %v, want both 10:05:00 UTC", a, b)
	}
}

func TestEveryAlignedRounding(t *testing.T) {
	t.Parallel()
	if got := EveryAligned(300 * time.Millisecond).Interval; got != time.Second {
		t.Errorf("EveryAligned(300ms).Interval = %v, want 1s", got)
	}
	if got := EveryAligned(90*time.Second + 400*time.Millisecond).Interval; got != 90*time.Second {
		t.Errorf("EveryAligned(90.4s).Interval = %v, want 1m30s", got)
	}
}

func TestParseEveryDescriptors(t *testing.T) {
	t.Parallel()
	plain := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	aligned := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor | AlignEvery)
	for _, tc := range []struct {
		parser      Parser
		spec        string
		wantAligned bool
	}{
		{plain, "@every 5m", false},
		{plain, "@every_start 5m", false},
		{plain, "@every_aligned 5m", true},
		{aligned, "@every 5m", true},
		{aligned, "@every_start 5m", false},
		{aligned, "@every_aligned 5m", true},
	} {
		s, err := tc.parser.Parse(tc.spec)
		if err != nil {
			t.Errorf("Parse(%q) with AlignEvery=%v: %v", tc.spec, tc.parser.options&AlignEvery != 0, err)
			continue
		}
		var interval time.Duration
		switch v := s.(type) {
		case AlignedSchedule:
			if !tc.wantAligned {
				t.Errorf("Parse(%q) with AlignEvery=%v = AlignedSchedule, want ConstantDelaySchedule", tc.spec, tc.parser.options&AlignEvery != 0)
			}
			interval = v.Interval
		case ConstantDelaySchedule:
			if tc.wantAligned {
				t.Errorf("Parse(%q) with AlignEvery=%v = ConstantDelaySchedule, want AlignedSchedule", tc.spec, tc.parser.options&AlignEvery != 0)
			}
			interval = v.Delay
		default:
			t.Errorf("Parse(%q) = %T", tc.spec, s)
		}
		if interval != 5*time.Minute {
			t.Errorf("Parse(%q) interval = %v, want 5m", tc.spec, interval)
		}
	}
	for _, bad := range []string{"@every_aligned 0.5s", "@every_start 0.5s", "@every_aligned five", "@every_aligned 1d", "@every_alignedx 5m"} {
		if _, err := plain.Parse(bad); err == nil {
			t.Errorf("Parse(%q) returned no error", bad)
		}
	}
	// AnalyzeSpec reports the interval the scheduler uses, after rounding.
	for _, spec := range []string{"@every_aligned 90.4s", "@every_start 90.4s", "@every 90.4s"} {
		if got := AnalyzeSpec(spec).Interval; got != 90*time.Second {
			t.Errorf("AnalyzeSpec(%q).Interval = %v, want 1m30s", spec, got)
		}
	}
}

// A scheduler started at 10:50 runs @every_aligned 1h at 11:00, not at
// 11:50, so restarting it every 50 minutes still lets the job run.
func TestAlignedEveryRunsDespiteRestart(t *testing.T) {
	clock := NewFakeClock(time.Date(2026, 10, 26, 10, 50, 0, 0, time.UTC))
	c := New(WithClock(clock), WithLocation(time.UTC))
	var runs atomic.Int32
	if _, err := c.AddFunc("@every_aligned 1h", func() { runs.Add(1) }); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()
	clock.BlockUntil(1)
	clock.Advance(10 * time.Minute)
	waitUntil(t, "the 11:00 run", func() bool { return runs.Load() == 1 })
}

// Two schedulers started four seconds apart share each scheduled time of
// @every_aligned, so SkipDuplicateRuns runs it once.
func TestAlignedEveryDeduplicatesAcrossInstances(t *testing.T) {
	clock := NewFakeClock(time.Date(2026, 10, 26, 10, 0, 3, 0, time.UTC))
	locker := NewMemoryLocker(clock)
	var runs atomic.Int32
	for i := range 2 {
		c := New(WithClock(clock), WithLocation(time.UTC))
		job := NewChain(SkipDuplicateRuns(locker, "report", time.Hour, DiscardLogger)).Then(FuncJob(func() { runs.Add(1) }))
		if _, err := c.AddJob("@every_aligned 5m", job); err != nil {
			t.Fatal(err)
		}
		c.Start()
		defer c.Stop()
		// Let this scheduler compute its next run before the clock moves on,
		// so the two really start four seconds apart.
		clock.BlockUntil(i + 1)
		clock.Advance(4 * time.Second)
	}
	clock.BlockUntil(2)
	clock.Advance(5 * time.Minute)
	waitUntil(t, "one run at 10:05", func() bool { return runs.Load() >= 1 })
	time.Sleep(20 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("runs at 10:05 across two schedulers = %d, want 1", got)
	}
}

// One hour after 01:00 EDT is 01:00 EST; an interval runs both. The guard
// exemption covers the value and the pointer form of both interval types.
func TestDSTFallBack_IntervalsKeepRegularRun(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	start := time.Date(2026, 11, 1, 0, 30, 0, 0, loc).UTC()
	aligned := AlignedSchedule{Interval: time.Hour}
	every := Every(time.Hour)
	for name, sched := range map[string]Schedule{
		"AlignedSchedule":        aligned,
		"*AlignedSchedule":       &aligned,
		"*ConstantDelaySchedule": &every,
	} {
		t.Run(name, func(t *testing.T) {
			clock := NewFakeClock(start)
			c := New(WithClock(clock), WithLocation(loc))
			var runs atomic.Int32
			c.Schedule(sched, FuncJob(func() { runs.Add(1) }))
			c.Start()
			defer c.Stop()
			for i := range 3 { // 01:00 EDT, 01:00 EST, 02:00 EST (aligned) or 01:30 EDT, 01:30 EST, 02:30 EST
				clock.BlockUntil(1)
				clock.Advance(time.Hour)
				waitUntil(t, "the next hourly run", func() bool { return runs.Load() == int32(i+1) })
			}
		})
	}
}

// A parser's minimum interval applies to @every_aligned as to @every.
func TestParseEveryAlignedMinInterval(t *testing.T) {
	t.Parallel()
	p := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor).WithMinEveryInterval(time.Minute)
	if _, err := p.Parse("@every_aligned 30s"); err == nil {
		t.Error("@every_aligned 30s with a 1m minimum: no error")
	}
	sub := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor).WithMinEveryInterval(100 * time.Millisecond)
	s, err := sub.Parse("@every_aligned 300ms")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.(AlignedSchedule).Interval; got != 300*time.Millisecond {
		t.Errorf("@every_aligned 300ms with a 100ms minimum: Interval = %v, want 300ms", got)
	}
}

// A zero or negative interval cannot form a grid; like ConstantDelaySchedule,
// the schedule then moves one second, so the scheduler does not spin and
// nothing divides by zero.
func TestAlignedScheduleInvalidInterval(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 26, 10, 17, 0, 0, time.UTC)
	for _, d := range []time.Duration{0, -5 * time.Minute} {
		s := AlignedSchedule{Interval: d}
		if got := s.Next(at); !got.Equal(at.Add(time.Second)) {
			t.Errorf("Interval %v: Next = %v, want %v", d, got, at.Add(time.Second))
		}
		if got := s.Prev(at); !got.Equal(at.Add(-time.Second)) {
			t.Errorf("Interval %v: Prev = %v, want %v", d, got, at.Add(-time.Second))
		}
	}
}
