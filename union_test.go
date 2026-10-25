// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func mustUnion(t *testing.T, specs ...string) *UnionSchedule {
	t.Helper()
	u, err := ParseUnion(standardParser, specs...)
	if err != nil {
		t.Fatalf("ParseUnion(%q): %v", specs, err)
	}
	return u
}

func TestUnionScheduleNext(t *testing.T) {
	t.Parallel()
	// 2026-10-26 is a Monday.
	u := mustUnion(t, "0 9 * * MON-FRI", "0 11 * * SAT,SUN", "0 9 * * MON")
	from := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)

	want := []time.Time{
		time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC), // Monday: two specs, one run
		time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 31, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 1, 11, 0, 0, 0, time.UTC),
		time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC),
	}
	got := from
	for i, w := range want {
		got = u.Next(got)
		if !got.Equal(w) {
			t.Fatalf("run %d: Next = %v, want %v", i, got, w)
		}
	}
}

// An exhausted member returns the zero time. It must drop out of the union
// instead of being taken as the earliest time and ending the union.
func TestUnionScheduleNextSkipsExhaustedMember(t *testing.T) {
	t.Parallel()
	daily, err := ParseStandard("0 9 * * *")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	want := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	for _, order := range [][]Schedule{{TriggeredSchedule{}, daily}, {daily, TriggeredSchedule{}}} {
		u, err := Union(order...)
		if err != nil {
			t.Fatal(err)
		}
		if got := u.Next(from); !got.Equal(want) {
			t.Errorf("Next with members %T, %T = %v, want %v", order[0], order[1], got, want)
		}
	}

	all, err := Union(TriggeredSchedule{}, TriggeredSchedule{})
	if err != nil {
		t.Fatal(err)
	}
	if got := all.Next(from); !got.IsZero() {
		t.Errorf("Next of a union of exhausted schedules = %v, want the zero time", got)
	}
}

func TestUnionSchedulePrev(t *testing.T) {
	t.Parallel()
	u := mustUnion(t, "0 9 * * MON-FRI", "0 11 * * SAT,SUN")
	// Monday 2026-10-26 08:00: the last run was Sunday 11:00.
	at := time.Date(2026, 10, 26, 8, 0, 0, 0, time.UTC)
	if got, want := u.Prev(at), time.Date(2026, 10, 25, 11, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("Prev = %v, want %v", got, want)
	}
}

type nextOnlySchedule struct{}

func (nextOnlySchedule) Next(t time.Time) time.Time { return t.Add(time.Minute) }

// Prev cannot know the last time of a member without Prev, so it reports
// none rather than a time that may be too early.
func TestUnionSchedulePrevWithoutMemberPrev(t *testing.T) {
	t.Parallel()
	daily, err := ParseStandard("0 9 * * *")
	if err != nil {
		t.Fatal(err)
	}
	u, err := Union(daily, nextOnlySchedule{})
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Prev(time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)); !got.IsZero() {
		t.Errorf("Prev = %v, want the zero time", got)
	}
}

func TestUnionConstructorErrors(t *testing.T) {
	t.Parallel()
	if _, err := Union(); !errors.Is(err, ErrEmptyUnion) {
		t.Errorf("Union() error = %v, want ErrEmptyUnion", err)
	}
	if _, err := ParseUnion(standardParser); !errors.Is(err, ErrEmptyUnion) {
		t.Errorf("ParseUnion() error = %v, want ErrEmptyUnion", err)
	}
	if _, err := Union(Every(time.Minute), nil); err == nil {
		t.Error("Union with a nil schedule returned no error")
	}
	_, err := ParseUnion(standardParser, "0 9 * * *", "61 * * * *")
	if err == nil || !strings.Contains(err.Error(), `"61 * * * *"`) {
		t.Errorf("ParseUnion error = %v, want it to name the spec that failed", err)
	}
}

// Union copies its argument, so a caller reusing the slice cannot change the
// schedule of a running entry.
func TestUnionCopiesSchedules(t *testing.T) {
	t.Parallel()
	s := []Schedule{Every(time.Minute)}
	u, err := Union(s...)
	if err != nil {
		t.Fatal(err)
	}
	s[0] = Every(time.Hour)
	from := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	if got, want := u.Next(from), from.Add(time.Minute); !got.Equal(want) {
		t.Errorf("Next = %v, want %v", got, want)
	}
}

func TestUnionScheduleLocation(t *testing.T) {
	t.Parallel()
	same := mustUnion(t, "CRON_TZ=America/New_York 30 1 * * *", "CRON_TZ=America/New_York 0 9 * * *", "0 12 * * *")
	if loc := same.location(); loc == nil || loc.String() != "America/New_York" {
		t.Errorf("location of specs sharing a zone = %v, want America/New_York", loc)
	}
	mixed := mustUnion(t, "CRON_TZ=America/New_York 30 1 * * *", "CRON_TZ=Europe/Berlin 0 9 * * *")
	if loc := mixed.location(); loc != nil {
		t.Errorf("location of specs in different zones = %v, want nil", loc)
	}
	none := mustUnion(t, "30 1 * * *")
	if loc := none.location(); loc != nil {
		t.Errorf("location of specs without a zone = %v, want nil", loc)
	}
}

// The scheduler suppresses the repeated wall-clock time on DST fall-back
// days in the schedule's own zone. A union whose specs share a zone must get
// the same suppression, also when the scheduler itself runs in UTC.
func TestDSTFallBack_UnionPerScheduleTZ(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skip("America/New_York timezone not available")
	}
	// Nov 1, 2026: 2:00 AM EDT -> 1:00 AM EST. 01:29 EDT = 05:29 UTC.
	clock := NewFakeClock(time.Date(2026, 11, 1, 5, 29, 0, 0, time.UTC))
	c := New(WithClock(clock), WithLocation(time.UTC))

	u := mustUnion(t, "CRON_TZ=America/New_York 30 1 * * *", "CRON_TZ=America/New_York 0 9 * * *")
	var runs atomic.Int32
	if _, err := c.ScheduleJob(u, FuncJob(func() { runs.Add(1) })); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	clock.BlockUntil(1)
	clock.Advance(time.Minute) // 01:30 EDT
	waitUntil(t, "the first 01:30 run", func() bool { return runs.Load() == 1 })

	clock.BlockUntil(1)
	clock.Advance(2 * time.Hour) // past 01:30 EST
	time.Sleep(20 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("runs on the fall-back day = %d, want 1", got)
	}
}
