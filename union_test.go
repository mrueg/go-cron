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

// runMinutes drives a scheduler in cronLoc from start through the given
// number of minutes, one minute at a time, and returns the runs of an entry
// on the union of specs.
func runMinutes(t *testing.T, cronLoc *time.Location, start time.Time, minutes int, specs ...string) int32 {
	t.Helper()
	clock := NewFakeClock(start)
	c := New(WithClock(clock), WithLocation(cronLoc))
	var runs atomic.Int32
	if _, err := c.ScheduleJob(mustUnion(t, specs...), FuncJob(func() { runs.Add(1) })); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()
	for range minutes {
		clock.BlockUntil(1)
		clock.Advance(time.Minute)
	}
	clock.BlockUntil(1)
	time.Sleep(20 * time.Millisecond)
	return runs.Load()
}

// On a DST fall-back day the scheduler skips the repeated wall-clock time.
// For a union it checks each member in that member's own zone.
func TestDSTFallBack_UnionPerMemberZone(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skip("Europe/Berlin timezone not available")
	}
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skip("America/New_York timezone not available")
	}
	for _, tc := range []struct {
		name    string
		cronLoc *time.Location
		start   time.Time
		specs   []string
		want    int32
	}{
		{
			// New York falls back on Nov 1, 2026: 01:30 EDT (05:30 UTC) and
			// 01:30 EST (06:30 UTC). The Berlin member does not fire then.
			name:    "members in different zones",
			cronLoc: time.UTC,
			start:   time.Date(2026, 11, 1, 5, 29, 0, 0, time.UTC),
			specs:   []string{"CRON_TZ=America/New_York 30 1 * * *", "CRON_TZ=Europe/Berlin 0 9 * * *"},
			want:    1,
		},
		{
			// Berlin falls back on Oct 25, 2026: 02:30 CEST (00:30 UTC) and
			// 02:30 CET (01:30 UTC). The member without a zone uses the
			// scheduler's Berlin location.
			name:    "member without a zone",
			cronLoc: berlin,
			start:   time.Date(2026, 10, 25, 0, 29, 0, 0, time.UTC),
			specs:   []string{"CRON_TZ=America/New_York 0 9 * * *", "30 2 * * *"},
			want:    1,
		},
		{
			// 01:30 UTC is Berlin's repeated 02:30 and the regular 21:30 EDT
			// of the New York member, which must still run.
			name:    "regular time of one member on another member's repeat",
			cronLoc: time.UTC,
			start:   time.Date(2026, 10, 25, 0, 29, 0, 0, time.UTC),
			specs:   []string{"CRON_TZ=Europe/Berlin 30 2 * * *", "CRON_TZ=America/New_York 30 21 * * *"},
			want:    2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := runMinutes(t, tc.cronLoc, tc.start, 121, tc.specs...); got != tc.want {
				t.Errorf("runs = %d, want %d", got, tc.want)
			}
		})
	}
}
