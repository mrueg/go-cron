package cron

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// fallBackRuns drives the run loop from start and returns the first n
// scheduled times of the single entry built by add.
func fallBackRuns(t *testing.T, loc *time.Location, start time.Time, n int, add func(c *Cron) error) []time.Time {
	t.Helper()
	clock := NewFakeClock(start)
	c := New(WithClock(clock), WithLocation(loc))
	if err := add(c); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	var runs []time.Time
	for range n {
		clock.BlockUntil(1)
		next := c.Entries()[0].Next
		if next.IsZero() {
			break
		}
		runs = append(runs, next)
		clock.Set(next)
		deadline := time.Now().Add(2 * time.Second)
		for c.Entries()[0].Next.Equal(next) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	return runs
}

// TestDSTFallBack_RestrictedHourRunsOnce pins issue #446: with a restricted
// hour field, every wall time in the repeated fall-back hour runs once, also
// when the spec has several of them. A spec with "*" in the hour field keeps
// the guard that compares with the run just fired (ADR-016).
func TestDSTFallBack_RestrictedHourRunsOnce(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt := func(h, m int) time.Time { return time.Date(2026, 11, 1, h, m, 0, 0, time.FixedZone("EDT", -4*3600)) }
	est := func(d, h, m int) time.Time { return time.Date(2026, 11, d, h, m, 0, 0, time.FixedZone("EST", -5*3600)) }
	start := time.Date(2026, 10, 31, 12, 0, 0, 0, ny)
	tests := []struct {
		spec string
		want []time.Time
	}{
		{"0,30 1 * * *", []time.Time{edt(1, 0), edt(1, 30), est(2, 1, 0)}},
		{"*/20 1 * * *", []time.Time{edt(1, 0), edt(1, 20), edt(1, 40), est(2, 1, 0)}},
		{"30 1-3 * * *", []time.Time{edt(1, 30), est(1, 2, 30), est(1, 3, 30)}},
		{"30 1 * * *", []time.Time{edt(1, 30), est(2, 1, 30)}},
		// Wildcard hour: unchanged.
		{"0 * * * *", []time.Time{edt(0, 0), edt(1, 0), est(1, 2, 0)}},
		{"*/30 * * * *", []time.Time{edt(0, 0), edt(0, 30), edt(1, 0), edt(1, 30), est(1, 1, 0), est(1, 1, 30)}},
	}
	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			from := start
			if tc.spec == "0 * * * *" || tc.spec == "*/30 * * * *" {
				from = time.Date(2026, 10, 31, 23, 59, 0, 0, ny)
			}
			got := fallBackRuns(t, ny, from, len(tc.want), func(c *Cron) error {
				_, err := c.AddFunc(tc.spec, func() {})
				return err
			})
			if len(got) != len(tc.want) {
				t.Fatalf("got %d runs %v, want %v", len(got), got, tc.want)
			}
			for i := range tc.want {
				if !got[i].Equal(tc.want[i]) {
					t.Errorf("run %d = %v, want %v", i, got[i].In(ny), tc.want[i].In(ny))
				}
			}
		})
	}
}

// TestDSTFallBack_StartedInRepeatedHour checks that a scheduler started in
// the second copy of the hour runs the times it never ran in the first copy.
func TestDSTFallBack_StartedInRepeatedHour(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	est := time.FixedZone("EST", -5*3600)
	start := time.Date(2026, 11, 1, 1, 10, 0, 0, est)
	got := fallBackRuns(t, ny, start, 2, func(c *Cron) error {
		_, err := c.AddFunc("0,30 1 * * *", func() {})
		return err
	})
	want := []time.Time{time.Date(2026, 11, 1, 1, 30, 0, 0, est), time.Date(2026, 11, 2, 1, 0, 0, 0, est)}
	for i := range want {
		if i >= len(got) || !got[i].Equal(want[i]) {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	}
}

// TestDSTFallBack_StartedAfterFirstCopy checks that an entry started after
// the first copy of 01:30 still runs the second copy: it never ran 01:30.
func TestDSTFallBack_StartedAfterFirstCopy(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	est := time.FixedZone("EST", -5*3600)
	start := time.Date(2026, 11, 1, 1, 40, 0, 0, time.FixedZone("EDT", -4*3600))
	got := fallBackRuns(t, ny, start, 3, func(c *Cron) error {
		_, err := c.AddFunc("0,30 1 * * *", func() {})
		return err
	})
	want := []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, est),
		time.Date(2026, 11, 1, 1, 30, 0, 0, est),
		time.Date(2026, 11, 2, 1, 0, 0, 0, est),
	}
	for i := range want {
		if i >= len(got) || !got[i].Equal(want[i]) {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	}
}

// TestDSTFallBack_RestrictedHourSeconds covers a seconds field and a TZ=
// override that differs from the cron location.
func TestDSTFallBack_RestrictedHourSeconds(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt := time.FixedZone("EDT", -4*3600)
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, ny)
	got := fallBackRuns(t, time.UTC, start, 3, func(c *Cron) error {
		sched, err := NewParser(Second | Minute | Hour | Dom | Month | Dow).Parse("TZ=America/New_York 15 0,30 1 * * *")
		if err != nil {
			return err
		}
		c.Schedule(sched, FuncJob(func() {}))
		return nil
	})
	want := []time.Time{
		time.Date(2026, 11, 1, 1, 0, 15, 0, edt),
		time.Date(2026, 11, 1, 1, 30, 15, 0, edt),
		time.Date(2026, 11, 2, 1, 0, 15, 0, time.FixedZone("EST", -5*3600)),
	}
	for i := range want {
		if i >= len(got) || !got[i].Equal(want[i]) {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	}
}

// TestDSTFallBack_UnionRestrictedHour checks the guard per union member.
func TestDSTFallBack_UnionRestrictedHour(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt := func(m int) time.Time { return time.Date(2026, 11, 1, 1, m, 0, 0, time.FixedZone("EDT", -4*3600)) }
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, ny)
	got := fallBackRuns(t, ny, start, 3, func(c *Cron) error {
		u, err := ParseUnion(NewParser(Minute|Hour|Dom|Month|Dow), "0 1 * * *", "30 1 * * *")
		if err != nil {
			return err
		}
		c.Schedule(u, FuncJob(func() {}))
		return nil
	})
	want := []time.Time{edt(0), edt(30), time.Date(2026, 11, 2, 1, 0, 0, 0, time.FixedZone("EST", -5*3600))}
	for i := range want {
		if i >= len(got) || !got[i].Equal(want[i]) {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	}
}

// stepRuns advances the clock to the entry's next time n times and returns
// the times stepped to. The entry must not be paused.
func stepRuns(t *testing.T, c *Cron, clock *FakeClock, n int) []time.Time {
	t.Helper()
	var runs []time.Time
	for range n {
		clock.BlockUntil(1)
		next := c.Entries()[0].Next
		if next.IsZero() {
			break
		}
		runs = append(runs, next)
		clock.Set(next)
		deadline := time.Now().Add(2 * time.Second)
		for c.Entries()[0].Next.Equal(next) && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	return runs
}

func equalRuns(t *testing.T, got, want []time.Time) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("runs = %v, want %v", got, want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	}
}

// TestDSTFallBack_PausedDuringFirstCopy checks that a time skipped while the
// entry was paused runs in the second copy of the hour.
func TestDSTFallBack_PausedDuringFirstCopy(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	clock := NewFakeClock(time.Date(2026, 11, 1, 0, 50, 0, 0, edt))
	c := New(WithClock(clock), WithLocation(ny))
	id, err := c.AddFunc("0,30 1 * * *", func() {})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	clock.BlockUntil(1)
	if err := c.PauseEntry(id); err != nil {
		t.Fatal(err)
	}
	// 01:00 EDT passes while paused.
	first := c.Entries()[0].Next
	clock.Set(first)
	deadline := time.Now().Add(2 * time.Second)
	for c.Entries()[0].Next.Equal(first) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clock.Set(time.Date(2026, 11, 1, 1, 10, 0, 0, edt))
	if err := c.ResumeEntry(id); err != nil {
		t.Fatal(err)
	}
	equalRuns(t, stepRuns(t, c, clock, 3), []time.Time{
		time.Date(2026, 11, 1, 1, 30, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 0, 0, 0, est),
		time.Date(2026, 11, 2, 1, 0, 0, 0, est),
	})
}

// TestDSTFallBack_UpsertSameSpec checks that re-upserting the same spec in
// the first copy of the hour (a config reload) keeps the guard.
func TestDSTFallBack_UpsertSameSpec(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	clock := NewFakeClock(time.Date(2026, 11, 1, 0, 50, 0, 0, edt))
	c := New(WithClock(clock), WithLocation(ny))
	job := FuncJob(func() {})
	if _, err := c.UpsertJob("0,30 1 * * *", job, WithName("reload")); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	got := stepRuns(t, c, clock, 2) // 01:00 and 01:30 EDT
	clock.Set(time.Date(2026, 11, 1, 1, 40, 0, 0, edt))
	if _, err := c.UpsertJob("0,30 1 * * *", job, WithName("reload")); err != nil {
		t.Fatal(err)
	}
	got = append(got, stepRuns(t, c, clock, 1)...)
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 30, 0, 0, edt),
		time.Date(2026, 11, 2, 1, 0, 0, 0, est),
	})
}

// TestDSTFallBack_AddedExactlyAtFirstCopy checks that an entry added at the
// instant of the first copy, which Next skips, runs the second copy.
func TestDSTFallBack_AddedExactlyAtFirstCopy(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	got := fallBackRuns(t, ny, time.Date(2026, 11, 1, 1, 0, 0, 0, edt), 3, func(c *Cron) error {
		_, err := c.AddFunc("0,30 1 * * *", func() {})
		return err
	})
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 30, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 0, 0, 0, est),
		time.Date(2026, 11, 2, 1, 0, 0, 0, est),
	})
}

// TestDSTFallBack_LateWake checks that times the scheduler slept through in
// the first copy of the hour run in the second copy.
func TestDSTFallBack_LateWake(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	clock := NewFakeClock(time.Date(2026, 11, 1, 0, 20, 0, 0, edt))
	c := New(WithClock(clock), WithLocation(ny))
	if _, err := c.AddFunc("0,30 0-1 * * *", func() {}); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	// The process sleeps from 00:20 to 01:40 EDT; 00:30 EDT fires late, and
	// 01:00 and 01:30 EDT are missed.
	clock.BlockUntil(1)
	due := c.Entries()[0].Next
	clock.Set(time.Date(2026, 11, 1, 1, 40, 0, 0, edt))
	deadline := time.Now().Add(2 * time.Second)
	for c.Entries()[0].Next.Equal(due) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	equalRuns(t, stepRuns(t, c, clock, 3), []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, est),
		time.Date(2026, 11, 1, 1, 30, 0, 0, est),
		time.Date(2026, 11, 2, 0, 0, 0, 0, est),
	})
}

// TestDSTFallBack_PausedMidFirstCopy checks that only the time skipped while
// paused runs in the second copy; the times that ran do not run again.
func TestDSTFallBack_PausedMidFirstCopy(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	clock := NewFakeClock(time.Date(2026, 11, 1, 0, 50, 0, 0, edt))
	c := New(WithClock(clock), WithLocation(ny))
	id, err := c.AddFunc("0,20,40 1 * * *", func() {})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	got := stepRuns(t, c, clock, 1) // 01:00 EDT
	clock.Set(time.Date(2026, 11, 1, 1, 10, 0, 0, edt))
	if err := c.PauseEntry(id); err != nil {
		t.Fatal(err)
	}
	// 01:20 EDT passes while paused.
	clock.BlockUntil(1)
	due := c.Entries()[0].Next
	clock.Set(due)
	deadline := time.Now().Add(2 * time.Second)
	for c.Entries()[0].Next.Equal(due) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clock.Set(time.Date(2026, 11, 1, 1, 30, 0, 0, edt))
	if err := c.ResumeEntry(id); err != nil {
		t.Fatal(err)
	}
	got = append(got, stepRuns(t, c, clock, 3)...)
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 40, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 20, 0, 0, est),
		time.Date(2026, 11, 2, 1, 0, 0, 0, est),
	})
}

// TestDSTFallBack_UnionMemberZones checks a union whose members run in
// different zones: the New York member's repeated hour is recorded in its own
// zone although the cron location and the first member are UTC.
func TestDSTFallBack_UnionMemberZones(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt := time.FixedZone("EDT", -4*3600)
	got := fallBackRuns(t, time.UTC, time.Date(2026, 11, 1, 0, 0, 0, 0, edt), 3, func(c *Cron) error {
		u, err := ParseUnion(NewParser(Minute|Hour|Dom|Month|Dow), "TZ=UTC 0 12 * * *", "TZ=America/New_York 0,30 1 * * *")
		if err != nil {
			return err
		}
		c.Schedule(u, FuncJob(func() {}))
		return nil
	})
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 30, 0, 0, edt),
		time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC),
	})
}

// TestDSTFallBack_PausedOverLastFirstCopyTime checks the paused branch: when
// the time skipped while paused is the last one of the first copy, the next
// time computed there must not be the second copy of a time that ran.
func TestDSTFallBack_PausedOverLastFirstCopyTime(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	clock := NewFakeClock(time.Date(2026, 11, 1, 0, 50, 0, 0, edt))
	c := New(WithClock(clock), WithLocation(ny))
	id, err := c.AddFunc("0,30 1 * * *", func() {})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	got := stepRuns(t, c, clock, 1) // 01:00 EDT
	clock.Set(time.Date(2026, 11, 1, 1, 10, 0, 0, edt))
	if err := c.PauseEntry(id); err != nil {
		t.Fatal(err)
	}
	// 01:30 EDT passes while paused.
	clock.BlockUntil(1)
	due := c.Entries()[0].Next
	clock.Set(due)
	deadline := time.Now().Add(2 * time.Second)
	for c.Entries()[0].Next.Equal(due) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	clock.Set(time.Date(2026, 11, 1, 1, 45, 0, 0, edt))
	if err := c.ResumeEntry(id); err != nil {
		t.Fatal(err)
	}
	got = append(got, stepRuns(t, c, clock, 2)...)
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 30, 0, 0, est),
		time.Date(2026, 11, 2, 1, 0, 0, 0, est),
	})
}

// scheduledTimes records the scheduled time of every run, catch-ups included.
type scheduledTimes struct {
	mu   sync.Mutex
	runs []time.Time
}

func (r *scheduledTimes) job() Job {
	return FuncJobWithContext(func(ctx context.Context) {
		info, _ := RunInfoFromContext(ctx)
		r.mu.Lock()
		r.runs = append(r.runs, info.ScheduledTime)
		r.mu.Unlock()
	})
}

// TestDSTFallBack_MissedPolicy checks that catch-up runs after a restart in
// the repeated hour count as runs of their time, and that the catch-up list
// holds each wall-clock time once.
func TestDSTFallBack_MissedPolicy(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	at := func(loc *time.Location, d, h, m int) time.Time { return time.Date(2026, 11, d, h, m, 0, 0, loc) }
	tests := []struct {
		name      string
		policy    MissedPolicy
		restart   time.Time
		scheduled int
		want      []time.Time
	}{
		{"RunOnce in first copy", MissedRunOnce, at(edt, 1, 1, 45), 2, []time.Time{at(edt, 1, 1, 30), at(est, 1, 1, 0), at(est, 2, 1, 0)}},
		{"RunAll in first copy", MissedRunAll, at(edt, 1, 1, 45), 1, []time.Time{at(edt, 1, 1, 0), at(edt, 1, 1, 30), at(est, 2, 1, 0)}},
		{"RunAll in second copy", MissedRunAll, at(est, 1, 1, 10), 1, []time.Time{at(edt, 1, 1, 0), at(edt, 1, 1, 30), at(est, 2, 1, 0)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &scheduledTimes{}
			clock := NewFakeClock(tc.restart)
			c := New(WithClock(clock), WithLocation(ny))
			if _, err := c.AddJob("0,30 1 * * *", rec.job(), WithPrev(time.Date(2026, 11, 1, 0, 30, 0, 0, ny)), WithMissedPolicy(tc.policy)); err != nil {
				t.Fatal(err)
			}
			c.Start()
			defer c.Stop()
			stepRuns(t, c, clock, tc.scheduled)
			deadline := time.Now().Add(2 * time.Second)
			for {
				rec.mu.Lock()
				n := len(rec.runs)
				rec.mu.Unlock()
				if n >= len(tc.want) || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(20 * time.Millisecond) // let an unexpected extra run show up
			rec.mu.Lock()
			got := slices.SortedFunc(slices.Values(rec.runs), time.Time.Compare)
			rec.mu.Unlock()
			equalRuns(t, got, tc.want)
		})
	}
}
