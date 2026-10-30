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

// every returns n times from start, step apart in real time.
func every(start time.Time, step time.Duration, n int) []time.Time {
	runs := make([]time.Time, n)
	for i := range runs {
		runs[i] = start.Add(time.Duration(i) * step)
	}
	return runs
}

// fallBackCase is one spec driven through a fall-back night.
type fallBackCase struct {
	spec string
	from time.Time
	want []time.Time
}

// checkFallBackCases runs each case through the run loop in loc.
func checkFallBackCases(t *testing.T, loc *time.Location, tests []fallBackCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			got := fallBackRuns(t, loc, tc.from, len(tc.want), func(c *Cron) error {
				_, err := c.AddFunc(tc.spec, func() {})
				return err
			})
			if len(got) != len(tc.want) {
				t.Fatalf("got %d runs %v, want %v", len(got), got, tc.want)
			}
			for i := range tc.want {
				if !got[i].Equal(tc.want[i]) {
					t.Errorf("run %d = %v, want %v", i, got[i].In(loc), tc.want[i].In(loc))
				}
			}
		})
	}
}

// TestDSTFallBack_RunsPerRhythm pins how often a spec runs in the repeated
// fall-back hour (issue #446, ADR-016). A spec that also matches one hour
// earlier or later runs every hour or more often there, and keeps that rhythm
// through both copies. Any other spec runs each wall-clock time once that day.
func TestDSTFallBack_RunsPerRhythm(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edtZone, estZone := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	edt := func(h, m int) time.Time { return time.Date(2026, 11, 1, h, m, 0, 0, edtZone) }
	est := func(d, h, m int) time.Time { return time.Date(2026, 11, d, h, m, 0, 0, estZone) }
	noon := time.Date(2026, 10, 31, 12, 0, 0, 0, ny)
	lateEvening := time.Date(2026, 10, 31, 23, 59, 0, 0, ny)
	checkFallBackCases(t, ny, []fallBackCase{
		// Fixed times: each wall-clock time once.
		{"0,30 1 * * *", noon, []time.Time{edt(1, 0), edt(1, 30), est(2, 1, 0)}},
		{"*/20 1 * * *", noon, []time.Time{edt(1, 0), edt(1, 20), edt(1, 40), est(2, 1, 0)}},
		{"30 1 * * *", noon, []time.Time{edt(1, 30), est(2, 1, 30)}},
		{"@daily", noon, []time.Time{edt(0, 0), est(2, 0, 0)}},
		// Every 5 minutes: 5 minutes apart in real time through both copies,
		// from 00:00 EDT to 02:00 EST.
		{"*/5 0-6 * * *", lateEvening, every(edt(0, 0), 5*time.Minute, 37)},
		{"*/30 * * * *", lateEvening, every(edt(0, 0), 30*time.Minute, 7)},
		// Every hour: one hour apart in real time.
		{"0 * * * *", lateEvening, every(edt(0, 0), time.Hour, 4)},
		{"@hourly", lateEvening, every(edt(0, 0), time.Hour, 4)},
		{"0 0-6 * * *", lateEvening, every(edt(0, 0), time.Hour, 4)},
		{"30 1-3 * * *", noon, every(edt(1, 30), time.Hour, 4)},
		{"0 1,2 * * *", noon, []time.Time{edt(1, 0), est(1, 1, 0), est(1, 2, 0), est(2, 1, 0)}},
	})
}

// TestDSTFallBack_RunsPerRhythmOtherZones covers a repeated hour at 02:00
// (Berlin), a repeated half hour (Lord Howe Island) and two repeated hours
// (Antarctica/Troll).
func TestDSTFallBack_RunsPerRhythmOtherZones(t *testing.T) {
	t.Run("Berlin", func(t *testing.T) {
		berlin, err := time.LoadLocation("Europe/Berlin")
		if err != nil {
			t.Skip("Europe/Berlin timezone not available")
		}
		cest, cet := time.FixedZone("CEST", 2*3600), time.FixedZone("CET", 3600)
		from := time.Date(2026, 10, 24, 23, 30, 0, 0, berlin)
		checkFallBackCases(t, berlin, []fallBackCase{
			// Every two hours: 02:00 CET would come one hour after 02:00 CEST.
			{"0 */2 * * *", from, []time.Time{
				time.Date(2026, 10, 25, 0, 0, 0, 0, cest),
				time.Date(2026, 10, 25, 2, 0, 0, 0, cest),
				time.Date(2026, 10, 25, 4, 0, 0, 0, cet),
			}},
			{"30 2 * * *", from, []time.Time{
				time.Date(2026, 10, 25, 2, 30, 0, 0, cest),
				time.Date(2026, 10, 26, 2, 30, 0, 0, cet),
			}},
			{"0 * * * *", from, every(time.Date(2026, 10, 25, 0, 0, 0, 0, cest), time.Hour, 5)},
		})
	})
	t.Run("Lord_Howe", func(t *testing.T) {
		lh, err := time.LoadLocation("Australia/Lord_Howe")
		if err != nil {
			t.Skip("Australia/Lord_Howe timezone not available")
		}
		// 02:00 +11 becomes 01:30 +1030 on 2026-04-05.
		dst, std := time.FixedZone("+11", 11*3600), time.FixedZone("+1030", 10*3600+1800)
		from := time.Date(2026, 4, 5, 0, 59, 0, 0, lh)
		checkFallBackCases(t, lh, []fallBackCase{
			{"*/30 * * * *", from, every(time.Date(2026, 4, 5, 1, 0, 0, 0, dst), 30*time.Minute, 4)},
			{"*/10 * * * *", from, every(time.Date(2026, 4, 5, 1, 0, 0, 0, dst), 10*time.Minute, 10)},
			{"45 1 * * *", from, []time.Time{
				time.Date(2026, 4, 5, 1, 45, 0, 0, dst),
				time.Date(2026, 4, 6, 1, 45, 0, 0, std),
			}},
			// Known limit: a 20-minute rhythm does not repeat every 30
			// minutes, so 01:40 runs once.
			{"*/20 * * * *", from, []time.Time{
				time.Date(2026, 4, 5, 1, 0, 0, 0, dst),
				time.Date(2026, 4, 5, 1, 20, 0, 0, dst),
				time.Date(2026, 4, 5, 1, 40, 0, 0, dst),
				time.Date(2026, 4, 5, 2, 0, 0, 0, std),
			}},
		})
	})
	t.Run("Troll", func(t *testing.T) {
		troll, err := time.LoadLocation("Antarctica/Troll")
		if err != nil {
			t.Skip("Antarctica/Troll timezone not available")
		}
		// 03:00 +02 becomes 01:00 +00 on 2026-10-25; 01:00 to 03:00 repeats.
		from := time.Date(2026, 10, 24, 23, 30, 0, 0, troll)
		start := time.Date(2026, 10, 25, 0, 0, 0, 0, time.FixedZone("+02", 2*3600))
		checkFallBackCases(t, troll, []fallBackCase{
			{"0 */2 * * *", from, every(start, 2*time.Hour, 4)},
			{"0 * * * *", from, every(start, time.Hour, 6)},
			{"30 2 * * *", from, []time.Time{
				time.Date(2026, 10, 25, 2, 30, 0, 0, time.FixedZone("+02", 2*3600)),
				time.Date(2026, 10, 26, 2, 30, 0, 0, time.UTC),
			}},
		})
	})
}

// TestDSTFallBack_PrevWithoutRecord checks a fixed time whose first copy is
// known only from Prev (WithPrev, a restored history), not from the record of
// first-copy runs: the second copy repeats the run just fired and is skipped.
func TestDSTFallBack_PrevWithoutRecord(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	got := fallBackRuns(t, ny, time.Date(2026, 11, 1, 1, 40, 0, 0, edt), 1, func(c *Cron) error {
		_, err := c.AddFunc("30 1 * * *", func() {}, WithPrev(time.Date(2026, 11, 1, 1, 30, 0, 0, edt)))
		return err
	})
	equalRuns(t, got, []time.Time{time.Date(2026, 11, 2, 1, 30, 0, 0, est)})
}

// customSchedule is a Schedule that is not a SpecSchedule.
type customSchedule struct{ s Schedule }

func (c customSchedule) Next(t time.Time) time.Time { return c.s.Next(t) }

// TestDSTFallBack_CustomSchedule checks that a Schedule other than a
// SpecSchedule keeps the comparison with the run just fired.
func TestDSTFallBack_CustomSchedule(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	got := fallBackRuns(t, ny, time.Date(2026, 11, 1, 1, 0, 0, 0, edt), 2, func(c *Cron) error {
		s, err := ParseStandard("30 1 * * *")
		if err != nil {
			return err
		}
		c.Schedule(customSchedule{s}, FuncJob(func() {}))
		return nil
	})
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 30, 0, 0, edt),
		time.Date(2026, 11, 2, 1, 30, 0, 0, est),
	})
}

// TestDSTFallBack_UnionMemberRhythm checks the rule per union member: the
// hourly member runs in the second copy, the daily member does not.
func TestDSTFallBack_UnionMemberRhythm(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	got := fallBackRuns(t, ny, time.Date(2026, 11, 1, 0, 30, 0, 0, edt), 4, func(c *Cron) error {
		u, err := ParseUnion(NewParser(Minute|Hour|Dom|Month|Dow), "0 * * * *", "30 1 * * *")
		if err != nil {
			return err
		}
		c.Schedule(u, FuncJob(func() {}))
		return nil
	})
	equalRuns(t, got, []time.Time{
		time.Date(2026, 11, 1, 1, 0, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 30, 0, 0, edt),
		time.Date(2026, 11, 1, 1, 0, 0, 0, est),
		time.Date(2026, 11, 1, 2, 0, 0, 0, est),
	})
}

// TestRecursAcrossRepeat pins the rhythm test on wall-clock times, including
// shifts across midnight and the day fields.
func TestRecursAcrossRepeat(t *testing.T) {
	p := NewParser(Minute | Hour | Dom | Month | Dow | DomL)
	monday := time.Date(2026, 11, 2, 0, 30, 0, 0, time.UTC) // a Monday
	last := time.Date(2026, 10, 31, 1, 0, 0, 0, time.UTC)   // last day of October
	tests := []struct {
		spec     string
		at       time.Time
		repeated time.Duration
		want     bool
	}{
		{"30 1 * * 1", monday, time.Hour, true},    // one hour later, same Monday
		{"30 23 * * 0", monday, time.Hour, true},   // one hour earlier, Sunday
		{"30 0 * * 1", monday, time.Hour, false},   // only 00:30
		{"30 23 * * 1", monday, time.Hour, false},  // Sunday 23:30 is not a Monday
		{"0,30 0 * * *", monday, time.Hour, false}, // 30 minutes is not a repeat length
		{"0,30 0 * * *", monday, 30 * time.Minute, true},
		{"0 0-6 L * *", last, time.Hour, true},
		{"0 1 L * *", last, time.Hour, false},
		{"0 0 1 11 *", last.Add(22 * time.Hour), time.Hour, true}, // into November
		{"0 */2 * * *", last.Add(time.Hour), 2 * time.Hour, true},
	}
	full := NewParser(Second | Minute | Hour | Dom | Month | Dow | Year)
	newYear := time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC)
	more := []struct {
		spec     string
		at       time.Time
		repeated time.Duration
		want     bool
	}{
		{"0 30 * * * * *", monday, time.Hour, true},
		{"0 15 * * * * *", monday, time.Hour, false},  // minute 15
		{"15 30 * * * * *", monday, time.Hour, false}, // second 15
		{"0 0 0 1 1 * 2027", newYear, time.Hour, true},
		{"0 0 0 1 1 * 2026", newYear, time.Hour, false}, // 2027-01-01 is not in 2026
		{"0 0 0 1 12 * *", last.Add(22 * time.Hour), time.Hour, false},
	}
	for _, tc := range more {
		t.Run(tc.spec, func(t *testing.T) {
			s, err := full.Parse(tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			if got := recursAcrossRepeat(s.(*SpecSchedule), tc.at, tc.repeated); got != tc.want {
				t.Errorf("recursAcrossRepeat(%q, %v, %v) = %v, want %v", tc.spec, tc.at, tc.repeated, got, tc.want)
			}
		})
	}
	for _, tc := range tests {
		t.Run(tc.spec, func(t *testing.T) {
			s, err := p.Parse(tc.spec)
			if err != nil {
				t.Fatal(err)
			}
			if got := recursAcrossRepeat(s.(*SpecSchedule), tc.at, tc.repeated); got != tc.want {
				t.Errorf("recursAcrossRepeat(%q, %v, %v) = %v, want %v", tc.spec, tc.at, tc.repeated, got, tc.want)
			}
		})
	}
}

// TestSecondCopyRepeat pins the repeat length reported for the second copy.
func TestSecondCopyRepeat(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	tests := []struct {
		name string
		t    time.Time
		loc  *time.Location
		want time.Duration
	}{
		{"second copy", time.Date(2026, 11, 1, 1, 30, 0, 0, est), ny, time.Hour},
		{"start of second copy", time.Date(2026, 11, 1, 1, 0, 0, 0, est), ny, time.Hour},
		{"end of second copy", time.Date(2026, 11, 1, 2, 0, 0, 0, est), ny, 0},
		{"first copy", time.Date(2026, 11, 1, 1, 30, 0, 0, edt), ny, 0},
		{"spring-forward zone", time.Date(2026, 3, 8, 3, 30, 0, 0, ny), ny, 0},
		{"zone without transitions", time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC), time.UTC, 0},
		{"zero time", time.Time{}, ny, 0},
		{"nil location", time.Date(2026, 11, 1, 1, 30, 0, 0, est), nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := secondCopyRepeat(tc.t, tc.loc); got != tc.want {
				t.Errorf("secondCopyRepeat(%v) = %v, want %v", tc.t, got, tc.want)
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
		{"RunOnce in second copy", MissedRunOnce, at(est, 1, 1, 10), 1, []time.Time{at(est, 1, 1, 0), at(est, 1, 1, 30)}},
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

// TestInFallBackFirstCopy pins which instants count as the first copy of a
// repeated hour.
func TestInFallBackFirstCopy(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt, est := time.FixedZone("EDT", -4*3600), time.FixedZone("EST", -5*3600)
	tests := []struct {
		name string
		t    time.Time
		loc  *time.Location
		want bool
	}{
		{"first copy", time.Date(2026, 11, 1, 1, 30, 0, 0, edt), ny, true},
		{"start of first copy", time.Date(2026, 11, 1, 1, 0, 0, 0, edt), ny, true},
		{"before the repeated hour", time.Date(2026, 11, 1, 0, 59, 59, 0, edt), ny, false},
		{"second copy", time.Date(2026, 11, 1, 1, 30, 0, 0, est), ny, false},
		{"spring-forward zone", time.Date(2026, 3, 8, 1, 30, 0, 0, ny), ny, false},
		{"zone without transitions", time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC), time.UTC, false},
		{"zero time", time.Time{}, ny, false},
		{"nil location", time.Date(2026, 11, 1, 1, 30, 0, 0, edt), nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := inFallBackFirstCopy(tc.t, tc.loc); got != tc.want {
				t.Errorf("inFallBackFirstCopy(%v) = %v, want %v", tc.t, got, tc.want)
			}
		})
	}
}

// TestAddFallBackRun_DropsEarlierTransition checks that a record from an
// earlier fall-back night is replaced, so the record does not grow.
func TestAddFallBackRun_DropsEarlierTransition(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	edt := time.FixedZone("EDT", -4*3600)
	c := New(WithLocation(ny))
	sched, err := ParseStandard("0,30 1 * * *")
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2025, 11, 2, 1, 30, 0, 0, edt)
	runs := []time.Time{time.Date(2025, 11, 2, 1, 0, 0, 0, edt), last}
	this := time.Date(2026, 11, 1, 1, 0, 0, 0, edt)
	got := c.addFallBackRun(sched, runs, this)
	if len(got) != 1 || !got[0].Equal(this) {
		t.Errorf("addFallBackRun = %v, want [%v]", got, this)
	}
	// A time outside a first copy is not recorded.
	if got := c.addFallBackRun(sched, nil, time.Date(2026, 7, 1, 1, 0, 0, 0, edt)); got != nil {
		t.Errorf("addFallBackRun outside a fall-back hour = %v, want nil", got)
	}
}
