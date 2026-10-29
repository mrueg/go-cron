package cron

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// fallBackZones have a fall-back transition in 2026, including repeats of 30
// minutes (Lord_Howe) and two hours (Troll), one that repeats the hour before
// midnight (Santiago) and southern-hemisphere ones.
var fallBackZones = []string{
	"America/New_York", "America/Chicago", "America/Los_Angeles", "America/Havana",
	"America/Santiago", "America/Nuuk", "Atlantic/Azores",
	"Europe/Berlin", "Europe/London", "Europe/Chisinau", "Asia/Beirut", "Asia/Gaza",
	"Africa/Cairo", "Australia/Sydney", "Australia/Lord_Howe", "Pacific/Auckland",
	"Pacific/Chatham", "Pacific/Norfolk", "Pacific/Easter", "Antarctica/Troll",
}

// fallBack is one fall-back transition: at instant at the clock goes back by
// repeated.
type fallBack struct {
	at       time.Time
	repeated time.Duration
}

// fallBacksIn lists the fall-back transitions of loc in year.
func fallBacksIn(loc *time.Location, year int) []fallBack {
	var out []fallBack
	t := time.Date(year, 1, 1, 0, 0, 0, 0, loc)
	for t.Year() == year {
		_, end := t.ZoneBounds()
		if end.IsZero() {
			break
		}
		if !end.After(t) {
			// Artificial bound at a year end; step over it.
			t = t.Add(24 * time.Hour)
			continue
		}
		_, before := end.Add(-time.Second).Zone()
		_, after := end.Zone()
		if after < before && end.Year() == year {
			out = append(out, fallBack{end, time.Duration(before-after) * time.Second})
		}
		t = end
	}
	return out
}

// wallUTC returns the wall-clock time t shows, as a UTC time.
func wallUTC(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
}

// naiveMatch reports whether s fires at wall-clock time w on a calendar
// without DST. It uses Next, not the code under test.
func naiveMatch(s *SpecSchedule, w time.Time) bool {
	naive := *s
	naive.Location = time.UTC
	return naive.Next(w.Add(-time.Second)).Equal(w)
}

// guardRuns returns the times the scheduler runs sched in [from, until): each
// run is followed by the steps postDispatchScheduled takes.
func guardRuns(c *Cron, sched Schedule, from, until time.Time) []time.Time {
	e := &Entry{Schedule: sched}
	c.scheduleEntryNext(e, from)
	var runs []time.Time
	for !e.Next.IsZero() && e.Next.Before(until) {
		runs = append(runs, e.Next)
		e.Prev = e.Next
		c.recordFallBackRun(e)
		e.Next = e.Schedule.Next(e.Prev)
		c.skipFallBackRepeats(e)
	}
	return runs
}

// TestDSTFallBackSweep checks the fall-back rule in every zone above, for
// specs that run every few minutes, hourly, every few hours and at fixed
// times in the repeated hour. The oracle takes the instants Next returns
// without the guard: each one runs, except in the second copy of the clock
// when the spec matches neither the wall-clock time one repeat length earlier
// nor later. Which instants Next returns is not under test here.
func TestDSTFallBackSweep(t *testing.T) {
	var secondCopyRuns, secondCopySkips, transitions int
	for _, name := range fallBackZones {
		loc, err := time.LoadLocation(name)
		if err != nil {
			t.Logf("skip %s: %v", name, err)
			continue
		}
		fbs := fallBacksIn(loc, 2026)
		if len(fbs) == 0 {
			t.Logf("%s has no fall-back in 2026", name)
		}
		for _, fb := range fbs {
			transitions++
			first := fb.at.Add(-fb.repeated).In(loc)
			h := first.Hour()
			specs := []string{
				"*/5 * * * *", "*/20 * * * *", "0,30 * * * *", "0 * * * *", "45 * * * *",
				"0 */2 * * *", "10 */3 * * *", "0 * * * 0,6", "*/15 * * * 1-5",
				fmt.Sprintf("%d %d * * *", first.Minute(), h),
				fmt.Sprintf("0,30 %d * * *", h),
				fmt.Sprintf("*/20 %d * * *", h),
				fmt.Sprintf("*/5 %d-%d * * *", h, min(h+2, 23)),
				fmt.Sprintf("15 %d,%d * * *", h, (h+1)%24),
			}
			from, until := fb.at.Add(-3*time.Hour), fb.at.Add(3*time.Hour)
			for _, spec := range specs {
				sched, err := ParseStandard(spec)
				if err != nil {
					t.Fatal(err)
				}
				ss := sched.(*SpecSchedule)
				var want []time.Time
				for m := sched.Next(from.Add(-time.Second)); !m.IsZero() && m.Before(until); m = sched.Next(m) {
					w := wallUTC(m.In(loc))
					inSecond := !m.Before(fb.at) && m.Before(fb.at.Add(fb.repeated))
					if inSecond && !naiveMatch(ss, w.Add(-fb.repeated)) && !naiveMatch(ss, w.Add(fb.repeated)) {
						secondCopySkips++
						continue
					}
					if inSecond {
						secondCopyRuns++
					}
					want = append(want, m)
				}
				c := New(WithLocation(loc), WithLogger(DiscardLogger))
				got := guardRuns(c, sched, from.Add(-time.Second), until)
				if !slices.EqualFunc(got, want, time.Time.Equal) {
					t.Errorf("%s %q around %v: got %v, want %v", name, spec, fb.at.In(loc), got, want)
				}
			}
		}
	}
	t.Logf("%d transitions, %d second-copy runs, %d second-copy skips", transitions, secondCopyRuns, secondCopySkips)
	if transitions == 0 || secondCopyRuns == 0 || secondCopySkips == 0 {
		t.Fatalf("sweep did not exercise both branches: %d transitions, %d runs, %d skips", transitions, secondCopyRuns, secondCopySkips)
	}
}
