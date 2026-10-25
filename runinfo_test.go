// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
	"errors"
	"testing"
	"time"
)

// runInfoRecorder returns a job that sends the RunInfo of each invocation to
// the returned channel; ok=false is sent as a zero RunInfo with Trigger 0.
func runInfoRecorder() (FuncJobWithContext, chan RunInfo) {
	infos := make(chan RunInfo, 10)
	return func(ctx context.Context) {
		info, _ := RunInfoFromContext(ctx)
		infos <- info
	}, infos
}

func nextRunInfo(t *testing.T, infos chan RunInfo) RunInfo {
	t.Helper()
	select {
	case info := <-infos:
		return info
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for a job invocation")
		return RunInfo{}
	}
}

func requireRunInfo(t *testing.T, got RunInfo, trigger TriggerKind, scheduled time.Time) {
	t.Helper()
	if got.Trigger != trigger {
		t.Errorf("Trigger = %v, want %v", got.Trigger, trigger)
	}
	if !got.ScheduledTime.Equal(scheduled) {
		t.Errorf("ScheduledTime = %v, want %v", got.ScheduledTime, scheduled)
	}
}

func TestRunInfo_ScheduledFire(t *testing.T) {
	start := time.Date(2026, 1, 18, 10, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	c := New(WithClock(clock), WithParser(secondParser))
	defer c.Stop()

	job, infos := runInfoRecorder()
	if _, err := c.AddJob("0 0 * * * *", job); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	c.Start()
	clock.BlockUntil(1)
	// The job starts a little after its scheduled time; ScheduledTime is the
	// schedule's time, not the start.
	clock.Advance(time.Hour + 3*time.Second)

	requireRunInfo(t, nextRunInfo(t, infos), TriggerScheduled, start.Add(time.Hour))
}

// The first run of an entry added WithRunImmediately is not a fire of the
// schedule: it reports TriggerImmediate and the time it started. The next
// fire is the schedule's again.
func TestRunInfo_RunImmediately(t *testing.T) {
	start := time.Date(2026, 1, 18, 10, 17, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	c := New(WithClock(clock), WithParser(secondParser))
	defer c.Stop()

	job, infos := runInfoRecorder()
	if _, err := c.AddJob("0 0 * * * *", job, WithRunImmediately()); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	c.Start()
	requireRunInfo(t, nextRunInfo(t, infos), TriggerImmediate, start)

	clock.BlockUntil(1)
	clock.Advance(43 * time.Minute)
	requireRunInfo(t, nextRunInfo(t, infos), TriggerScheduled, time.Date(2026, 1, 18, 11, 0, 0, 0, time.UTC))
}

// A Prev in the future (persisted by a host whose clock ran ahead) makes the
// scheduler reschedule the run-immediately entry from its schedule. The fire
// it then gets is a fire of the schedule, not the immediate run.
func TestRunInfo_RunImmediatelyRescheduledByTimeBackwards(t *testing.T) {
	start := time.Date(2026, 1, 18, 10, 17, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	c := New(WithClock(clock), WithParser(secondParser))
	defer c.Stop()

	job, infos := runInfoRecorder()
	if _, err := c.AddJob("0 0 * * * *", job, WithRunImmediately(), WithPrev(start.Add(13*time.Minute))); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	c.Start()
	clock.BlockUntil(1)
	clock.Advance(43 * time.Minute)
	requireRunInfo(t, nextRunInfo(t, infos), TriggerScheduled, time.Date(2026, 1, 18, 11, 0, 0, 0, time.UTC))
}

func TestRunInfo_ManualTrigger(t *testing.T) {
	now := time.Date(2026, 1, 18, 10, 30, 0, 0, time.UTC)
	clock := NewFakeClock(now)
	c := New(WithClock(clock), WithParser(secondParser))
	defer c.Stop()

	job, infos := runInfoRecorder()
	if _, err := c.AddJob("0 0 * * * *", job, WithName("job")); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	c.Start()
	if err := c.TriggerEntryByName("job"); err != nil {
		t.Fatalf("TriggerEntryByName: %v", err)
	}

	requireRunInfo(t, nextRunInfo(t, infos), TriggerManual, now)
}

// A manual trigger of a workflow root is manual, and the child it starts is a
// dependency run.
func TestRunInfo_ManualWorkflowRootAndChild(t *testing.T) {
	now := time.Date(2026, 1, 18, 10, 30, 0, 0, time.UTC)
	clock := NewFakeClock(now)
	c := New(WithClock(clock), WithParser(secondParser))
	defer c.Stop()

	rootJob, rootInfos := runInfoRecorder()
	childJob, childInfos := runInfoRecorder()
	if _, err := c.AddJob("@triggered", rootJob, WithName("root")); err != nil {
		t.Fatalf("AddJob root: %v", err)
	}
	if _, err := c.AddJob("@triggered", childJob, WithName("child")); err != nil {
		t.Fatalf("AddJob child: %v", err)
	}
	if err := c.AddDependencyByName("child", "root", OnSuccess); err != nil {
		t.Fatalf("AddDependencyByName: %v", err)
	}
	c.Start()
	if err := c.TriggerEntryByName("root"); err != nil {
		t.Fatalf("TriggerEntryByName: %v", err)
	}

	requireRunInfo(t, nextRunInfo(t, rootInfos), TriggerManual, now)
	requireRunInfo(t, nextRunInfo(t, childInfos), TriggerDependency, now)
}

func TestRunInfo_ScheduledWorkflowRoot(t *testing.T) {
	start := time.Date(2026, 1, 18, 10, 0, 0, 0, time.UTC)
	clock := NewFakeClock(start)
	c := New(WithClock(clock), WithParser(secondParser))
	defer c.Stop()

	rootJob, rootInfos := runInfoRecorder()
	childJob, childInfos := runInfoRecorder()
	if _, err := c.AddJob("0 0 * * * *", rootJob, WithName("root")); err != nil {
		t.Fatalf("AddJob root: %v", err)
	}
	if _, err := c.AddJob("@triggered", childJob, WithName("child")); err != nil {
		t.Fatalf("AddJob child: %v", err)
	}
	if err := c.AddDependencyByName("child", "root", OnSuccess); err != nil {
		t.Fatalf("AddDependencyByName: %v", err)
	}
	c.Start()
	clock.BlockUntil(1)
	clock.Advance(time.Hour)

	requireRunInfo(t, nextRunInfo(t, rootInfos), TriggerScheduled, start.Add(time.Hour))
	if got := nextRunInfo(t, childInfos); got.Trigger != TriggerDependency {
		t.Errorf("child Trigger = %v, want %v", got.Trigger, TriggerDependency)
	}
}

func TestRunInfo_CatchUp(t *testing.T) {
	for _, policy := range []MissedPolicy{MissedRunOnce, MissedRunAll} {
		t.Run(policy.String(), func(t *testing.T) {
			now := time.Date(2026, 1, 18, 10, 0, 0, 0, time.UTC)
			clock := NewFakeClock(now)
			c := New(WithClock(clock), WithParser(secondParser))
			defer c.Stop()

			job, infos := runInfoRecorder()
			// Last ran at 8:00, so 9:00 was missed.
			if _, err := c.AddJob("0 0 * * * *", job,
				WithPrev(time.Date(2026, 1, 18, 8, 0, 0, 0, time.UTC)),
				WithMissedPolicy(policy),
			); err != nil {
				t.Fatalf("AddJob: %v", err)
			}
			c.Start()

			requireRunInfo(t, nextRunInfo(t, infos), TriggerCatchUp, time.Date(2026, 1, 18, 9, 0, 0, 0, time.UTC))
		})
	}
}

// The chain wrappers pass the context on, so the RunInfo reaches a job that
// runs behind them.
func TestRunInfo_ReachesJobThroughChain(t *testing.T) {
	now := time.Date(2026, 1, 18, 10, 30, 0, 0, time.UTC)
	clock := NewFakeClock(now)
	logger := DiscardLogger
	c := New(WithClock(clock), WithParser(secondParser),
		WithChain(Recover(logger), SkipIfStillRunning(logger)))
	defer c.Stop()

	job, infos := runInfoRecorder()
	if _, err := c.AddJob("@triggered", job, WithName("job")); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	c.Start()
	if err := c.TriggerEntryByName("job"); err != nil {
		t.Fatalf("TriggerEntryByName: %v", err)
	}

	requireRunInfo(t, nextRunInfo(t, infos), TriggerManual, now)
}

func TestRunInfoFromContext_NotARun(t *testing.T) {
	if info, ok := RunInfoFromContext(context.Background()); ok {
		t.Errorf("RunInfoFromContext(background) = %+v, true; want ok=false", info)
	}
}

func TestTriggerKind_StringAndValid(t *testing.T) {
	cases := []struct {
		kind  TriggerKind
		name  string
		valid bool
	}{
		{TriggerKind(0), "Unknown", false},
		{TriggerScheduled, "Scheduled", true},
		{TriggerManual, "Manual", true},
		{TriggerCatchUp, "CatchUp", true},
		{TriggerDependency, "Dependency", true},
		{TriggerImmediate, "Immediate", true},
		{TriggerKind(99), "Unknown", false},
	}
	for _, tc := range cases {
		if got := tc.kind.String(); got != tc.name {
			t.Errorf("TriggerKind(%d).String() = %q, want %q", int(tc.kind), got, tc.name)
		}
		if got := tc.kind.Valid(); got != tc.valid {
			t.Errorf("TriggerKind(%d).Valid() = %v, want %v", int(tc.kind), got, tc.valid)
		}
	}
}

// A caller-supplied RunInfo reaches the job and the OnJobStart hook in place
// of TriggerManual and the current time.
func TestTriggerEntryWithInfo_CarriesTheGivenRunInfo(t *testing.T) {
	now := time.Date(2026, 1, 18, 10, 30, 0, 0, time.UTC)
	missed := time.Date(2026, 1, 18, 2, 0, 0, 0, time.UTC)
	clock := NewFakeClock(now)
	hookTimes := make(chan time.Time, 1)
	c := New(WithClock(clock), WithParser(secondParser), WithObservability(ObservabilityHooks{
		OnJobStart: func(_ EntryID, _ string, scheduled time.Time) { hookTimes <- scheduled },
	}))
	defer c.Stop()

	job, infos := runInfoRecorder()
	if _, err := c.AddJob("0 0 2 * * *", job, WithName("nightly")); err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	c.Start()
	if err := c.TriggerEntryWithInfoByName("nightly", RunInfo{ScheduledTime: missed, Trigger: TriggerCatchUp}); err != nil {
		t.Fatalf("TriggerEntryWithInfoByName: %v", err)
	}

	requireRunInfo(t, nextRunInfo(t, infos), TriggerCatchUp, missed)
	select {
	case got := <-hookTimes:
		if !got.Equal(missed) {
			t.Errorf("OnJobStart scheduled time = %v, want %v", got, missed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnJobStart did not fire")
	}
}

func TestTriggerEntryWithInfo_WorkflowRoot(t *testing.T) {
	now := time.Date(2026, 1, 18, 10, 30, 0, 0, time.UTC)
	missed := time.Date(2026, 1, 18, 2, 0, 0, 0, time.UTC)
	c := New(WithClock(NewFakeClock(now)), WithParser(secondParser))
	defer c.Stop()

	rootJob, rootInfos := runInfoRecorder()
	childJob, childInfos := runInfoRecorder()
	root, _ := c.AddJob("0 0 2 * * *", rootJob, WithName("root"))
	if _, err := c.AddJob("@triggered", childJob, WithName("child")); err != nil {
		t.Fatalf("AddJob child: %v", err)
	}
	if err := c.AddDependencyByName("child", "root", OnSuccess); err != nil {
		t.Fatalf("AddDependencyByName: %v", err)
	}
	c.Start()
	if err := c.TriggerEntryWithInfo(root, RunInfo{ScheduledTime: missed, Trigger: TriggerCatchUp}); err != nil {
		t.Fatalf("TriggerEntryWithInfo: %v", err)
	}

	requireRunInfo(t, nextRunInfo(t, rootInfos), TriggerCatchUp, missed)
	if got := nextRunInfo(t, childInfos); got.Trigger != TriggerDependency {
		t.Errorf("child Trigger = %v, want %v", got.Trigger, TriggerDependency)
	}
}

func TestTriggerEntryWithInfo_ZeroTimeMeansNow(t *testing.T) {
	now := time.Date(2026, 1, 18, 10, 30, 0, 0, time.UTC)
	c := New(WithClock(NewFakeClock(now)), WithParser(secondParser))
	defer c.Stop()

	job, infos := runInfoRecorder()
	id, _ := c.AddJob("@triggered", job)
	c.Start()
	if err := c.TriggerEntryWithInfo(id, RunInfo{Trigger: TriggerScheduled}); err != nil {
		t.Fatalf("TriggerEntryWithInfo: %v", err)
	}

	requireRunInfo(t, nextRunInfo(t, infos), TriggerScheduled, now)
}

func TestTriggerEntryWithInfo_Errors(t *testing.T) {
	c := New()
	job, infos := runInfoRecorder()
	id, _ := c.AddJob("@triggered", job, WithName("job"))
	paused, _ := c.AddJob("@triggered", job, WithName("paused"), WithPaused())
	valid := RunInfo{Trigger: TriggerCatchUp}

	if err := c.TriggerEntryWithInfo(id, valid); !errors.Is(err, ErrNotRunning) {
		t.Errorf("before Start: err = %v, want ErrNotRunning", err)
	}
	c.Start()
	defer c.Stop()

	cases := map[string]struct {
		err  error
		want error
	}{
		"zero kind":     {c.TriggerEntryWithInfo(id, RunInfo{}), ErrInvalidRunInfo},
		"unknown kind":  {c.TriggerEntryWithInfo(id, RunInfo{Trigger: TriggerKind(99)}), ErrInvalidRunInfo},
		"unknown entry": {c.TriggerEntryWithInfo(EntryID(999), valid), ErrEntryNotFound},
		"unknown name":  {c.TriggerEntryWithInfoByName("missing", valid), ErrEntryNotFound},
		"paused entry":  {c.TriggerEntryWithInfo(paused, valid), ErrEntryPaused},
	}
	for name, tc := range cases {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, tc.err, tc.want)
		}
	}
	select {
	case info := <-infos:
		t.Errorf("a refused trigger ran the job (%+v)", info)
	case <-time.After(50 * time.Millisecond):
	}
}
