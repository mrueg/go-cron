// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
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
