// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
	"time"
)

// TriggerKind says what started an invocation of an entry.
type TriggerKind int

const (
	// TriggerScheduled is a fire of the entry's schedule.
	TriggerScheduled TriggerKind = iota + 1

	// TriggerManual is a run started by TriggerEntry or TriggerEntryByName.
	// Its scheduled time is the time of the call.
	TriggerManual

	// TriggerCatchUp is a run for a missed scheduled time, started by the
	// entry's MissedPolicy when the entry is added or the scheduler starts.
	TriggerCatchUp

	// TriggerDependency is a run of a workflow child, started because its
	// parents reached the required results. Its scheduled time is the time the
	// child was started.
	TriggerDependency

	// TriggerImmediate is the first run of an entry added WithRunImmediately,
	// started when the entry is added or the scheduler starts rather than at a
	// time of its schedule. Its scheduled time is the time it was started.
	TriggerImmediate
)

// String returns a human-readable representation of the TriggerKind.
func (k TriggerKind) String() string {
	switch k {
	case TriggerScheduled:
		return "Scheduled"
	case TriggerManual:
		return "Manual"
	case TriggerCatchUp:
		return "CatchUp"
	case TriggerDependency:
		return "Dependency"
	case TriggerImmediate:
		return "Immediate"
	default:
		return "Unknown"
	}
}

// Valid returns true if the kind is a known value.
func (k TriggerKind) Valid() bool {
	return k >= TriggerScheduled && k <= TriggerImmediate
}

// RunInfo describes one invocation of an entry. The scheduler puts it into
// the context that a JobWithContext receives.
type RunInfo struct {
	// ScheduledTime is the time the invocation was scheduled for. For a
	// scheduled fire and a catch-up it is the schedule's time, not the time
	// the job started; for a manual, dependency or immediate run it is the
	// time the run was started.
	ScheduledTime time.Time

	// Trigger says what started the invocation.
	Trigger TriggerKind
}

type runInfoContextKey struct{}

// triggerRequest asks the run loop to start an entry now. info is nil for a
// plain manual trigger, which runs with TriggerManual and the current time.
type triggerRequest struct {
	id   EntryID
	info *RunInfo
}

// RunInfoFromContext returns the RunInfo of the invocation that ctx belongs
// to. ok is false if ctx was not created by the scheduler for a job run.
//
//	func (j *myJob) RunWithContext(ctx context.Context) {
//	    if info, ok := cron.RunInfoFromContext(ctx); ok && info.Trigger == cron.TriggerScheduled {
//	        recordFire(info.ScheduledTime)
//	    }
//	}
func RunInfoFromContext(ctx context.Context) (RunInfo, bool) {
	info, ok := ctx.Value(runInfoContextKey{}).(RunInfo)
	return info, ok
}
