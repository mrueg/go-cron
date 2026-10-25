// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
	"errors"
	"sync"
	"time"
)

// errNoRunInfo is logged when SkipDuplicateRuns gets a context without
// RunInfo and cannot tell which scheduled time the run is for.
var errNoRunInfo = errors.New("cron: no RunInfo in the job context")

// Locker reserves keys for coordination between scheduler instances that run
// the same jobs, for example several replicas of one service. go-cron ships
// only MemoryLocker, which coordinates within one process; a Locker for
// several processes wraps a store they share, such as Redis, etcd or a SQL
// database, and lives in user code (ADR-025).
type Locker interface {
	// TryLock reserves key until ttl has elapsed or release is called,
	// whichever comes first. It does not wait: ok is false if key is
	// already reserved. A non-nil err means the store could not answer, and
	// ok is then false. release is non-nil whenever ok is true and may be
	// called more than once; once the reservation has expired, release must
	// not remove a reservation that another caller took since.
	TryLock(ctx context.Context, key string, ttl time.Duration) (release func(), ok bool, err error)
}

// MemoryLocker is a Locker for one process. Expired keys are removed when
// TryLock next sees them.
type MemoryLocker struct {
	clock Clock
	mu    sync.Mutex
	held  map[string]memoryLock
	seq   uint64
}

type memoryLock struct {
	until time.Time
	token uint64
}

// NewMemoryLocker returns a MemoryLocker that reads the time from clock, or
// from the system clock if clock is nil.
func NewMemoryLocker(clock Clock) *MemoryLocker {
	if clock == nil {
		clock = RealClock{}
	}
	return &MemoryLocker{clock: clock, held: make(map[string]memoryLock)}
}

// TryLock implements Locker. It never returns an error.
func (m *MemoryLocker) TryLock(_ context.Context, key string, ttl time.Duration) (release func(), ok bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	if l, ok := m.held[key]; ok && now.Before(l.until) {
		return nil, false, nil
	}
	m.seq++
	token := m.seq
	m.held[key] = memoryLock{until: now.Add(ttl), token: token}
	release = func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if l, ok := m.held[key]; ok && l.token == token {
			delete(m.held, key)
		}
	}
	return release, true, nil
}

// dedupJob implements Job and JobWithContext for SkipDuplicateRuns.
type dedupJob struct {
	inner  Job
	locker Locker
	name   string
	ttl    time.Duration
	logger Logger
}

// Run implements Job. Without a scheduler context there is no RunInfo, so
// the run is skipped; see SkipDuplicateRuns.
func (d *dedupJob) Run() {
	d.RunWithContext(context.Background())
}

// RunWithContext implements JobWithContext.
func (d *dedupJob) RunWithContext(ctx context.Context) {
	info, ok := RunInfoFromContext(ctx)
	if !ok {
		d.logger.Error(errNoRunInfo, "skip", "job", d.name,
			"hint", "place SkipDuplicateRuns before wrappers that drop the context, such as RetryWithBackoff")
		return
	}
	if info.Trigger != TriggerScheduled && info.Trigger != TriggerCatchUp {
		RunJob(ctx, d.inner)
		return
	}
	key := dedupKey(d.name, info.ScheduledTime)
	_, claimed, err := d.locker.TryLock(ctx, key, d.ttl)
	if err != nil {
		d.logger.Error(err, "skip", "job", d.name, "key", key, "reason", "locker failed")
		return
	}
	if !claimed {
		d.logger.Info("skip", "job", d.name, "key", key, "reason", "run already claimed")
		return
	}
	RunJob(ctx, d.inner)
}

// dedupKey identifies one scheduled time of one job. The time is written in
// UTC, so instances in different zones produce the same key for the same
// instant.
func dedupKey(name string, scheduled time.Time) string {
	return name + "@" + scheduled.UTC().Format(time.RFC3339Nano)
}

// SkipDuplicateRuns lets one scheduled time of a job run once across all
// scheduler instances that share locker. Before a scheduled run or a
// catch-up, it reserves the key "name@<scheduled time in UTC>" for ttl. The
// instance that gets the reservation runs the job; every other instance
// skips that time. The reservation is not released after the run, so an
// instance whose clock is late still finds the time taken. Manual,
// dependency and immediate runs carry the time they were started, which no
// other instance shares, and run without a reservation.
//
// name identifies the job and must be the same on every instance. ttl must
// be longer than the largest clock difference between the instances; a
// longer ttl only keeps the key in the store longer. All instances must
// compute the same scheduled times, so they need the same spec and the same
// time zone (CRON_TZ= in the spec, or the same WithLocation).
//
// The run is skipped, and an error logged, when the locker returns an error
// and when the context carries no RunInfo. The latter happens when a wrapper
// before this one drops the context, such as RetryWithBackoff, RetryOnError
// and CircuitBreaker; place SkipDuplicateRuns before them in the chain.
//
// The guarantee is at most once: if the instance holding the reservation
// stops during the run, no other instance runs that time.
//
// name must not be empty and ttl must be positive; otherwise it panics.
//
// Example:
//
//	locker := myRedisLocker{} // implements cron.Locker
//	job := cron.NewChain(
//	    cron.SkipDuplicateRuns(locker, "nightly-report", 10*time.Minute, logger),
//	    cron.RetryWithBackoff(logger, 3, time.Second, time.Minute, 2),
//	).Then(reportJob)
//	c.ScheduleJob(sched, job, cron.WithName("nightly-report"))
func SkipDuplicateRuns(locker Locker, name string, ttl time.Duration, logger Logger) JobWrapper {
	if name == "" {
		panic("cron: SkipDuplicateRuns requires a name")
	}
	if ttl <= 0 {
		panic("cron: SkipDuplicateRuns requires ttl > 0")
	}
	return func(j Job) Job {
		return &dedupJob{inner: j, locker: locker, name: name, ttl: ttl, logger: logger}
	}
}
