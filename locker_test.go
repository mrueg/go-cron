// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryLockerTryLock(t *testing.T) {
	t.Parallel()
	clock := NewFakeClock(time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC))
	m := NewMemoryLocker(clock)
	ctx := context.Background()

	release, ok, err := m.TryLock(ctx, "k", time.Minute)
	if err != nil || !ok || release == nil {
		t.Fatalf("first TryLock = (%v, %v, %v), want a reservation", release != nil, ok, err)
	}
	if _, ok, _ := m.TryLock(ctx, "k", time.Minute); ok {
		t.Fatal("second TryLock on a held key succeeded")
	}
	if _, ok, _ := m.TryLock(ctx, "other", time.Minute); !ok {
		t.Fatal("TryLock on a different key failed")
	}

	release()
	release() // a second call is allowed
	if _, ok, _ := m.TryLock(ctx, "k", time.Minute); !ok {
		t.Fatal("TryLock after release failed")
	}
}

func TestMemoryLockerExpiry(t *testing.T) {
	t.Parallel()
	clock := NewFakeClock(time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC))
	m := NewMemoryLocker(clock)
	ctx := context.Background()

	stale, _, _ := m.TryLock(ctx, "k", time.Minute)
	clock.Advance(59 * time.Second)
	if _, ok, _ := m.TryLock(ctx, "k", time.Minute); ok {
		t.Fatal("TryLock succeeded before the reservation expired")
	}
	clock.Advance(time.Second)
	if _, ok, _ := m.TryLock(ctx, "k", time.Minute); !ok {
		t.Fatal("TryLock failed after the reservation expired")
	}

	// The first holder's release must not remove the second reservation.
	stale()
	if _, ok, _ := m.TryLock(ctx, "k", time.Minute); ok {
		t.Fatal("a stale release removed a newer reservation")
	}
}

func TestDedupKeyIsUTC(t *testing.T) {
	t.Parallel()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("America/New_York timezone not available")
	}
	utc := time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC)
	if a, b := dedupKey("job", utc), dedupKey("job", utc.In(ny)); a != b {
		t.Errorf("keys for one instant differ by zone: %q and %q", a, b)
	}
	if got, want := dedupKey("job", utc), "job@2026-10-26T13:00:00Z"; got != want {
		t.Errorf("dedupKey = %q, want %q", got, want)
	}
}

type countingJob struct{ n atomic.Int32 }

func (j *countingJob) Run() { j.n.Add(1) }

func runWithInfo(j Job, info RunInfo) {
	RunJob(context.WithValue(context.Background(), runInfoContextKey{}, info), j)
}

// Two instances that share a locker run one scheduled time once, also when
// the second asks later; the next scheduled time runs again.
func TestSkipDuplicateRunsAcrossInstances(t *testing.T) {
	t.Parallel()
	clock := NewFakeClock(time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC))
	locker := NewMemoryLocker(clock)
	var a, b countingJob
	instA := SkipDuplicateRuns(locker, "report", 10*time.Minute, DiscardLogger)(&a)
	instB := SkipDuplicateRuns(locker, "report", 10*time.Minute, DiscardLogger)(&b)

	nine := RunInfo{ScheduledTime: clock.Now(), Trigger: TriggerScheduled}
	runWithInfo(instA, nine)
	clock.Advance(5 * time.Second) // B's clock is late
	runWithInfo(instB, nine)
	if got := a.n.Load() + b.n.Load(); got != 1 {
		t.Fatalf("runs for one scheduled time = %d, want 1", got)
	}

	ten := RunInfo{ScheduledTime: nine.ScheduledTime.Add(time.Hour), Trigger: TriggerScheduled}
	runWithInfo(instB, ten)
	if got := b.n.Load(); got != 1 {
		t.Fatalf("instance B runs for the next scheduled time = %d, want 1", got)
	}
}

// A catch-up claims the same key as the scheduled run it replaces; manual,
// dependency and immediate runs carry their start time and run unclaimed.
func TestSkipDuplicateRunsTriggerKinds(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		trigger TriggerKind
		claims  bool
	}{
		{TriggerScheduled, true},
		{TriggerCatchUp, true},
		{TriggerManual, false},
		{TriggerDependency, false},
		{TriggerImmediate, false},
	} {
		t.Run(tc.trigger.String(), func(t *testing.T) {
			t.Parallel()
			locker := NewMemoryLocker(NewFakeClock(at))
			var j countingJob
			w := SkipDuplicateRuns(locker, "report", time.Minute, DiscardLogger)(&j)
			info := RunInfo{ScheduledTime: at, Trigger: tc.trigger}
			runWithInfo(w, info)
			runWithInfo(w, info)
			want := int32(2)
			if tc.claims {
				want = 1
			}
			if got := j.n.Load(); got != want {
				t.Errorf("runs = %d, want %d", got, want)
			}
		})
	}
}

type failingLocker struct{}

func (failingLocker) TryLock(context.Context, string, time.Duration) (func(), bool, error) {
	return nil, false, errors.New("store down")
}

type recordingLogger struct{ errs atomic.Int32 }

func (*recordingLogger) Info(string, ...any)           {}
func (l *recordingLogger) Error(error, string, ...any) { l.errs.Add(1) }

// The wrapper fails closed: when the locker errors or no RunInfo tells it the
// scheduled time, it skips the run and logs an error.
func TestSkipDuplicateRunsFailsClosed(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)

	var j countingJob
	log := &recordingLogger{}
	runWithInfo(SkipDuplicateRuns(failingLocker{}, "report", time.Minute, log)(&j),
		RunInfo{ScheduledTime: at, Trigger: TriggerScheduled})
	if j.n.Load() != 0 || log.errs.Load() != 1 {
		t.Errorf("locker error: runs = %d, logged errors = %d, want 0 and 1", j.n.Load(), log.errs.Load())
	}

	var k countingJob
	log = &recordingLogger{}
	SkipDuplicateRuns(NewMemoryLocker(nil), "report", time.Minute, log)(&k).Run()
	if k.n.Load() != 0 || log.errs.Load() != 1 {
		t.Errorf("no RunInfo: runs = %d, logged errors = %d, want 0 and 1", k.n.Load(), log.errs.Load())
	}
}

// The scheduler puts RunInfo into the context, so the wrapper works on real
// fires: two schedulers on one locker run each scheduled time once.
func TestSkipDuplicateRunsWithScheduler(t *testing.T) {
	clock := NewFakeClock(time.Date(2026, 10, 26, 8, 59, 30, 0, time.UTC))
	locker := NewMemoryLocker(clock)
	var runs atomic.Int32
	job := FuncJob(func() { runs.Add(1) })

	for range 2 {
		c := New(WithClock(clock), WithLocation(time.UTC))
		if _, err := c.AddJob("0 9 * * *", NewChain(SkipDuplicateRuns(locker, "report", time.Hour, DiscardLogger)).Then(job)); err != nil {
			t.Fatal(err)
		}
		c.Start()
		defer c.Stop()
	}
	clock.BlockUntil(2)
	clock.Advance(30 * time.Second)
	waitUntil(t, "one run at 09:00", func() bool { return runs.Load() >= 1 })
	time.Sleep(20 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("runs at 09:00 across two schedulers = %d, want 1", got)
	}
}

// SkipDuplicateRuns never releases its keys, so every scheduled time leaves
// one behind. TryLock must remove the expired ones, or a long-running process
// keeps one key per run forever.
func TestMemoryLockerRemovesExpiredKeys(t *testing.T) {
	t.Parallel()
	clock := NewFakeClock(time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC))
	m := NewMemoryLocker(clock)
	for i := range 1000 {
		if _, ok, _ := m.TryLock(context.Background(), dedupKey("job", clock.Now()), time.Minute); !ok {
			t.Fatalf("TryLock %d failed", i)
		}
		clock.Advance(time.Minute)
	}
	m.mu.Lock()
	n := len(m.held)
	m.mu.Unlock()
	if n > minSweep {
		t.Errorf("keys held after 1000 expired reservations = %d, want at most %d", n, minSweep)
	}
}

// A workflow parent that another instance already ran counts as skipped, so
// its dependents run only on the instance that ran the parent.
func TestSkipDuplicateRunsWorkflowChildRunsOnce(t *testing.T) {
	clock := NewFakeClock(time.Date(2026, 10, 26, 8, 59, 30, 0, time.UTC))
	locker := NewMemoryLocker(clock)
	var parents, children atomic.Int32

	for range 2 {
		c := New(WithClock(clock), WithLocation(time.UTC))
		c.Start()
		defer c.Stop()
		parent, err := c.AddJob("0 9 * * *", NewChain(SkipDuplicateRuns(locker, "parent", time.Hour, DiscardLogger)).
			Then(FuncJob(func() { parents.Add(1) })))
		if err != nil {
			t.Fatal(err)
		}
		child, err := c.AddFunc("@triggered", func() { children.Add(1) })
		if err != nil {
			t.Fatal(err)
		}
		if err := c.AddDependency(child, parent, OnSuccess); err != nil {
			t.Fatal(err)
		}
	}
	clock.BlockUntil(2)
	clock.Advance(30 * time.Second)
	waitUntil(t, "the parent and its child", func() bool { return parents.Load() >= 1 && children.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if p, ch := parents.Load(), children.Load(); p != 1 || ch != 1 {
		t.Errorf("parent runs = %d, child runs = %d across two instances, want 1 and 1", p, ch)
	}
}

func TestSkipDuplicateRunsPanicsOnBadArguments(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]func(){
		"empty name": func() { SkipDuplicateRuns(NewMemoryLocker(nil), "", time.Minute, DiscardLogger) },
		"zero ttl":   func() { SkipDuplicateRuns(NewMemoryLocker(nil), "job", 0, DiscardLogger) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			f()
		})
	}
}

// While one job holds the resource key, a run of another job on that key is
// skipped and marked as skipped for a workflow; once the first run ends and
// releases the key, the other job runs.
func TestSkipWhileLockedExcludesByKey(t *testing.T) {
	t.Parallel()
	locker := NewMemoryLocker(NewFakeClock(time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)))
	started, release := make(chan struct{}), make(chan struct{})
	backup := SkipWhileLocked(locker, "db", time.Hour, DiscardLogger)(FuncJob(func() {
		close(started)
		<-release
	}))
	var reindexRuns atomic.Int32
	reindex := SkipWhileLocked(locker, "db", time.Hour, DiscardLogger)(FuncJob(func() { reindexRuns.Add(1) }))

	done := make(chan struct{})
	go func() { backup.Run(); close(done) }()
	<-started

	var skipped atomic.Bool
	RunJob(context.WithValue(context.Background(), runSkippedKey{}, &skipped), reindex)
	if reindexRuns.Load() != 0 || !skipped.Load() {
		t.Errorf("while the key was held: runs = %d, marked skipped = %v, want 0 and true", reindexRuns.Load(), skipped.Load())
	}

	close(release)
	<-done
	reindex.Run()
	if got := reindexRuns.Load(); got != 1 {
		t.Errorf("runs after the key was released = %d, want 1", got)
	}
}

// A panicking job still releases the key, so the resource does not stay
// blocked until the ttl expires.
func TestSkipWhileLockedReleasesOnPanic(t *testing.T) {
	t.Parallel()
	locker := NewMemoryLocker(NewFakeClock(time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)))
	w := SkipWhileLocked(locker, "db", time.Hour, DiscardLogger)
	func() {
		defer func() { _ = recover() }()
		w(FuncJob(func() { panic("job failed") })).Run()
	}()
	var runs atomic.Int32
	w(FuncJob(func() { runs.Add(1) })).Run()
	if got := runs.Load(); got != 1 {
		t.Errorf("runs after a panicking run = %d, want 1", got)
	}
}

func TestSkipWhileLockedFailsClosed(t *testing.T) {
	t.Parallel()
	var runs atomic.Int32
	var skipped atomic.Bool
	log := &recordingLogger{}
	ctx := context.WithValue(context.Background(), runSkippedKey{}, &skipped)
	RunJob(ctx, SkipWhileLocked(failingLocker{}, "db", time.Hour, log)(FuncJob(func() { runs.Add(1) })))
	if runs.Load() != 0 || log.errs.Load() != 1 || !skipped.Load() {
		t.Errorf("locker error: runs = %d, logged errors = %d, marked skipped = %v, want 0, 1 and true",
			runs.Load(), log.errs.Load(), skipped.Load())
	}
}

// infoJob records the RunInfo of the context it runs with.
type infoJob struct {
	ran  bool
	info RunInfo
	ok   bool
}

func (j *infoJob) Run() { j.RunWithContext(context.Background()) }

func (j *infoJob) RunWithContext(ctx context.Context) {
	j.ran = true
	j.info, j.ok = RunInfoFromContext(ctx)
}

// The inner job gets the run's context, so a context-aware job or wrapper
// inside keeps RunInfo and the entry's cancellation.
func TestSkipWhileLockedPassesContext(t *testing.T) {
	t.Parallel()
	info := RunInfo{ScheduledTime: time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC), Trigger: TriggerScheduled}
	inner := &infoJob{}
	runWithInfo(SkipWhileLocked(NewMemoryLocker(nil), "db", time.Hour, DiscardLogger)(inner), info)
	if !inner.ran {
		t.Fatal("inner job did not run")
	}
	if !inner.ok || inner.info != info {
		t.Errorf("inner job's RunInfo = %+v, %v, want %+v, true", inner.info, inner.ok, info)
	}
}

func TestSkipWhileLockedPanicsOnBadArguments(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]func(){
		"empty key": func() { SkipWhileLocked(NewMemoryLocker(nil), "", time.Minute, DiscardLogger) },
		"zero ttl":  func() { SkipWhileLocked(NewMemoryLocker(nil), "db", 0, DiscardLogger) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			f()
		})
	}
}
