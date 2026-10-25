// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// blockingJob runs until release is closed and reports its start on started.
type blockingJob struct {
	started chan struct{}
	release chan struct{}
	runs    atomic.Int32
}

func newBlockingJob() *blockingJob {
	return &blockingJob{started: make(chan struct{}, 10), release: make(chan struct{})}
}

func (j *blockingJob) Run() {
	j.runs.Add(1)
	j.started <- struct{}{}
	<-j.release
}

func waitStarted(t *testing.T, j *blockingJob) {
	t.Helper()
	select {
	case <-j.started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}
}

// Two wrappers of one group, created independently, share its slot: while one
// job runs, the other job is skipped.
func TestConcurrencyGroupsSkipSharesSlotByName(t *testing.T) {
	t.Parallel()
	g := NewConcurrencyGroups()
	backup, reindex := newBlockingJob(), newBlockingJob()
	wBackup := g.MaxConcurrentSkip(DiscardLogger, "db", 1)(backup)
	wReindex := g.MaxConcurrentSkip(DiscardLogger, "db", 1)(reindex)

	done := make(chan struct{})
	go func() { wBackup.Run(); close(done) }()
	waitStarted(t, backup)

	close(reindex.release) // would return at once if it ran
	wReindex.Run()
	if got := reindex.runs.Load(); got != 0 {
		t.Errorf("second job of the group ran while the first held the slot (%d runs)", got)
	}

	close(backup.release)
	<-done
	wReindex.Run()
	if got := reindex.runs.Load(); got != 1 {
		t.Errorf("second job runs after the slot was freed = %d, want 1", got)
	}
}

// The waiting variant starts the second job only after the first finished.
func TestConcurrencyGroupsWaitSharesSlotByName(t *testing.T) {
	t.Parallel()
	g := NewConcurrencyGroups()
	first, second := newBlockingJob(), newBlockingJob()
	w1 := g.MaxConcurrent("db", 1)(first)
	w2 := g.MaxConcurrent("db", 1)(second)

	go w1.Run()
	waitStarted(t, first)
	go w2.Run()

	select {
	case <-second.started:
		t.Fatal("second job started while the first held the group's slot")
	case <-time.After(50 * time.Millisecond):
	}
	close(first.release)
	waitStarted(t, second)
	close(second.release)
}

// A run waiting for a slot gives up when its context is canceled.
func TestConcurrencyGroupsWaitHonorsContext(t *testing.T) {
	t.Parallel()
	g := NewConcurrencyGroups()
	first, second := newBlockingJob(), newBlockingJob()
	go g.MaxConcurrent("db", 1)(first).Run()
	waitStarted(t, first)
	defer close(first.release)

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		RunJob(ctx, g.MaxConcurrent("db", 1)(second))
		close(returned)
	}()
	cancel()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting run did not return after its context was canceled")
	}
	if got := second.runs.Load(); got != 0 {
		t.Errorf("canceled run executed %d times", got)
	}
}

// Different names, and different ConcurrencyGroups values, do not share.
func TestConcurrencyGroupsAreIndependent(t *testing.T) {
	t.Parallel()
	g, other := NewConcurrencyGroups(), NewConcurrencyGroups()
	holder := newBlockingJob()
	go g.MaxConcurrentSkip(DiscardLogger, "db", 1)(holder).Run()
	waitStarted(t, holder)
	defer close(holder.release)

	for name, w := range map[string]JobWrapper{
		"other name":   g.MaxConcurrentSkip(DiscardLogger, "mail", 1),
		"other groups": other.MaxConcurrentSkip(DiscardLogger, "db", 1),
	} {
		var n atomic.Int32
		w(FuncJob(func() { n.Add(1) })).Run()
		if n.Load() != 1 {
			t.Errorf("%s: job was skipped by an unrelated group", name)
		}
	}
}

func TestConcurrencyGroupsPanics(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]func(*ConcurrencyGroups){
		"zero limit":     func(g *ConcurrencyGroups) { g.MaxConcurrent("db", 0) },
		"negative limit": func(g *ConcurrencyGroups) { g.MaxConcurrentSkip(DiscardLogger, "db", -1) },
		"limit changes": func(g *ConcurrencyGroups) {
			g.MaxConcurrent("db", 1)
			g.MaxConcurrentSkip(DiscardLogger, "db", 2)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Error("no panic")
				}
			}()
			f(NewConcurrencyGroups())
		})
	}
}
