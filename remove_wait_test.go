// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// startBlockingEntry adds a triggered entry whose job ignores cancellation
// and blocks until release is closed, triggers it, and waits until it runs.
func startBlockingEntry(t *testing.T, c *Cron, name string) (EntryID, chan struct{}) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	id, err := c.AddFunc("@triggered", func() {
		once.Do(func() { close(started) })
		<-release
	}, WithName(name))
	if err != nil {
		t.Fatalf("AddFunc: %v", err)
	}
	if err := c.TriggerEntry(id); err != nil {
		t.Fatalf("TriggerEntry: %v", err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}
	return id, release
}

func requireBlocked(t *testing.T, done chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s returned (%v) while the invocation was still running", what, err)
	case <-time.After(50 * time.Millisecond):
	}
}

func requireReturned(t *testing.T, done chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not return after the invocation ended", what)
		return nil
	}
}

func TestRemoveAndWait_BlocksUntilInvocationReturns(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	id, release := startBlockingEntry(t, c, "job")

	done := make(chan error, 1)
	go func() { done <- c.RemoveAndWait(context.Background(), id) }()
	requireBlocked(t, done, "RemoveAndWait")

	if c.Entry(id).Valid() {
		t.Error("entry still scheduled while RemoveAndWait waits")
	}

	close(release)
	if err := requireReturned(t, done, "RemoveAndWait"); err != nil {
		t.Errorf("RemoveAndWait = %v, want nil", err)
	}
}

// The case of issue #428: after RemoveByName, WaitForJobByName no longer finds
// the name and returns at once; RemoveByNameAndWait waits.
func TestRemoveByNameAndWait_BlocksUntilInvocationReturns(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	_, release := startBlockingEntry(t, c, "job")

	done := make(chan error, 1)
	go func() { done <- c.RemoveByNameAndWait(context.Background(), "job") }()
	requireBlocked(t, done, "RemoveByNameAndWait")

	if c.EntryByName("job").Valid() {
		t.Error("entry still scheduled while RemoveByNameAndWait waits")
	}

	close(release)
	if err := requireReturned(t, done, "RemoveByNameAndWait"); err != nil {
		t.Errorf("RemoveByNameAndWait = %v, want nil", err)
	}
}

// Removal cancels the entry context, so a job that honors it ends and the
// wait returns.
func TestRemoveAndWait_CancelsEntryContext(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	started := make(chan struct{})
	var once sync.Once
	id, err := c.AddJob("@triggered", FuncJobWithContext(func(ctx context.Context) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	}))
	if err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	if err := c.TriggerEntry(id); err != nil {
		t.Fatalf("TriggerEntry: %v", err)
	}
	<-started

	done := make(chan error, 1)
	go func() { done <- c.RemoveAndWait(context.Background(), id) }()
	if err := requireReturned(t, done, "RemoveAndWait"); err != nil {
		t.Errorf("RemoveAndWait = %v, want nil", err)
	}
}

// When ctx ends first, the entry is removed and ctx.Err() is returned.
func TestRemoveAndWait_ContextExpires(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	id, release := startBlockingEntry(t, c, "job")
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.RemoveAndWait(ctx, id) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveAndWait kept waiting after ctx expired")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("RemoveAndWait = %v, want context.DeadlineExceeded", err)
	}
	if c.Entry(id).Valid() {
		t.Error("entry still scheduled after RemoveAndWait gave up waiting")
	}
}

func TestRemoveAndWait_IdleEntryReturnsAtOnce(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	id, err := c.AddFunc("@every 1h", func() {}, WithName("idle"))
	if err != nil {
		t.Fatalf("AddFunc: %v", err)
	}
	if err := c.RemoveAndWait(context.Background(), id); err != nil {
		t.Errorf("RemoveAndWait = %v, want nil", err)
	}
	if c.Entry(id).Valid() {
		t.Error("entry still scheduled")
	}
}

func TestRemoveAndWait_NotFound(t *testing.T) {
	c := New()
	c.Start()
	defer c.Stop()

	if err := c.RemoveAndWait(context.Background(), EntryID(999)); !errors.Is(err, ErrEntryNotFound) {
		t.Errorf("RemoveAndWait(unknown) = %v, want ErrEntryNotFound", err)
	}
	if err := c.RemoveByNameAndWait(context.Background(), "missing"); !errors.Is(err, ErrEntryNotFound) {
		t.Errorf("RemoveByNameAndWait(unknown) = %v, want ErrEntryNotFound", err)
	}
}

// A stopped scheduler removes the entry directly; the wait still covers an
// invocation that outlived Stop.
func TestRemoveAndWait_StoppedScheduler(t *testing.T) {
	c := New()
	c.Start()

	id, release := startBlockingEntry(t, c, "job")
	c.Stop()

	done := make(chan error, 1)
	go func() { done <- c.RemoveAndWait(context.Background(), id) }()
	requireBlocked(t, done, "RemoveAndWait")

	close(release)
	if err := requireReturned(t, done, "RemoveAndWait"); err != nil {
		t.Errorf("RemoveAndWait = %v, want nil", err)
	}
	if c.Entry(id).Valid() {
		t.Error("entry still present after RemoveAndWait on a stopped scheduler")
	}
}
