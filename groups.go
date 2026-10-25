// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"fmt"
	"sync"
)

// ConcurrencyGroups hands out MaxConcurrent and MaxConcurrentSkip wrappers
// that share a limit by name. Jobs that load the same resource, such as one
// database, can then be wrapped independently, by code that never shares a
// wrapper value, and still never exceed the group's limit together.
//
// The groups exist per ConcurrencyGroups value, not per process, so tests
// and separate schedulers do not affect each other. The limit applies within
// one process; for several processes see ADR-025.
type ConcurrencyGroups struct {
	mu   sync.Mutex
	sems map[string]chan struct{}
}

// NewConcurrencyGroups returns an empty set of named groups.
func NewConcurrencyGroups() *ConcurrencyGroups {
	return &ConcurrencyGroups{sems: make(map[string]chan struct{})}
}

// sem returns the semaphore of group name, creating it with n slots. A group
// keeps the limit it was created with, so asking for another n panics.
func (g *ConcurrencyGroups) sem(caller, name string, n int) chan struct{} {
	if n <= 0 {
		panic(fmt.Sprintf("cron: ConcurrencyGroups.%s requires n > 0", caller))
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if s, ok := g.sems[name]; ok {
		if cap(s) != n {
			panic(fmt.Sprintf("cron: concurrency group %q has limit %d, not %d", name, cap(s), n))
		}
		return s
	}
	s := make(chan struct{}, n)
	g.sems[name] = s
	return s
}

// MaxConcurrent returns a wrapper like [MaxConcurrent] whose n slots are
// shared by every wrapper of group name: when all are taken, a run waits
// for a slot or for its context to be canceled.
//
// It panics if n <= 0 or if the group already exists with another limit.
//
// Example:
//
//	groups := cron.NewConcurrencyGroups()
//	c.AddJob("0 * * * *", cron.NewChain(groups.MaxConcurrent("db", 1)).Then(backup))
//	c.AddJob("*/15 * * * *", cron.NewChain(groups.MaxConcurrent("db", 1)).Then(reindex))
func (g *ConcurrencyGroups) MaxConcurrent(name string, n int) JobWrapper {
	sem := g.sem("MaxConcurrent", name, n)
	return func(j Job) Job {
		return &maxConcurrentJob{inner: j, sem: sem}
	}
}

// MaxConcurrentSkip returns a wrapper like [MaxConcurrentSkip] whose n slots
// are shared by every wrapper of group name: when all are taken, the run is
// skipped and logged.
//
// It panics if n <= 0 or if the group already exists with another limit.
func (g *ConcurrencyGroups) MaxConcurrentSkip(logger Logger, name string, n int) JobWrapper {
	sem := g.sem("MaxConcurrentSkip", name, n)
	return func(j Job) Job {
		return &maxConcurrentSkipJob{inner: j, sem: sem, logger: logger}
	}
}
