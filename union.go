// Copyright (c) 2026 Netresearch DTT GmbH
// SPDX-License-Identifier: MIT.

package cron

import (
	"errors"
	"fmt"
	"time"
)

// UnionSchedule fires at every time at which any of its schedules fires. A
// time that several schedules share fires once, so a job that needs more than
// one cron expression runs once per distinct time instead of once per
// expression.
//
// A schedule that returns the zero time is exhausted (ADR-015) and no longer
// contributes; the union is exhausted only when all of its schedules are.
//
// On DST fall-back days the scheduler skips the repeated wall-clock time per
// member, in that member's own time zone (ADR-016), so members with
// different CRON_TZ= zones, or none, each run their repeated hour once.
type UnionSchedule struct {
	Schedules []Schedule
}

// ErrEmptyUnion is returned by Union and ParseUnion when no schedule is given.
var ErrEmptyUnion = errors.New("cron: union needs at least one schedule")

// Union returns a schedule that fires at the times of all given schedules.
// It returns ErrEmptyUnion if schedules is empty and an error if one of them
// is nil.
//
// Example:
//
//	// Weekdays at 09:00 and weekends at 11:00, as one entry.
//	weekdays, _ := cron.ParseStandard("0 9 * * MON-FRI")
//	weekends, _ := cron.ParseStandard("0 11 * * SAT,SUN")
//	u, err := cron.Union(weekdays, weekends)
//	if err != nil {
//	    return err
//	}
//	c.ScheduleJob(u, job, cron.WithName("report"))
func Union(schedules ...Schedule) (*UnionSchedule, error) {
	if len(schedules) == 0 {
		return nil, ErrEmptyUnion
	}
	for i, s := range schedules {
		if s == nil {
			return nil, fmt.Errorf("cron: union schedule %d is nil", i)
		}
	}
	return &UnionSchedule{Schedules: append([]Schedule(nil), schedules...)}, nil
}

// ParseUnion parses each spec with p and returns their union. The error names
// the spec that failed to parse.
func ParseUnion(p ScheduleParser, specs ...string) (*UnionSchedule, error) {
	if len(specs) == 0 {
		return nil, ErrEmptyUnion
	}
	schedules := make([]Schedule, len(specs))
	for i, spec := range specs {
		s, err := p.Parse(spec)
		if err != nil {
			return nil, fmt.Errorf("cron: union spec %d %q: %w", i, spec, err)
		}
		schedules[i] = s
	}
	return Union(schedules...)
}

// Next returns the earliest time after t at which any schedule fires, or the
// zero time if every schedule is exhausted.
func (u *UnionSchedule) Next(t time.Time) time.Time {
	var next time.Time
	for _, s := range u.Schedules {
		n := s.Next(t)
		if n.IsZero() {
			continue
		}
		if next.IsZero() || n.Before(next) {
			next = n
		}
	}
	return next
}

// Prev returns the latest time before t at which any schedule fired, or the
// zero time if none did. If a schedule does not implement ScheduleWithPrev,
// Prev cannot know that schedule's last time and returns the zero time.
func (u *UnionSchedule) Prev(t time.Time) time.Time {
	var prev time.Time
	for _, s := range u.Schedules {
		sp, ok := s.(ScheduleWithPrev)
		if !ok {
			return time.Time{}
		}
		p := sp.Prev(t)
		if p.After(prev) {
			prev = p
		}
	}
	return prev
}
