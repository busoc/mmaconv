package options

import (
	"sort"
	"strings"
	"time"
)

type Interval struct {
	Starts time.Time
	Ends   time.Time
}

func (i Interval) IsBetween(t time.Time) bool {
	return (t.After(i.Starts) && t.Before(i.Ends)) || t.Equal(i.Starts) || t.Equal(i.Ends)
}

type Schedule struct {
	Ranges []Interval
}

func (s *Schedule) Set(file string) error {
	return nil
}

func (s *Schedule) String() string {
	return "schedule"
}

func (s *Schedule) Keep(acq time.Time) bool {
	for _, i := range s.Ranges {
		if i.IsBetween(acq) {
			return true
		}
	}
	return false
}

type Exclude struct {
	names []string
}

func (e *Exclude) String() string {
	return strings.Join(e.names, ",")
}

func (e *Exclude) Set(str string) error {
	for _, s := range strings.Split(str, ",") {
		e.names = append(e.names, strings.TrimSpace(s))
	}
	sort.Strings(e.names)
	return nil
}

func (e *Exclude) Has(file string) bool {
	if len(e.names) == 0 {
		return false
	}
	for _, n := range e.names {
		if strings.HasSuffix(file, n) {
			return true
		}
	}
	return false
}
