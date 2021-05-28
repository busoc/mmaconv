package options

import (
	"sort"
	"strings"
	"time"

	"github.com/midbel/toml"
)

type Interval struct {
	Starts time.Time
	Ends   time.Time
}

func (i Interval) IsBetween(t time.Time) bool {
	return (t.After(i.Starts) && t.Before(i.Ends)) || t.Equal(i.Starts) || t.Equal(i.Ends)
}

func (i Interval) isValid() bool {
	if i.Starts.IsZero() || i.Ends.IsZero() {
		return false
	}
	return i.Starts.Equal(i.Ends) || i.Starts.Before(i.Ends)
}

type Schedule struct {
	Ranges []Interval `toml:"range"`
}

func (s *Schedule) Set(file string) error {
	return toml.DecodeFile(file, s)
}

func (s *Schedule) String() string {
	return "schedule"
}

func (s *Schedule) Keep(acq time.Time) bool {
	if len(s.Ranges) == 0 {
		return true
	}
	for _, i := range s.Ranges {
		if !i.isValid() {
			continue
		}
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
