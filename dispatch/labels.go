package dispatch

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Mask is a label set encoded as a bitmask, one bit per distinct label.
// Subset tests become one AND instead of a nested string comparison.
type Mask uint64

// Covers reports whether a runner with labels m can run a job needing need.
func (m Mask) Covers(need Mask) bool { return need&^m == 0 }

// MaxLabels is how many distinct labels one Interner can hold.
const MaxLabels = 64

// Interner maps label strings to bits. It is safe for concurrent use.
type Interner struct {
	mu    sync.RWMutex
	bits  map[string]Mask
	names []string
}

func NewInterner() *Interner { return &Interner{bits: map[string]Mask{}} }

// Mask returns the bitmask for a set of labels, registering new ones.
// Labels are case-insensitive, as they are in GitHub Actions.
func (in *Interner) Mask(labels ...string) (Mask, error) {
	var m Mask
	for _, l := range labels {
		l = strings.ToLower(strings.TrimSpace(l))
		if l == "" {
			continue
		}
		in.mu.RLock()
		b, ok := in.bits[l]
		in.mu.RUnlock()
		if !ok {
			in.mu.Lock()
			if b, ok = in.bits[l]; !ok {
				if len(in.names) == MaxLabels {
					in.mu.Unlock()
					return 0, fmt.Errorf("label %q: more than %d distinct labels", l, MaxLabels)
				}
				b = Mask(1) << len(in.names)
				in.bits[l] = b
				in.names = append(in.names, l)
			}
			in.mu.Unlock()
		}
		m |= b
	}
	return m, nil
}

// MustMask is Mask for tests and fixed configuration.
func (in *Interner) MustMask(labels ...string) Mask {
	m, err := in.Mask(labels...)
	if err != nil {
		panic(err)
	}
	return m
}

// Names turns a mask back into sorted label names.
func (in *Interner) Names(m Mask) []string {
	in.mu.RLock()
	defer in.mu.RUnlock()
	var out []string
	for i, n := range in.names {
		if m&(Mask(1)<<i) != 0 {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}
