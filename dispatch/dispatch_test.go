package dispatch

import (
	"fmt"
	"math/rand"
	"testing"
)

// impls lists every Dispatcher under test; each test runs against all of them.
var impls = map[string]func(Caps) Dispatcher{
	"naive": func(c Caps) Dispatcher { return NewNaive(c) },
}

func forEach(t *testing.T, fn func(t *testing.T, mk func(Caps) Dispatcher)) {
	for name, mk := range impls {
		t.Run(name, func(t *testing.T) { fn(t, mk) })
	}
}

var lab = NewInterner()

func TestRunnerMustHaveEveryLabel(t *testing.T) {
	forEach(t, func(t *testing.T, mk func(Caps) Dispatcher) {
		d := mk(Caps{Default: 10})
		d.Enqueue(Job{ID: 1, Org: "a", Labels: lab.MustMask("linux", "gpu")})
		if _, ok := d.Assign(lab.MustMask("linux")); ok {
			t.Fatal("gpu job assigned to a runner without gpu")
		}
		j, ok := d.Assign(lab.MustMask("linux", "gpu", "x64"))
		if !ok || j.ID != 1 {
			t.Fatalf("superset runner should take the job, got %v %v", j, ok)
		}
	})
}

func TestOrgCapIsRespectedAndReleased(t *testing.T) {
	forEach(t, func(t *testing.T, mk func(Caps) Dispatcher) {
		d := mk(Caps{Default: 2})
		linux := lab.MustMask("linux")
		for i := uint64(1); i <= 3; i++ {
			d.Enqueue(Job{ID: i, Org: "a", Labels: linux})
		}
		a, _ := d.Assign(linux)
		d.Assign(linux)
		if _, ok := d.Assign(linux); ok {
			t.Fatal("third job ran while org a was at its cap of 2")
		}
		d.Complete(a.ID)
		if j, ok := d.Assign(linux); !ok || j.ID != 3 {
			t.Fatalf("finishing a job should free a slot, got %v %v", j, ok)
		}
	})
}

func TestSingleOrgIsFIFO(t *testing.T) {
	forEach(t, func(t *testing.T, mk func(Caps) Dispatcher) {
		d := mk(Caps{Default: 100})
		linux := lab.MustMask("linux")
		for i := uint64(1); i <= 20; i++ {
			d.Enqueue(Job{ID: i, Org: "a", Labels: linux})
		}
		for want := uint64(1); want <= 20; want++ {
			if j, _ := d.Assign(linux); j.ID != want {
				t.Fatalf("got job %d, want %d", j.ID, want)
			}
		}
	})
}

func TestEmptyAndUnknownCompleteAreHarmless(t *testing.T) {
	forEach(t, func(t *testing.T, mk func(Caps) Dispatcher) {
		d := mk(Caps{Default: 1})
		if _, ok := d.Assign(lab.MustMask("linux")); ok {
			t.Fatal("assigned from an empty queue")
		}
		d.Complete(999) // never ran
		if d.Queued() != 0 {
			t.Fatal("queue should be empty")
		}
	})
}

// model is a brute-force reference: the set of queued and running jobs.
type model struct {
	caps    Caps
	queued  map[uint64]Job
	running map[uint64]Job
	perOrg  map[string]int
}

func (m *model) eligible(r Mask) bool {
	for _, j := range m.queued {
		if r.Covers(j.Labels) && m.perOrg[j.Org] < m.caps.Of(j.Org) {
			return true
		}
	}
	return false
}

// TestRandomOperationsKeepInvariants is a property test: thousands of random
// enqueue/assign/complete sequences, checked against the model after every step.
//
//   - every assigned job was queued, fits the runner, and its org was under cap
//   - no job is assigned twice or lost
//   - work-conserving: Assign only says "nothing" when nothing is eligible
func TestRandomOperationsKeepInvariants(t *testing.T) {
	in := NewInterner()
	pool := []Mask{
		in.MustMask("linux"), in.MustMask("linux", "x64"), in.MustMask("windows"),
		in.MustMask("linux", "gpu"), in.MustMask("macos", "arm64"),
	}
	runners := []Mask{
		in.MustMask("linux", "x64"), in.MustMask("linux", "x64", "gpu"),
		in.MustMask("windows", "x64"), in.MustMask("macos", "arm64"), in.MustMask("linux"),
	}
	orgs := []string{"a", "b", "c", "d", "e", "f"}
	forEach(t, func(t *testing.T, mk func(Caps) Dispatcher) {
		for seed := int64(0); seed < 300; seed++ {
			rng := rand.New(rand.NewSource(seed))
			caps := Caps{Default: 1 + rng.Intn(4), PerOrg: map[string]int{"a": 1 + rng.Intn(2)}}
			d := mk(caps)
			m := &model{caps: caps, queued: map[uint64]Job{}, running: map[uint64]Job{}, perOrg: map[string]int{}}
			var next uint64
			for step := 0; step < 400; step++ {
				switch op := rng.Intn(10); {
				case op < 4:
					next++
					j := Job{ID: next, Org: orgs[rng.Intn(len(orgs))], Labels: pool[rng.Intn(len(pool))]}
					d.Enqueue(j)
					m.queued[j.ID] = j
				case op < 8:
					r := runners[rng.Intn(len(runners))]
					want := m.eligible(r)
					j, ok := d.Assign(r)
					ctx := fmt.Sprintf("seed %d step %d", seed, step)
					if ok != want {
						t.Fatalf("%s: Assign=%v but eligible job exists=%v", ctx, ok, want)
					}
					if !ok {
						continue
					}
					q, was := m.queued[j.ID]
					switch {
					case !was:
						t.Fatalf("%s: job %d was not queued (double assign?)", ctx, j.ID)
					case !r.Covers(q.Labels):
						t.Fatalf("%s: job %d labels not covered by runner", ctx, j.ID)
					case m.perOrg[q.Org] >= caps.Of(q.Org):
						t.Fatalf("%s: org %s over cap", ctx, q.Org)
					}
					delete(m.queued, j.ID)
					m.running[j.ID] = q
					m.perOrg[q.Org]++
				default:
					for id, j := range m.running {
						d.Complete(id)
						delete(m.running, id)
						m.perOrg[j.Org]--
						break
					}
				}
				if d.Queued() != len(m.queued) {
					t.Fatalf("seed %d: Queued()=%d, model has %d", seed, d.Queued(), len(m.queued))
				}
			}
		}
	})
}
