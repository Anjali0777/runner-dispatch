package dispatch

import (
	"sync"
	"sync/atomic"
	"testing"
)

// A burst from one org must not push everyone else to the back of the line.
func TestIndexedRoundRobinsAcrossOrgs(t *testing.T) {
	in := NewInterner()
	linux := in.MustMask("linux")
	d := NewIndexed(Caps{Default: 1000})
	var id uint64
	for i := 0; i < 500; i++ { // burst arrives first
		id++
		d.Enqueue(Job{ID: id, Org: "burst", Labels: linux})
	}
	for _, org := range []string{"small-1", "small-2"} {
		id++
		d.Enqueue(Job{ID: id, Org: org, Labels: linux})
	}
	seen := map[string]int{}
	for i := 0; i < 3; i++ {
		j, _ := d.Assign(linux)
		seen[j.Org]++
	}
	if seen["small-1"] != 1 || seen["small-2"] != 1 {
		t.Fatalf("small orgs should be served within the first round, got %v", seen)
	}
	// The naive FIFO makes them wait behind all 500.
	n := NewNaive(Caps{Default: 1000})
	id = 0
	for i := 0; i < 500; i++ {
		id++
		n.Enqueue(Job{ID: id, Org: "burst", Labels: linux})
	}
	id++
	n.Enqueue(Job{ID: id, Org: "small-1", Labels: linux})
	for i := 0; i < 3; i++ {
		if j, _ := n.Assign(linux); j.Org != "burst" {
			t.Fatal("expected naive to be strictly FIFO")
		}
	}
}

// Parking must cover every pool an org uses, and unparking must restore them.
func TestIndexedParksOrgAcrossPools(t *testing.T) {
	in := NewInterner()
	linux, win := in.MustMask("linux"), in.MustMask("windows")
	both := linux | win
	d := NewIndexed(Caps{Default: 1})
	d.Enqueue(Job{ID: 1, Org: "a", Labels: linux})
	d.Enqueue(Job{ID: 2, Org: "a", Labels: win})
	d.Enqueue(Job{ID: 3, Org: "b", Labels: win})
	j, _ := d.Assign(linux)
	if j.ID != 1 {
		t.Fatalf("got %d", j.ID)
	}
	if j, _ := d.Assign(both); j.ID != 3 {
		t.Fatalf("org a is at cap; expected b's job, got %d", j.ID)
	}
	d.Complete(1)
	if j, ok := d.Assign(win); !ok || j.ID != 2 {
		t.Fatalf("org a should be unparked in the windows pool, got %v %v", j, ok)
	}
}

func TestInternerLimitsAndRoundTrip(t *testing.T) {
	in := NewInterner()
	m := in.MustMask("Linux", " GPU ", "")
	if got := in.Names(m); len(got) != 2 || got[0] != "gpu" || got[1] != "linux" {
		t.Fatalf("names = %v", got)
	}
	for i := 0; i < MaxLabels-2; i++ {
		in.MustMask(string(rune('A'+i%26)) + string(rune('a'+i/26)))
	}
	if _, err := in.Mask("one-too-many"); err == nil {
		t.Fatal("expected an error past 64 labels")
	}
}

// Many goroutines enqueue, assign and complete at once through Locked.
// Run with -race. Every job must be assigned exactly once.
func TestLockedConcurrentStress(t *testing.T) {
	in := NewInterner()
	masks := []Mask{in.MustMask("linux"), in.MustMask("windows"), in.MustMask("linux", "gpu")}
	runnerSets := []Mask{in.MustMask("linux", "gpu"), in.MustMask("windows"), in.MustMask("linux")}
	d := NewLocked(NewIndexed(Caps{Default: 8}))
	const producers, perProducer, workers = 4, 2000, 8
	total := int64(producers * perProducer)

	var assigned sync.Map
	var done atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := uint64(p*perProducer + i + 1)
				d.Enqueue(Job{ID: id, Org: string(rune('a' + id%10)), Labels: masks[id%3]})
			}
		}(p)
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := runnerSets[w%3]
			for done.Load() < total {
				j, ok := d.Assign(r)
				if !ok {
					continue
				}
				if _, dup := assigned.LoadOrStore(j.ID, w); dup {
					t.Errorf("job %d assigned twice", j.ID)
				}
				d.Complete(j.ID)
				done.Add(1)
			}
		}(w)
	}
	wg.Wait()
	if done.Load() != total || d.Queued() != 0 {
		t.Fatalf("assigned %d of %d, %d still queued", done.Load(), total, d.Queued())
	}
}

func BenchmarkAssignNoisy100k(b *testing.B) {
	for _, impl := range []struct {
		name string
		mk   func(Caps) Dispatcher
	}{{"naive", func(c Caps) Dispatcher { return NewNaive(c) }}, {"indexed", func(c Caps) Dispatcher { return NewIndexed(c) }}} {
		b.Run(impl.name, func(b *testing.B) {
			in := NewInterner()
			linux := in.MustMask("linux")
			d := impl.mk(Caps{Default: 50, PerOrg: map[string]int{"burst": 1}})
			var id uint64
			for i := 0; i < 100_000; i++ { // burst org is capped at 1: all blocked
				id++
				d.Enqueue(Job{ID: id, Org: "burst", Labels: linux})
			}
			d.Assign(linux) // burst now at its cap
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id++
				d.Enqueue(Job{ID: id, Org: "small", Labels: linux})
				j, _ := d.Assign(linux)
				d.Complete(j.ID)
			}
		})
	}
}
