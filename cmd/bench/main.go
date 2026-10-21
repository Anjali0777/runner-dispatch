// Command bench measures dispatcher implementations on synthetic CI traffic.
//
//	go run ./cmd/bench                 # all scenarios, all implementations
//	go run ./cmd/bench -quick          # smaller sizes, for CI
//	go run ./cmd/bench -out results/bench.md
//
// Scenarios:
//
//	steady  queue held at a fixed depth; each step one runner goes idle,
//	        takes a job, an old job finishes and a new one arrives.
//	noisy   the same, but half the traffic is one org bursting while it is
//	        at its concurrency cap, so its jobs clog the front of the queue.
//	drain   recovery after an outage: a backlog and a fleet of runners,
//	        no new arrivals; how much dispatcher time to empty the queue.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Anjali0777/runner-dispatch/dispatch"
	"github.com/Anjali0777/runner-dispatch/workload"
)

var impls = []struct {
	name string
	mk   func(dispatch.Caps) dispatch.Dispatcher
}{
	{"naive", func(c dispatch.Caps) dispatch.Dispatcher { return dispatch.NewNaive(c) }},
}

var caps = dispatch.Caps{Default: 60, PerOrg: map[string]int{workload.NoisyOrg: 200}}

const runners = 5000 // jobs allowed to be running at once in steady/noisy

type steadyResult struct {
	p50, p99, mean time.Duration
	perSec         float64
	fairP99        int // p99 wait, in steps, for jobs not from the noisy org
	assigned       int
}

// steady holds the queue at depth and measures each Assign call.
func steady(mk func(dispatch.Caps) dispatch.Dispatcher, depth, steps int, noisy float64, budget time.Duration) steadyResult {
	g := workload.New(workload.Config{Orgs: 2000, ZipfS: 1.2, NoisyShare: noisy, Seed: 42})
	d := mk(caps)
	enqAt := map[uint64]int{}
	isNoisy := map[uint64]bool{}
	add := func(step int) {
		j := g.Job()
		enqAt[j.ID] = step
		isNoisy[j.ID] = j.Org == workload.NoisyOrg
		d.Enqueue(j)
	}
	for i := 0; i < depth; i++ {
		add(-depth + i)
	}
	var running []uint64
	lat := make([]time.Duration, 0, steps)
	var waits []int
	start := time.Now()
	for step := 0; step < steps; step++ {
		r := g.Runner()
		t0 := time.Now()
		j, ok := d.Assign(r)
		lat = append(lat, time.Since(t0))
		if ok {
			running = append(running, j.ID)
			if !isNoisy[j.ID] {
				waits = append(waits, step-enqAt[j.ID])
			}
			delete(enqAt, j.ID)
			delete(isNoisy, j.ID)
		}
		if len(running) > runners {
			d.Complete(running[0])
			running = running[1:]
		}
		for d.Queued() < depth {
			add(step)
		}
		if time.Since(start) > budget {
			break
		}
	}
	elapsed := time.Since(start)
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	sort.Ints(waits)
	var sum time.Duration
	for _, l := range lat {
		sum += l
	}
	res := steadyResult{
		p50:      lat[len(lat)/2],
		p99:      lat[len(lat)*99/100],
		mean:     sum / time.Duration(len(lat)),
		perSec:   float64(len(lat)) / elapsed.Seconds(),
		assigned: len(lat),
	}
	if len(waits) > 0 {
		res.fairP99 = waits[len(waits)*99/100]
	}
	return res
}

type drainResult struct {
	dispatchTime time.Duration
	drained      float64 // fraction of backlog assigned
	timedOut     bool
}

// drain empties a backlog with a fixed fleet. Jobs run for 1-20 rounds.
// Only time spent inside Assign/Complete counts.
func drain(mk func(dispatch.Caps) dispatch.Dispatcher, backlog, fleet int, budget time.Duration) drainResult {
	g := workload.New(workload.Config{Orgs: 2000, ZipfS: 1.2, NoisyShare: 0.4, Seed: 7})
	d := mk(caps)
	for i := 0; i < backlog; i++ {
		d.Enqueue(g.Job())
	}
	fleetLabels := make([]dispatch.Mask, fleet)
	for i := range fleetLabels {
		fleetLabels[i] = g.Runner()
	}
	slots := make([]slot, fleet)
	var spent time.Duration
	assigned := 0
	for d.Queued() > 0 {
		progress := false
		for i := range slots {
			s := &slots[i]
			if s.busy > 0 {
				s.busy--
				if s.busy == 0 {
					t0 := time.Now()
					d.Complete(s.job)
					spent += time.Since(t0)
					progress = true
				}
				continue
			}
			t0 := time.Now()
			j, ok := d.Assign(fleetLabels[i])
			spent += time.Since(t0)
			if ok {
				*s = slot{job: j.ID, busy: 1 + g.Intn(20)}
				assigned++
				progress = true
			}
		}
		if spent > budget {
			return drainResult{spent, float64(assigned) / float64(backlog), true}
		}
		if !progress && allIdle(slots) {
			break // remaining jobs need runners this fleet doesn't have
		}
	}
	return drainResult{spent, float64(assigned) / float64(backlog), false}
}

type slot struct {
	job  uint64
	busy int // rounds left
}

func allIdle(slots []slot) bool {
	for _, s := range slots {
		if s.busy > 0 {
			return false
		}
	}
	return true
}

func us(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%d ns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1f µs", float64(d.Nanoseconds())/1e3)
	default:
		return fmt.Sprintf("%.2f ms", float64(d.Nanoseconds())/1e6)
	}
}

func main() {
	quick := flag.Bool("quick", false, "smaller sizes")
	out := flag.String("out", "", "also write the markdown tables to this file")
	flag.Parse()

	depths := []int{1_000, 10_000, 100_000}
	backlogs := []int{10_000, 50_000, 200_000}
	steps, budget := 20_000, 20*time.Second
	if *quick {
		depths, backlogs, steps, budget = []int{1_000, 5_000}, []int{5_000}, 3_000, 5*time.Second
	}

	var w io.Writer = os.Stdout
	var buf strings.Builder
	if *out != "" {
		w = io.MultiWriter(os.Stdout, &buf)
	}

	for _, sc := range []struct {
		name  string
		noisy float64
	}{{"steady", 0}, {"noisy", 0.5}} {
		fmt.Fprintf(w, "\n### %s (Assign latency, queue held at depth)\n\n", sc.name)
		fmt.Fprintln(w, "| impl | depth | p50 | p99 | mean | assigns/s | p99 wait, other orgs (steps) |")
		fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|")
		for _, depth := range depths {
			for _, im := range impls {
				r := steady(im.mk, depth, steps, sc.noisy, budget)
				fmt.Fprintf(w, "| %s | %d | %s | %s | %s | %.0f | %d |\n",
					im.name, depth, us(r.p50), us(r.p99), us(r.mean), r.perSec, r.fairP99)
			}
		}
	}

	fmt.Fprintf(w, "\n### drain (dispatcher time to clear a backlog, %d runners, 40%% noisy)\n\n", 2000)
	fmt.Fprintln(w, "| impl | backlog | dispatcher time | drained |")
	fmt.Fprintln(w, "|---|---:|---:|---:|")
	for _, b := range backlogs {
		for _, im := range impls {
			r := drain(im.mk, b, 2000, budget*3)
			t := us(r.dispatchTime)
			if r.timedOut {
				t = "> " + t + " (stopped)"
			}
			fmt.Fprintf(w, "| %s | %d | %s | %.0f%% |\n", im.name, b, t, r.drained*100)
		}
	}
	if *out != "" {
		if err := os.WriteFile(*out, []byte(buf.String()), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
