// Package workload generates synthetic CI traffic shaped like a hosted CI
// service: many orgs with a long-tailed (Zipf) share of jobs, a handful of
// common runner configurations, and optionally one "noisy" org whose burst
// fills the queue while it sits at its concurrency cap.
package workload

import (
	"math/rand"
	"strconv"

	"github.com/Anjali0777/runner-dispatch/dispatch"
)

// Config describes one traffic mix.
type Config struct {
	Orgs       int     // number of ordinary orgs
	ZipfS      float64 // skew of jobs across orgs (>1)
	NoisyShare float64 // fraction of jobs from the noisy org (0 = none)
	Seed       int64
}

// Gen produces jobs and runner label sets.
type Gen struct {
	cfg     Config
	rng     *rand.Rand
	zipf    *rand.Zipf
	jobMix  []weighted
	runMix  []weighted
	nextID  uint64
	Labels  *dispatch.Interner
	NoisyID string
}

type weighted struct {
	mask dispatch.Mask
	w    float64
}

// NoisyOrg is the name used for the bursting org.
const NoisyOrg = "monorepo-co"

func New(cfg Config) *Gen {
	in := dispatch.NewInterner()
	rng := rand.New(rand.NewSource(cfg.Seed))
	g := &Gen{
		cfg:     cfg,
		rng:     rng,
		zipf:    rand.NewZipf(rng, cfg.ZipfS, 1, uint64(cfg.Orgs-1)),
		Labels:  in,
		NoisyID: NoisyOrg,
	}
	// What jobs ask for (runs-on), roughly hosted-CI shaped.
	g.jobMix = []weighted{
		{in.MustMask("ubuntu-latest"), 0.62},
		{in.MustMask("ubuntu-latest", "4-core"), 0.10},
		{in.MustMask("windows-latest"), 0.10},
		{in.MustMask("macos-latest"), 0.07},
		{in.MustMask("ubuntu-latest", "gpu"), 0.03},
		{in.MustMask("self-hosted", "linux", "x64"), 0.06},
		{in.MustMask("self-hosted", "linux", "arm64"), 0.02},
	}
	// Which runner configurations become idle.
	g.runMix = []weighted{
		{in.MustMask("ubuntu-latest"), 0.55},
		{in.MustMask("ubuntu-latest", "4-core"), 0.12},
		{in.MustMask("windows-latest"), 0.12},
		{in.MustMask("macos-latest"), 0.07},
		{in.MustMask("ubuntu-latest", "4-core", "gpu"), 0.04},
		{in.MustMask("self-hosted", "linux", "x64"), 0.07},
		{in.MustMask("self-hosted", "linux", "arm64"), 0.03},
	}
	return g
}

func pick(rng *rand.Rand, mix []weighted) dispatch.Mask {
	x := rng.Float64()
	for _, m := range mix {
		if x < m.w {
			return m.mask
		}
		x -= m.w
	}
	return mix[len(mix)-1].mask
}

// Job returns the next synthetic job.
func (g *Gen) Job() dispatch.Job {
	g.nextID++
	org := "org-" + strconv.FormatUint(g.zipf.Uint64(), 10)
	labels := pick(g.rng, g.jobMix)
	if g.cfg.NoisyShare > 0 && g.rng.Float64() < g.cfg.NoisyShare {
		org = g.NoisyID
		labels = g.jobMix[0].mask // a monorepo burst is mostly plain ubuntu jobs
	}
	return dispatch.Job{ID: g.nextID, Org: org, Labels: labels}
}

// Runner returns the label set of the next runner to become idle.
func (g *Gen) Runner() dispatch.Mask { return pick(g.rng, g.runMix) }

// Intn exposes the generator's RNG for simulations that need more randomness.
func (g *Gen) Intn(n int) int { return g.rng.Intn(n) }
