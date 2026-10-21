// Package dispatch assigns queued CI jobs to idle runners.
//
// A job asks for a set of labels (say {self-hosted, linux, gpu}) and may only
// run on a runner that has every one of them. Each organisation also has a
// cap on how many of its jobs may run at once. When a runner reports idle,
// the dispatcher must pick a job for it quickly, because while it is picking,
// that runner sits idle and the queue keeps growing.
package dispatch

// Job is a unit of queued work.
type Job struct {
	ID     uint64
	Org    string
	Labels Mask   // labels the job requires
	Seq    uint64 // enqueue order, set by the dispatcher
}

// Dispatcher is implemented by both the naive baseline and the indexed design.
// Implementations are not safe for concurrent use; wrap them in Locked.
type Dispatcher interface {
	// Enqueue adds a job to the queue.
	Enqueue(j Job)
	// Assign picks a job for an idle runner with the given labels.
	// It returns false if no queued job can run there right now.
	Assign(runner Mask) (Job, bool)
	// Complete records that a running job finished, freeing org capacity.
	Complete(jobID uint64)
	// Queued is the number of jobs waiting.
	Queued() int
}

// Caps holds per-org concurrency limits. Orgs not listed get Default.
type Caps struct {
	Default int
	PerOrg  map[string]int
}

func (c Caps) Of(org string) int {
	if n, ok := c.PerOrg[org]; ok {
		return n
	}
	return c.Default
}
