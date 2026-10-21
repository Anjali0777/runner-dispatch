package dispatch

// Naive is the obvious first design: one FIFO slice, and on every idle runner
// a scan from the front for the first job the runner can take.
//
//	Assign:   O(Q) scan + O(Q) slice delete, Q = queued jobs
//	Enqueue:  O(1) amortised
//	Complete: O(1)
//
// It is correct, and fast while the queue is short. The trouble starts when
// the front of the queue fills with jobs nobody can take right now (an org at
// its cap, or a label no idle runner has). Every Assign walks past all of
// them again, so the dispatcher gets slower exactly when it is most behind.
type Naive struct {
	caps    Caps
	queue   []Job
	running map[string]int
	owner   map[uint64]string // running job -> org
	seq     uint64
}

func NewNaive(caps Caps) *Naive {
	return &Naive{caps: caps, running: map[string]int{}, owner: map[uint64]string{}}
}

func (d *Naive) Enqueue(j Job) {
	d.seq++
	j.Seq = d.seq
	d.queue = append(d.queue, j)
}

func (d *Naive) Assign(runner Mask) (Job, bool) {
	for i, j := range d.queue {
		if !runner.Covers(j.Labels) || d.running[j.Org] >= d.caps.Of(j.Org) {
			continue
		}
		copy(d.queue[i:], d.queue[i+1:])
		d.queue = d.queue[:len(d.queue)-1]
		d.running[j.Org]++
		d.owner[j.ID] = j.Org
		return j, true
	}
	return Job{}, false
}

func (d *Naive) Complete(jobID uint64) {
	if org, ok := d.owner[jobID]; ok {
		d.running[org]--
		delete(d.owner, jobID)
	}
}

func (d *Naive) Queued() int { return len(d.queue) }
