package dispatch

// Indexed never looks at a job it can't hand out.
//
// Jobs are grouped first by the exact label set they need (a "pool"), then
// by org inside each pool. Each pool keeps a ring of the org queues that are
// currently eligible: non-empty, and the org is under its cap. An org that
// hits its cap is unlinked from every ring it is in ("parked") and relinked
// when one of its jobs completes. So the blocked jobs that the naive scan
// walks past over and over are not on the hot path at all.
//
// An idle runner looks up which pools it covers (cached per runner label set),
// takes the pool whose next candidate is oldest, and serves orgs inside that
// pool round-robin, so one org's burst can't starve everyone else.
//
//	Assign:   O(k), k = pools this runner covers (a handful in practice),
//	          plus O(p) when an org hits its cap, p = pools that org uses
//	Enqueue:  O(1) amortised
//	Complete: O(1), plus O(p) when it unparks an org
//	Space:    O(Q + orgs × pools in use)
type Indexed struct {
	caps     Caps
	pools    map[Mask]*pool
	poolList []*pool
	compat   map[Mask][]*pool // runner labels -> pools it can serve
	orgQs    map[string][]*orgQueue
	running  map[string]int
	owner    map[uint64]string
	queued   int
	seq      uint64
}

type pool struct {
	labels Mask
	byOrg  map[string]*orgQueue
	cursor *orgQueue // next org to serve; nil when nothing in this pool is eligible
}

// orgQueue is one org's FIFO of jobs needing one label set. It doubles as a
// node in its pool's circular ring of eligible orgs.
type orgQueue struct {
	org        string
	pool       *pool
	jobs       []Job
	head       int
	prev, next *orgQueue
	linked     bool
}

func (q *orgQueue) empty() bool { return q.head == len(q.jobs) }
func (q *orgQueue) front() *Job { return &q.jobs[q.head] }

func (q *orgQueue) pop() Job {
	j := q.jobs[q.head]
	q.jobs[q.head] = Job{}
	q.head++
	switch {
	case q.empty():
		q.jobs, q.head = q.jobs[:0], 0
	case q.head > 1024 && q.head*2 > len(q.jobs):
		n := copy(q.jobs, q.jobs[q.head:])
		q.jobs, q.head = q.jobs[:n], 0
	}
	return j
}

func NewIndexed(caps Caps) *Indexed {
	return &Indexed{
		caps:    caps,
		pools:   map[Mask]*pool{},
		compat:  map[Mask][]*pool{},
		orgQs:   map[string][]*orgQueue{},
		running: map[string]int{},
		owner:   map[uint64]string{},
	}
}

func (d *Indexed) Enqueue(j Job) {
	d.seq++
	j.Seq = d.seq
	p := d.pools[j.Labels]
	if p == nil {
		p = &pool{labels: j.Labels, byOrg: map[string]*orgQueue{}}
		d.pools[j.Labels] = p
		d.poolList = append(d.poolList, p)
		clear(d.compat) // a new label set can change any runner's answer
	}
	q := p.byOrg[j.Org]
	if q == nil {
		q = &orgQueue{org: j.Org, pool: p}
		p.byOrg[j.Org] = q
		d.orgQs[j.Org] = append(d.orgQs[j.Org], q)
	}
	q.jobs = append(q.jobs, j)
	d.queued++
	if !q.linked && d.running[j.Org] < d.caps.Of(j.Org) {
		p.link(q)
	}
}

func (d *Indexed) compatible(r Mask) []*pool {
	ps, ok := d.compat[r]
	if !ok {
		for _, p := range d.poolList {
			if r.Covers(p.labels) {
				ps = append(ps, p)
			}
		}
		d.compat[r] = ps
	}
	return ps
}

func (d *Indexed) Assign(r Mask) (Job, bool) {
	var best *pool
	for _, p := range d.compatible(r) {
		if p.cursor != nil && (best == nil || p.cursor.front().Seq < best.cursor.front().Seq) {
			best = p
		}
	}
	if best == nil {
		return Job{}, false
	}
	q := best.cursor
	j := q.pop()
	if q.empty() {
		best.unlink(q)
	} else {
		best.cursor = q.next // round-robin to the next org
	}
	d.queued--
	d.running[j.Org]++
	d.owner[j.ID] = j.Org
	if d.running[j.Org] >= d.caps.Of(j.Org) {
		for _, oq := range d.orgQs[j.Org] { // park: off every ring until a slot frees
			if oq.linked {
				oq.pool.unlink(oq)
			}
		}
	}
	return j, true
}

func (d *Indexed) Complete(jobID uint64) {
	org, ok := d.owner[jobID]
	if !ok {
		return
	}
	delete(d.owner, jobID)
	wasCapped := d.running[org] >= d.caps.Of(org)
	d.running[org]--
	if wasCapped && d.running[org] < d.caps.Of(org) {
		for _, oq := range d.orgQs[org] { // unpark
			if !oq.linked && !oq.empty() {
				oq.pool.link(oq)
			}
		}
	}
}

func (d *Indexed) Queued() int { return d.queued }

// link adds q to the ring just before the cursor, so it is served last this round.
func (p *pool) link(q *orgQueue) {
	if p.cursor == nil {
		q.prev, q.next = q, q
		p.cursor = q
	} else {
		tail := p.cursor.prev
		tail.next, q.prev = q, tail
		q.next, p.cursor.prev = p.cursor, q
	}
	q.linked = true
}

func (p *pool) unlink(q *orgQueue) {
	if q.next == q {
		p.cursor = nil
	} else {
		q.prev.next, q.next.prev = q.next, q.prev
		if p.cursor == q {
			p.cursor = q.next
		}
	}
	q.prev, q.next, q.linked = nil, nil, false
}
