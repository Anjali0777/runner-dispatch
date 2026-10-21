package dispatch

import "sync"

// Locked makes any Dispatcher safe for concurrent use with one mutex.
//
// A single lock is the right first step here: Indexed does O(k) work per call,
// so critical sections are a few hundred nanoseconds and one dispatcher can
// serve hundreds of thousands of assignments a second. Sharding (say, one
// dispatcher per runner pool) is the next step if that ever isn't enough;
// see DESIGN.md.
type Locked struct {
	mu sync.Mutex
	d  Dispatcher
}

func NewLocked(d Dispatcher) *Locked { return &Locked{d: d} }

func (l *Locked) Enqueue(j Job) {
	l.mu.Lock()
	l.d.Enqueue(j)
	l.mu.Unlock()
}

func (l *Locked) Assign(r Mask) (Job, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.d.Assign(r)
}

func (l *Locked) Complete(id uint64) {
	l.mu.Lock()
	l.d.Complete(id)
	l.mu.Unlock()
}

func (l *Locked) Queued() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.d.Queued()
}
