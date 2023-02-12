package queue_demo

import "sync"

type Cond struct {
	signal chan struct{}
	l      sync.Locker
}

func NewCond(l sync.Locker) *Cond {
	return &Cond{
		signal: make(chan struct{}),
		l:      l,
	}
}

func (c *Cond) Broadcast() {
	signal := make(chan struct{})
	old := c.signal
	c.signal = signal
	c.l.Unlock()
	close(old)
}

func (c *Cond) SignalCh() <-chan struct{} {
	res := c.signal
	c.l.Unlock()
	return res
}
