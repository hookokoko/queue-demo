package queue_demo

import (
	"context"
	"sync"
	"time"
)

// 使用channel实现的sync.Cond , 并带有超时的实现

type CondV1 struct {
	L  sync.Mutex // used by caller
	mu sync.Mutex // guards ch , 这里的也可以不用这个guard，直接atomic 操作ch 指针
	ch chan struct{}
}

func (c *CondV1) Wait() {
	c.mu.Lock()
	ch := c.ch
	c.mu.Unlock()
	c.L.Unlock()
	<-ch
	c.L.Lock()
}

func (c *CondV1) WaitTimeout(t time.Duration) {
	c.mu.Lock()
	ch := c.ch
	c.mu.Unlock()
	c.L.Unlock()
	select {
	case <-ch:
	case <-time.Tick(t):
		return
	}
	c.L.Lock()
}

func (c *CondV1) WaitTimeoutCtx(ctx context.Context) error {
	c.mu.Lock()
	ch := c.ch
	c.mu.Unlock()
	c.L.Unlock()
	select {
	case <-ch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.L.Lock()
	return nil
}

func (c *CondV1) Signal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
}

func (c *CondV1) Broadcast() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.ch)
	c.ch = make(chan struct{})
}
