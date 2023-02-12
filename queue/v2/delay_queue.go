package v2

import (
	"context"
	"queue_demo"
	"sync"
	"time"
)

// 这是一个 sync.Cond版本的实现，但是未带超时控制

type Delayable interface {
	Delay() time.Duration
}

type DelayQueue[T Delayable] struct {
	pq   *queue_demo.PriorityQueue[T]
	lock sync.Mutex

	// 使用Cond就没法进行超时控制了，所以后面还要实现一个带超时控制的Cond
	signalEnqueueCond sync.Cond
	signalDequeueCond sync.Cond
}

func NewDelayQueue[T Delayable](size int) *DelayQueue[T] {
	l := sync.Mutex{}
	return &DelayQueue[T]{
		pq: queue_demo.NewPriorityQueue[T](size, func(src, dest T) int {
			srcDelay := src.Delay()
			destDelay := dest.Delay()
			if srcDelay < destDelay {
				return 1
			}
			if srcDelay == destDelay {
				return 0
			}
			return -1
		}),
		lock:              l,
		signalEnqueueCond: sync.Cond{L: &l},
		signalDequeueCond: sync.Cond{L: &l},
	}
}

func (dq *DelayQueue[T]) Enqueue(ctx context.Context, val T) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		dq.lock.Lock()
		err := dq.pq.Enqueue(val)
		// TODO 这里面加释放锁的时机，是不是需要斟酌一下？
		switch err {
		case queue_demo.ErrOutOfCapacity:
			dq.lock.Unlock()
			// 这里是等待有元素出队的信号，意味着可以入队了，即等待下一轮循环入队
			// 使用了sync.Cond做不到超时控制
			dq.signalEnqueueCond.Wait()
		case nil:
			// 入队成功就要发一个有元素入队的信号，防止出队阻塞的情况下，能通知到到它**能**出队了
			// 这里告诉空了的队列，已经有元素可以出队了
			// 注意这里还得广播，因为deque的时候不止一个地方在等这个信号，一个是出队时，空队列阻塞等待；一个是出队队首元素等待时间比新入队的元素等待时间长
			dq.lock.Unlock()
			dq.signalDequeueCond.Broadcast()
			return nil
		default:
			// 其他的错误，这里就是直接返回了
			dq.lock.Unlock()
			return err
		}
	}
}

func (dq *DelayQueue[T]) Dequeue(ctx context.Context) (T, error) {
	var timer *time.Timer
	for {
		select {
		case <-ctx.Done():
			var t T
			return t, ctx.Err()
		default:
		}
		dq.lock.Lock()
		val, err := dq.pq.Peek()
		switch err {
		// 同上，从优先级队列中获取队首元素，队列为空的情况
		case queue_demo.ErrEmptyQueue:
			dq.lock.Unlock()
			// 接收队列已经入了一个元素的信号，表示这里可以出队，也就是进入下一轮出队循环。
			dq.signalDequeueCond.Wait()
		// 从优先级队列中获取队首元素成功，这里需要考虑延时队列等待队首元素出队期间，新加入元素的时间和队首时间的比较情况
		case nil:
			if timer == nil {
				timer = time.NewTimer(val.Delay())
			} else {
				timer.Reset(val.Delay())
			}
			select {
			case <-ctx.Done():
				dq.lock.Unlock()
				var t T
				return t, ctx.Err()
			// 到时间了，该出队了
			case <-timer.C:
				_, err := dq.pq.Dequeue()
				dq.lock.Unlock()
				// 这里告诉满了的队列，有元素出队，你可以入队了
				dq.signalEnqueueCond.Signal()
				return val, err
			// 有新元素入队的情况，如何判断，这个chan会在上面把它取走？
			// 1. 新入队的元素延迟的时间 < 队首元素延迟的时间，重新计时（退出到外层的for），否则就继续阻塞在这个for中
			default:
				dq.lock.Lock()
				dq.signalDequeueCond.Wait()
			}
			return val, nil
		// 从优先级队列中获取队首元素时，发生其他错误
		default:
			dq.lock.Unlock()
			var t T
			return t, err
		}
	}
}
