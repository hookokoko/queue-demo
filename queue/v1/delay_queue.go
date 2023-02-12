package v1

import (
	"context"
	"log"
	"queue_demo"
	"sync"
	"time"
)

// 这是一个 单个chan 的实现，仅作为信号传递的示例，具备多个通知的见v2 v3

type Delayable interface {
	Delay() time.Duration
}

type DelayQueue[T Delayable] struct {
	pq   *queue_demo.PriorityQueue[T]
	lock *sync.Mutex

	signalEnqueue chan struct{}
	signalDequeue chan struct{}
}

func NewDelayQueue[T Delayable](size int) *DelayQueue[T] {
	l := &sync.Mutex{}
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
		lock:          l,
		signalEnqueue: make(chan struct{}),
		signalDequeue: make(chan struct{}),
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
			select {
			case <-dq.signalEnqueue:
			case <-ctx.Done():
				return ctx.Err()
			}
		case nil:
			// 入队成功就要发一个有元素入队的信号，防止出队阻塞的情况下，能通知到到它**能**出队了
			// 这里告诉空了的队列，已经有元素可以出队了
			dq.signalDequeue <- struct{}{} // 同理，这里是不是需要一个广播信号
			dq.lock.Unlock()
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
			select {
			case <-dq.signalDequeue:
			case <-ctx.Done():
				var t T
				return t, ctx.Err()
			}
		// 从优先级队列中获取队首元素成功，这里需要考虑延时队列等待队首元素出队期间，新加入元素的时间和队首时间的比较情况
		case nil:
			if timer == nil {
				timer = time.NewTimer(val.Delay())
			} else {
				timer.Reset(val.Delay())
			}
			select {
			case <-ctx.Done():
				var t T
				return t, ctx.Err()
			// 到时间了，该出队了
			case <-timer.C:
				_, err := dq.pq.Dequeue()
				dq.lock.Unlock()
				// 这里告诉满了的队列，有元素出队，你可以入队了
				dq.signalEnqueue <- struct{}{} // fixme 这里是不是需要一个广播信号？
				return val, err
			// 有新元素入队的情况
			// 即直接继续下一轮循环，然后重新拿到队首的timer，即可
			case <-dq.signalDequeue:
				log.Println("....")
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
