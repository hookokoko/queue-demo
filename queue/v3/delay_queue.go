package v3

import (
	"context"
	"queue_demo"
	"sync"
	"time"
)

// 这是一个 自定义cond版本的实现

type Delayable interface {
	Delay() time.Duration
}

type DelayQueue[T Delayable] struct {
	pq   *queue_demo.PriorityQueue[T]
	lock *sync.Mutex

	signalEnqueue *queue_demo.Cond
	signalDequeue *queue_demo.Cond
}

func NewDelayQueue[T Delayable](size int) *DelayQueue[T] {
	l := &sync.Mutex{}
	return &DelayQueue[T]{
		pq: queue_demo.NewPriorityQueue[T](size, func(src, dest T) int {
			srcDelay := src.Delay()
			destDelay := dest.Delay()
			if srcDelay > destDelay {
				return 1
			}
			if srcDelay == destDelay {
				return 0
			}
			return -1
		}),
		lock:          l,
		signalEnqueue: queue_demo.NewCond(l),
		signalDequeue: queue_demo.NewCond(l),
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
			ch := dq.signalEnqueue.SignalCh()
			// 这里是等待有元素出队的信号，意味着可以入队了，即等待下一轮循环入队
			select {
			case <-ch:
			case <-ctx.Done():
				return ctx.Err()
			}
		case nil:
			// 入队成功就要发一个有元素入队的信号，防止出队阻塞的情况下，能通知到到它**能**出队了
			// 这里告诉空了的队列，已经有元素可以出队了
			dq.signalDequeue.Broadcast() // 同理，这里是不是需要一个广播信号
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
		//log.Println("err: ", err)
		switch err {
		// 同上，从优先级队列中获取队首元素，队列为空的情况
		case queue_demo.ErrEmptyQueue:
			ch := dq.signalDequeue.SignalCh()
			// 接收队列已经入了一个元素的信号，表示这里可以出队，也就是进入下一轮出队循环。
			select {
			case <-ch:
				//log.Println("empty queue get enqueue signal.")
			case <-ctx.Done():
				var t T
				return t, ctx.Err()
			}
		// 从优先级队列中获取队首元素成功，这里需要考虑延时队列等待队首元素出队期间，新加入元素的时间和队首时间的比较情况
		case nil:
			if timer == nil {
				timer = time.NewTimer(val.Delay())
				//log.Printf("set %f %+v.\n", val.Delay().Seconds(), val)
			} else {
				timer.Reset(val.Delay())
				//log.Printf("reset %f %+v.\n", val.Delay().Seconds(), val)
			}
			ch := dq.signalDequeue.SignalCh()
			select {
			case <-ctx.Done():
				var t T
				return t, ctx.Err()
			// 到时间了，该出队了
			case <-timer.C:
				dq.lock.Lock()
				// 原队头可能已经被其他协程先出队，故再次检查队头
				val, err := dq.pq.Peek()
				if err != nil || val.Delay() > 0 {
					dq.lock.Unlock()
					continue
				}
				// 验证元素过期后将其出队
				//log.Println("time to deque")
				val, err = dq.pq.Dequeue()
				// 这里告诉满了的队列，有元素出队，你可以入队了
				dq.signalEnqueue.Broadcast() // fixme 这里是不是需要一个广播信号？
				return val, err
			// 有新元素入队的情况
			// 即直接继续下一轮循环，然后重新拿到队首的timer，即可
			case <-ch:
				//log.Println("no empty queue get enqueue signal")
			}
		// 从优先级队列中获取队首元素时，发生其他错误
		default:
			dq.lock.Unlock()
			var t T
			return t, err
		}
	}
}
