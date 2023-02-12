package v1

import (
	"context"
	"fmt"
	"time"
)

type delayElem struct {
	deadline time.Time
	val      int
}

func (d delayElem) Delay() time.Duration {
	return time.Until(d.deadline)
}

func ExampleNewDelayQueue() {
	q := NewDelayQueue[delayElem](10)
	ctx := context.TODO()
	now := time.Now()
	_ = q.Enqueue(ctx, delayElem{
		// 3 秒后过期
		deadline: now.Add(time.Second * 3),
		val:      3,
	})

	val, _ := q.Dequeue(ctx)

	fmt.Println(val.val)

	// Output:
	// 3
}
