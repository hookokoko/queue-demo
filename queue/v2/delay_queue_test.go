package v2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelayQueue_Dequeue(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name    string
		q       *DelayQueue[delayElem]
		timeout time.Duration
		wantVal int
		wantErr error
	}{
		{
			name: "dequeued",
			q: newDelayQueue(t, delayElem{
				deadline: now.Add(time.Millisecond * 10),
				val:      11,
			}),
			timeout: time.Second,
			wantVal: 11,
		},
		{
			// 元素本身就已经过期了
			name: "already deadline",
			q: newDelayQueue(t, delayElem{
				deadline: now.Add(-time.Millisecond * 10),
				val:      11,
			}),
			timeout: time.Second,
			wantVal: 11,
		},
		{
			// 已经超时了的 context 设置
			name: "invalid context",
			q: newDelayQueue(t, delayElem{
				deadline: now.Add(time.Millisecond * 10),
				val:      11,
			}),
			timeout: -time.Second,
			wantErr: context.DeadlineExceeded,
		},
		{
			name:    "empty and timeout",
			q:       NewDelayQueue[delayElem](10),
			timeout: time.Second,
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "not empty but timeout",
			q: newDelayQueue(t, delayElem{
				deadline: now.Add(time.Second * 10),
				val:      11,
			}),
			timeout: time.Second,
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range testCases {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			ele, err := tc.q.Dequeue(ctx)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, ele.val)
		})
	}
}

func TestDelayQueue_Enqueue(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name    string
		q       *DelayQueue[delayElem]
		timeout time.Duration
		val     delayElem
		wantErr error
	}{
		{
			name:    "enqueued",
			q:       NewDelayQueue[delayElem](3),
			timeout: time.Second,
			val:     delayElem{val: 123, deadline: now.Add(time.Minute)},
		},
		{
			// context 本身已经过期了
			name:    "invalid context",
			q:       NewDelayQueue[delayElem](3),
			timeout: -time.Second,
			val:     delayElem{val: 123, deadline: now.Add(time.Minute)},
			wantErr: context.DeadlineExceeded,
		},
		{
			// enqueue 的时候阻塞住了，直到超时
			name:    "enqueue timeout",
			q:       newDelayQueue(t, delayElem{val: 123, deadline: now.Add(time.Minute)}),
			timeout: time.Millisecond * 100,
			val:     delayElem{val: 234, deadline: now.Add(time.Minute)},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			err := tc.q.Enqueue(ctx, tc.val)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func newDelayQueue(t *testing.T, eles ...delayElem) *DelayQueue[delayElem] {
	q := NewDelayQueue[delayElem](len(eles))
	for _, ele := range eles {
		err := q.Enqueue(context.Background(), ele)
		require.NoError(t, err)
	}
	return q
}

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
		deadline: now.Add(time.Second * 60),
		val:      3,
	})

	val, _ := q.Dequeue(ctx)

	fmt.Println(val.val)

	// Output:
	// 3
}
