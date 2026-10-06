package scheduler

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// 本文件为最小优先队列 + 延迟队列实现，借鉴 nsq 的 pqueue 与 timing wheel。
// 延迟队列按到期时间排序，队首到期后经 C 通道交给时间轮消费。
//
// Min-heap priority queue and delay queue inspired by nsq's pqueue/timing wheel.
// Elements are ordered by expiration time and delivered through C when due.
//
// 借鉴来源 / Borrowed from: https://github.com/nsqio/nsq

// item 为堆元素，Priority 越小越先出队。
// item is a heap element; the smaller Priority is dequeued first.
type item struct {
	Value    interface{}
	Priority int64
	Index    int
}

// priorityQueue 为最小堆，队首（0 号元素）优先级最低。
// priorityQueue is a min heap; index 0 holds the lowest priority.
type priorityQueue []*item

// newPriorityQueue 创建指定初始容量的最小堆。
// Creates a min heap with the given initial capacity.
func newPriorityQueue(capacity int) priorityQueue {
	return make(priorityQueue, 0, capacity)
}

// Len 返回堆元素个数。Returns the number of elements.
func (pq priorityQueue) Len() int {
	return len(pq)
}

// Less 比较优先级。Compares priorities.
func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].Priority < pq[j].Priority
}

// Swap 交换元素并同步索引。Swaps elements and keeps indices in sync.
func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push 压入元素并按需扩容（heap.Interface）。
// Pushes an element, growing capacity when needed (heap.Interface).
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c {
		npq := make(priorityQueue, n, c*2)
		copy(npq, *pq)
		*pq = npq
	}
	*pq = (*pq)[0 : n+1]
	item := x.(*item)
	item.Index = n
	(*pq)[n] = item
}

// Pop 弹出堆顶元素（heap.Interface）。
// Pops the top element (heap.Interface).
func (pq *priorityQueue) Pop() interface{} {
	n := len(*pq)
	c := cap(*pq)
	if n < (c/2) && c > 25 {
		npq := make(priorityQueue, n, c/2)
		copy(npq, *pq)
		*pq = npq
	}
	item := (*pq)[n-1]
	item.Index = -1
	*pq = (*pq)[0 : n-1]
	return item
}

// PeekAndShift 队首到期（Priority <= max）时弹出并返回，
// 否则返回 nil 与距离到期还有多久。
// Removes and returns the head when it is due (Priority <= max);
// otherwise returns nil plus the remaining delay in milliseconds.
func (pq *priorityQueue) PeekAndShift(max int64) (*item, int64) {
	if pq.Len() == 0 {
		return nil, 0
	}

	item := (*pq)[0]
	if item.Priority > max {
		return nil, item.Priority - max
	}
	heap.Remove(pq, 0)

	return item, 0
}

// ---------- 优先队列结束 / End of priority queue ----------

// DelayQueue 为无界阻塞延迟队列，元素到期后才能取出，队首是最早到期者。
// DelayQueue is an unbounded blocking queue whose elements can only be taken
// once their delay has expired; the head expired furthest in the past.
type DelayQueue struct {
	C chan interface{} // 到期元素输出通道 / channel delivering expired elements

	mu sync.Mutex
	pq priorityQueue

	// 与 runtime.timer 的休眠状态类似，休眠时 Poll 阻塞在 wakeupC。
	// Mirrors the sleeping state of runtime timers: Poll blocks on wakeupC.
	sleeping int32
	wakeupC  chan struct{}
}

// New 创建延迟队列，size 为堆初始容量。
// Creates a delay queue with the given initial heap capacity.
func New(size int) *DelayQueue {
	return &DelayQueue{
		C:       make(chan interface{}),
		pq:      newPriorityQueue(size),
		wakeupC: make(chan struct{}),
	}
}

// Offer 按到期时间插入元素；若成为队首则唤醒 Poll。
// Inserts an element by expiration time and wakes Poll when it becomes the head.
func (dq *DelayQueue) Offer(elem interface{}, expiration int64) {
	item := &item{Value: elem, Priority: expiration}

	dq.mu.Lock()
	heap.Push(&dq.pq, item)
	index := item.Index
	dq.mu.Unlock()

	if index == 0 {
		// A new item with the earliest expiration is added.
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			dq.wakeupC <- struct{}{}
		}
	}
}

// Poll 循环等待元素到期并发送到 C；exitC 关闭时退出。
// Loopingly waits for elements to expire and sends them to C; exits when exitC closes.
func (dq *DelayQueue) Poll(exitC chan struct{}, nowF func() int64) {
	for {
		now := nowF()

		dq.mu.Lock()
		item, delta := dq.pq.PeekAndShift(now)
		if item == nil {
			// No items left or at least one item is pending.

			// We must ensure the atomicity of the whole operation, which is
			// composed of the above PeekAndShift and the following StoreInt32,
			// to avoid possible race conditions between Offer and Poll.
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if item == nil {
			if delta == 0 {
				// No items left.
				select {
				case <-dq.wakeupC:
					// Wait until a new item is added.
					continue
				case <-exitC:
					goto exit
				}
			} else if delta > 0 {
				// At least one item is pending.
				select {
				case <-dq.wakeupC:
					// A new item with an "earlier" expiration than the current "earliest" one is added.
					continue
				case <-time.After(time.Duration(delta) * time.Millisecond):
					// The current "earliest" item expires.

					// Reset the sleeping state since there's no need to receive from wakeupC.
					if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
						// A caller of Offer() is being blocked on sending to wakeupC,
						// drain wakeupC to unblock the caller.
						<-dq.wakeupC
					}
					continue
				case <-exitC:
					goto exit
				}
			}
		}

		select {
		case dq.C <- item.Value:
			// The expired element has been sent out successfully.
		case <-exitC:
			goto exit
		}
	}

exit:
	// 重置休眠状态 / reset the sleeping state
	atomic.StoreInt32(&dq.sleeping, 0)
}
