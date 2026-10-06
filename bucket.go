package scheduler

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Timer 表示一个待触发的任务，到期后执行其函数。
// Timer is a single pending job executed when it expires.
type Timer struct {
	expiration int64          // 到期时间（毫秒）/ expiration in milliseconds
	function   func(bool)     // 入参 startNow 表示是否手动立即执行 / startNow means run immediately
	b          unsafe.Pointer // 所属槽位 / owning bucket, type: *bucket
	element    *list.Element  // 在槽位链表中的位置 / position in the bucket list
	stopped    atomic.Bool    // 是否已暂停，仅用于统计 / paused flag, kept for statistics
}

// setStopped 标记任务暂停状态，供统计接口读取。
// Records the paused flag so that statistics can exclude paused jobs.
func (t *Timer) setStopped(v bool) { t.stopped.Store(v) }

// isStopped 读取暂停状态。Reads the paused flag.
func (t *Timer) isStopped() bool { return t.stopped.Load() }

// getBucket 原子读取所属槽位。Atomically reads the owning bucket.
func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

// setBucket 原子写入所属槽位。Atomically sets the owning bucket.
func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// stop 阻止任务触发；成功拦截返回 true，已到期或已停止返回 false。
// 注意：已在独立 goroutine 中开始执行的任务不会被等待。
// stop prevents the timer from firing and reports whether it intercepted it.
// Tasks already running in their own goroutines are not awaited.
func (t *Timer) stop() bool {
	stopped := false
	// 任务可能正被时间轮在槽位之间搬运，循环直到任务不属于任何槽位。
	// The task may be moving between buckets, so retry until it belongs to none.
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		stopped = b.Remove(t)
	}
	return stopped
}

// bucket 为时间轮槽位，保存同一轮次内到期的任务。
// bucket holds the timers expiring in the same round.
type bucket struct {
	expiration int64 // 本轮到期时间 / expiration of this round
	mu         sync.Mutex
	timers     *list.List
}

func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1,
	}
}

// Expiration 读取槽位到期时间。Reads the bucket expiration.
func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// SetExpiration 设置到期时间，值发生变化时返回 true。
// Sets the expiration and reports whether it actually changed.
func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// Add 将任务加入槽位。Adds a timer to the bucket.
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	e := b.timers.PushBack(t)
	t.setBucket(b)
	t.element = e
}

// remove 在已持锁的前提下移除任务。Removes a timer; caller must hold the lock.
func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		// 任务已被时间轮搬到其它槽位（Flush -> remove -> 其它槽位 Add），
		// 此处无需处理。The timer has been moved to another bucket already.
		return false
	}
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

// Remove 移除任务。Removes a timer from the bucket.
func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// Flush 取出槽位内全部任务并重新插入时间轮，随后重置到期时间。
// Drains the bucket, reinserting every timer through reinsert.
func (b *bucket) Flush(reinsert func(*Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for e := b.timers.Front(); e != nil; {
		next := e.Next()
		t := e.Value.(*Timer)
		b.remove(t)
		reinsert(t)
		e = next
	}

	b.SetExpiration(-1)
}
