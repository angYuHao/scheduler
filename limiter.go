package scheduler

import "sync"

// limiter 是可动态调整的并发闸门，用于限制同一时刻正在执行的任务数。
// limit <= 0 表示不限制；cur 为当前已占用名额数。
// 等待者通过 notify 通道被唤醒（关闭并重建实现广播），
// 因此调整上限、归还名额都能立即唤醒排队中的任务。
//
// limiter is a resizable concurrency gate. It limits how many jobs may run at
// the same time; limit <= 0 means unlimited. Waiters are woken up by closing
// and recreating the notify channel, so both releasing a slot and changing the
// limit wake queued jobs immediately.
type limiter struct {
	mu     sync.Mutex
	notify chan struct{} // 状态变化广播 / change broadcast
	limit  int           // 上限，<=0 不限 / limit, <=0 means unlimited
	cur    int           // 已占用名额 / occupied slots
}

func newLimiter(limit int) *limiter {
	return &limiter{
		notify: make(chan struct{}),
		limit:  limit,
	}
}

// acquire 申请一个执行名额，直到有空闲名额为止。
// stop 关闭时立即放弃等待并返回 false（调度器已停止）。
//
// acquire reserves one execution slot, blocking until one is free.
// It returns false without waiting when stop is closed.
func (l *limiter) acquire(stop <-chan struct{}) bool {
	l.mu.Lock()
	for l.limit > 0 && l.cur >= l.limit {
		wait := l.notify // 先取快照再放锁，避免被广播遗漏 / snapshot before unlocking
		l.mu.Unlock()
		select {
		case <-wait:
		case <-stop:
			return false
		}
		l.mu.Lock()
	}
	l.cur++
	l.mu.Unlock()
	return true
}

// release 归还一个名额并唤醒等待者。
// release returns a slot and wakes up waiters.
func (l *limiter) release() {
	l.mu.Lock()
	if l.cur > 0 {
		l.cur--
	}
	l.broadcastLocked()
	l.mu.Unlock()
}

// setLimit 动态调整上限；n <= 0 表示不限制。
// Resizes the gate; n <= 0 means unlimited.
func (l *limiter) setLimit(n int) {
	l.mu.Lock()
	l.limit = n
	l.broadcastLocked()
	l.mu.Unlock()
}

// broadcastLocked 关闭并重建 notify 向所有等待者广播，必须持锁调用。
// Must be called with the lock held.
func (l *limiter) broadcastLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}

// snapshot 返回当前上限与已占用名额数。
// Returns the current limit and the number of occupied slots.
func (l *limiter) snapshot() (limit, cur int) {
	l.mu.Lock()
	limit, cur = l.limit, l.cur
	l.mu.Unlock()
	return
}

// saturated 判断是否已达上限（仅在「跳过」策略下用于快速判定）。
// Reports whether the gate is full; only used by the skip policy.
func (l *limiter) saturated() bool {
	l.mu.Lock()
	full := l.limit > 0 && l.cur >= l.limit
	l.mu.Unlock()
	return full
}
