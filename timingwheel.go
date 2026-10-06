package scheduler

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

// TimingWheel 是层级时间轮实现（移植自 nsq 的 timing wheel）。
// 只有当前层能容纳的到期时间留在本层，其余交给上层溢出轮，
// 因此可以低成本支持跨度远大于 wheelSize*tick 的调度。
//
// TimingWheel is a hierarchical timing wheel (ported from nsq's timing wheel).
// Expirations beyond the current wheel are delegated to an overflow wheel.
type TimingWheel struct {
	tick      int64 // 毫秒 / tick in milliseconds
	wheelSize int64

	interval    int64 // 一轮覆盖的时间跨度 / span of one full round
	currentTime int64
	buckets     []*bucket
	queue       *DelayQueue

	// 上层溢出轮，通过 Add() 并发读写 / the higher-level overflow wheel
	overflowWheel unsafe.Pointer // type: *TimingWheel

	exitC     chan struct{}
	waitGroup waitGroupWrapper
}

// NewTimingWheel 创建时间轮，tick 须不小于 1ms，wheelSize 须大于 0。
// Creates a timing wheel; tick must be >= 1ms and wheelSize > 0.
func NewTimingWheel(tick time.Duration, wheelSize int64) *TimingWheel {
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}
	if wheelSize <= 0 {
		panic(errors.New("wheelSize must be greater than 0"))
	}

	startMs := time.Now().UnixMilli()

	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		New(int(wheelSize)),
	)
}

// newTimingWheel 是实际的构造辅助函数。
// Internal constructor shared by the current and overflow wheels.
func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimingWheel {
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = newBucket()
	}
	return &TimingWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs),
		interval:    tickMs * wheelSize,
		buckets:     buckets,
		queue:       queue,
		exitC:       make(chan struct{}),
	}
}

// add 将任务放入当前时间轮相应槽位，超出本轮则交给溢出轮。
// 已到期返回 false。Inserts a timer; returns false when it is already expired.
func (tw *TimingWheel) add(t *Timer) bool {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if t.expiration < currentTime+tw.tick {
		// Already expired
		return false
	} else if t.expiration < currentTime+tw.interval {
		// 放入本轮对应的槽位 / put it into its own bucket
		virtualID := t.expiration / tw.tick
		b := tw.buckets[virtualID%tw.wheelSize]
		b.Add(t)

		// 槽位到期时间变化时才重新入队，避免同一轮重复入队。
		// Re-enqueue only when the bucket expiration changes.
		if b.SetExpiration(virtualID * tw.tick) {
			tw.queue.Offer(b, b.Expiration())
		}

		return true
	} else {
		// 超出本轮范围，交由上层溢出轮 / delegate to the overflow wheel
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil {
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,
					tw.wheelSize,
					currentTime,
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*TimingWheel)(overflowWheel).add(t)
	}
}

// addOrRun 将任务放入时间轮；已到期则直接触发执行。
// Inserts the timer, or runs it immediately when already expired.
func (tw *TimingWheel) addOrRun(t *Timer) {
	if !tw.add(t) {
		go t.function(false)
	}
}

// advanceClock 将轮子时间推进到 expiration，并同步推进溢出轮。
// Advances the wheel clock and the overflow wheel if present.
func (tw *TimingWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick {
		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// start 启动时间轮：延迟队列负责唤醒，槽位到期后重新调度其中任务。
// Starts the delay queue poller and the bucket consumer loop.
func (tw *TimingWheel) start() {
	tw.waitGroup.Wrap(func() {
		tw.queue.Poll(tw.exitC, func() int64 {
			return time.Now().UnixMilli()
		})
	})

	tw.waitGroup.Wrap(func() {
		for {
			select {
			case elem := <-tw.queue.C:
				b := elem.(*bucket)
				tw.advanceClock(b.Expiration())
				b.Flush(tw.addOrRun)
			case <-tw.exitC:
				return
			}
		}
	})
}

// stop 停止时间轮。注意：已在独立 goroutine 中执行的任务不会被等待。
// Stops the timing wheel. Note: tasks already running in their own goroutines
// are not awaited.
func (tw *TimingWheel) stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}
