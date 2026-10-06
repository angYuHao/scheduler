package scheduler

import (
	"sync"
	"time"
)

// truncate 将 x 向下取整到 m 的整数倍（m<=0 时原样返回）。
// truncate rounds x down to a multiple of m.
func truncate(x, m int64) int64 {
	if m <= 0 {
		return x
	}
	return x - x%m
}

// waitGroupWrapper 封装 WaitGroup，简化 goroutine 启动与等待。
// waitGroupWrapper simplifies spawning tracked goroutines.
type waitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap 启动一个受 WaitGroup 跟踪的 goroutine。
// Wrap runs cb in a tracked goroutine.
func (w *waitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		defer w.Done()
		cb()
	}()
}

// formatTime 格式化时间为 "2006-01-02 15:04:05"。
// formatTime renders a time as "2006-01-02 15:04:05".
func formatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}
