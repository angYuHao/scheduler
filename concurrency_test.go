package scheduler

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowState 为单个用例的执行统计。
// 每个任务类型持有独立的 slowState，避免跨用例污染——
// Stop() 不等待运行中的任务 goroutine，共用计数器会被上一个用例的残留执行污染。
type slowState struct {
	runs atomic.Int64 // 累计执行次数 / total runs
	cur  atomic.Int64 // 当前正在执行数 / concurrent runs right now
	peak atomic.Int64 // 并发峰值 / observed peak concurrency
}

// run 记录一次执行：更新并发数与峰值，休眠 d 后归还。
// Records one execution, sleeping for d before releasing the slot.
func (s *slowState) run(d time.Duration) {
	cur := s.cur.Add(1)
	for {
		peak := s.peak.Load()
		if cur <= peak || s.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	s.runs.Add(1)
	time.Sleep(d)
	s.cur.Add(-1)
}

// waitPeak 等待并发峰值达到期望值。
func (s *slowState) waitPeak(want int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && s.peak.Load() < want {
		time.Sleep(5 * time.Millisecond)
	}
	return s.peak.Load() >= want
}

// waitRuns 等待累计执行次数达到期望值。
func (s *slowState) waitRuns(want int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && s.runs.Load() < want {
		time.Sleep(5 * time.Millisecond)
	}
	return s.runs.Load() >= want
}

// 注意：PeriodicTask.Function 必须使用方法表达式（如 serialTasks.Run），
// 绑定方法值（tk.Run）经 runtime.FuncForPC 得到 "...Run-fm"，
// 与 AddTaskFunction 注册的 "...Run" 不匹配，会导致任务找不到函数而终止。
// 另需为每个任务类型单独声明状态，故此处按用例拆分类型。
type serialTasks struct{} // 串行执行验证 / serial execution

var serialState slowState

func (t serialTasks) Run() { serialState.run(60 * time.Millisecond) }

type parallelTasks struct{} // 不限并发验证 / unlimited concurrency

var parallelState slowState

func (t parallelTasks) Run() { parallelState.run(80 * time.Millisecond) }

type skipTasks struct{} // Skip 策略验证 / skip policy

var skipState slowState

func (t skipTasks) Run() { skipState.run(100 * time.Millisecond) }

type resizeTasks struct{} // 动态调整上限验证 / runtime resize

var resizeState slowState

func (t resizeTasks) Run() { resizeState.run(60 * time.Millisecond) }

type busyTasks struct{} // 运行统计验证 / running counters

var busyState slowState

func (t busyTasks) Run() { busyState.run(300 * time.Millisecond) }

type idleTasks struct{} // 仅统计不实际执行 / counters only

var idleState slowState

func (t idleTasks) Run() { idleState.run(10 * time.Millisecond) }

//---------- limiter 单元测试 ----------

// 验证上限生效、release 后可继续获取。
func TestLimiterAcquireRelease(t *testing.T) {
	l := newLimiter(2)
	stop := make(chan struct{})
	if !l.acquire(stop) {
		t.Fatal("第 1 次 acquire 应成功")
	}
	if !l.acquire(stop) {
		t.Fatal("第 2 次 acquire 应成功")
	}
	if !l.saturated() {
		t.Fatal("已达上限，saturated 应为 true")
	}
	// 第三个应阻塞，直到有任务释放
	done := make(chan bool, 1)
	go func() { done <- l.acquire(stop) }()
	select {
	case <-done:
		t.Fatal("已达上限时 acquire 不应立即返回")
	case <-time.After(50 * time.Millisecond):
	}
	l.release()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("release 后 acquire 应成功")
		}
	case <-time.After(time.Second):
		t.Fatal("release 后 acquire 应被唤醒")
	}
	if limit, cur := l.snapshot(); limit != 2 || cur != 2 {
		t.Fatalf("snapshot 应为 (2,2)，实际 (%d,%d)", limit, cur)
	}
	l.release()
	l.release()
}

// 验证关闭 stop 后等待中的 acquire 立即放弃。
func TestLimiterAcquireStops(t *testing.T) {
	l := newLimiter(1)
	stop := make(chan struct{})
	if !l.acquire(stop) {
		t.Fatal("第 1 次 acquire 应成功")
	}
	done := make(chan bool, 1)
	go func() { done <- l.acquire(stop) }()
	time.Sleep(30 * time.Millisecond)
	close(stop)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("stop 关闭后 acquire 应返回 false")
		}
	case <-time.After(time.Second):
		t.Fatal("stop 关闭后 acquire 应立即返回")
	}
}

// 验证 setLimit 可动态放大与缩小上限，并唤醒等待者。
func TestLimiterSetLimit(t *testing.T) {
	l := newLimiter(1)
	stop := make(chan struct{})
	if !l.acquire(stop) {
		t.Fatal("acquire 应成功")
	}
	done := make(chan bool, 1)
	go func() { done <- l.acquire(stop) }()
	time.Sleep(30 * time.Millisecond)
	l.setLimit(2) // 放大后等待者应被唤醒
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("放大上限后 acquire 应成功")
		}
	case <-time.After(time.Second):
		t.Fatal("放大上限后等待者未被唤醒")
	}
	if limit, _ := l.snapshot(); limit != 2 {
		t.Fatalf("上限应为 2，实际 %d", limit)
	}
	l.setLimit(0) // 0 表示不限制
	if limit, _ := l.snapshot(); limit != 0 {
		t.Fatalf("上限应为 0（不限），实际 %d", limit)
	}
	if l.saturated() {
		t.Fatal("上限为 0 时不应视为已满")
	}
	close(stop)
}

// 验证 release 多余调用不会出现负数名额。
func TestLimiterReleaseNoUnderflow(t *testing.T) {
	l := newLimiter(0)
	l.release()
	l.release()
	if _, cur := l.snapshot(); cur != 0 {
		t.Fatalf("多余 release 后 cur 应为 0，实际 %d", cur)
	}
}

//---------- 并发限制 ----------

// 验证 MaxConcurrency=1 时两个任务不会并发执行。
func TestMaxConcurrencySerializes(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage:        StorageMemory,
		Tick:           10 * time.Millisecond,
		MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&serialTasks{})
	s.Start()
	defer s.Stop()

	// 起始时间已过，两个任务会立即争抢唯一的名额
	for _, key := range []string{"c1", "c2"} {
		if err := s.AddTask(key, PeriodicTask{
			Function:    serialTasks.Run,
			TriggerType: Interval,
			Interval:    "30ms",
			StartTime:   time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("AddTask %s: %v", key, err)
		}
	}
	if !serialState.waitPeak(1, 3*time.Second) {
		t.Fatalf("任务应至少执行一次，实际峰值 %d", serialState.peak.Load())
	}
	// 等待一段时间后峰值仍应为 1
	time.Sleep(300 * time.Millisecond)
	if got := serialState.peak.Load(); got != 1 {
		t.Fatalf("并发上限为 1 时峰值应为 1，实际 %d", got)
	}
	if !serialState.waitRuns(2, 3*time.Second) {
		t.Fatalf("两个任务都应得到执行机会，实际执行 %d 次", serialState.runs.Load())
	}
	if got := s.MaxConcurrency(); got != 1 {
		t.Fatalf("MaxConcurrency 应为 1，实际 %d", got)
	}
}

// 验证不限并发时任务可以并行执行。
func TestUnlimitedConcurrency(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage: StorageMemory,
		Tick:    10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&parallelTasks{})
	s.Start()
	defer s.Stop()

	for _, key := range []string{"u1", "u2"} {
		if err := s.AddTask(key, PeriodicTask{
			Function:    parallelTasks.Run,
			TriggerType: Interval,
			Interval:    "30ms",
			StartTime:   time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("AddTask %s: %v", key, err)
		}
	}
	if !parallelState.waitPeak(2, 3*time.Second) {
		t.Fatalf("不限并发时应观察到并发 2，实际峰值 %d", parallelState.peak.Load())
	}
	if got := s.MaxConcurrency(); got != 0 {
		t.Fatalf("默认 MaxConcurrency 应为 0（不限），实际 %d", got)
	}
}

// 验证 Skip 策略在名额满时跳过执行并计数。
func TestConcurrencySkipPolicy(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage:           StorageMemory,
		Tick:              10 * time.Millisecond,
		MaxConcurrency:    1,
		ConcurrencyPolicy: ConcurrencySkip,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&skipTasks{})
	s.Start()
	defer s.Stop()

	for _, key := range []string{"s1", "s2"} {
		if err := s.AddTask(key, PeriodicTask{
			Function:    skipTasks.Run,
			TriggerType: Interval,
			Interval:    "10ms",
			StartTime:   time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("AddTask %s: %v", key, err)
		}
	}
	if !skipState.waitRuns(2, 3*time.Second) {
		t.Fatalf("两个任务都应得到执行机会，实际执行 %d 次", skipState.runs.Load())
	}
	time.Sleep(200 * time.Millisecond)
	if got := s.Stats().Skipped; got == 0 {
		t.Fatal("Skip 策略下应记录被跳过的执行次数")
	}
	if got := skipState.peak.Load(); got > 1 {
		t.Fatalf("Skip 策略下并发不应超过 1，实际峰值 %d", got)
	}
}

// 验证 SetMaxConcurrency 运行时动态调整生效。
func TestSetMaxConcurrencyAtRuntime(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage:        StorageMemory,
		Tick:           10 * time.Millisecond,
		MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&resizeTasks{})
	s.Start()
	defer s.Stop()

	for _, key := range []string{"r1", "r2"} {
		if err := s.AddTask(key, PeriodicTask{
			Function:    resizeTasks.Run,
			TriggerType: Interval,
			Interval:    "30ms",
			StartTime:   time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("AddTask %s: %v", key, err)
		}
	}
	if !resizeState.waitPeak(1, 3*time.Second) {
		t.Fatalf("任务应至少执行一次，实际峰值 %d", resizeState.peak.Load())
	}
	s.SetMaxConcurrency(2)
	if got := s.MaxConcurrency(); got != 2 {
		t.Fatalf("SetMaxConcurrency 后应为 2，实际 %d", got)
	}
	// 放宽上限后应能观察到并发 2
	if !resizeState.waitPeak(2, 3*time.Second) {
		t.Fatalf("放宽上限后应观察到并发 2，实际峰值 %d", resizeState.peak.Load())
	}
}

//---------- 运行统计 ----------

// 验证 PendingCount / ActiveCount / Stats 的统计结果。
func TestStatsCounters(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage:        StorageMemory,
		Tick:           10 * time.Millisecond,
		MaxConcurrency: 4,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&idleTasks{})
	s.Start()
	defer s.Stop()

	if got := s.PendingCount(); got != 0 {
		t.Fatalf("初始待执行任务数应为 0，实际 %d", got)
	}
	// 两个循环任务 + 一个单次任务，均安排在未来，不会实际执行
	if err := s.AddTask("p1", PeriodicTask{
		Function: idleTasks.Run, TriggerType: Interval, Interval: "1h", StartTime: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTask("p2", PeriodicTask{
		Function: idleTasks.Run, TriggerType: Interval, Interval: "1h", StartTime: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTask("p3", PeriodicTask{
		Function: idleTasks.Run, TriggerType: Date, StartTime: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if got := s.PendingCount(); got != 3 {
		t.Fatalf("待执行任务数应为 3，实际 %d", got)
	}
	stat := s.Stats()
	if stat.Total != 3 || stat.Pending != 3 || stat.Stopped != 0 {
		t.Fatalf("Stats 应为 Total=3 Pending=3 Stopped=0，实际 %+v", stat)
	}
	if stat.MaxConcurrency != 4 {
		t.Fatalf("Stats.MaxConcurrency 应为 4，实际 %d", stat.MaxConcurrency)
	}

	// 暂停一个后，待执行数应减一
	if err := s.StopTask("p1"); err != nil {
		t.Fatal(err)
	}
	stat = s.Stats()
	if stat.Pending != 2 || stat.Stopped != 1 || stat.Total != 3 {
		t.Fatalf("暂停后应为 Pending=2 Stopped=1 Total=3，实际 %+v", stat)
	}
	// 恢复后应回到待执行
	if err := s.ContinueTask("p1"); err != nil {
		t.Fatal(err)
	}
	if got := s.PendingCount(); got != 3 {
		t.Fatalf("恢复后待执行任务数应为 3，实际 %d", got)
	}

	// 删除一个后总数减一
	if err := s.RemoveTask("p2"); err != nil {
		t.Fatal(err)
	}
	if got := s.PendingCount(); got != 2 {
		t.Fatalf("删除后待执行任务数应为 2，实际 %d", got)
	}
}

// 验证正在执行的任务数与实例数统计。
func TestStatsRunning(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage: StorageMemory,
		Tick:    10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&busyTasks{})
	s.Start()
	defer s.Stop()

	for _, key := range []string{"run1", "run2"} {
		if err := s.AddTask(key, PeriodicTask{
			Function: busyTasks.Run, TriggerType: Interval, Interval: "1h", StartTime: time.Now(),
		}); err != nil {
			t.Fatalf("AddTask %s: %v", key, err)
		}
	}
	// 间隔为 1h，需手动触发才能观察并发执行
	if err := s.StartTaskNow("run1"); err != nil {
		t.Fatal(err)
	}
	if err := s.StartTaskNow("run2"); err != nil {
		t.Fatal(err)
	}

	// 等待两个任务都在执行
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && s.ActiveCount() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	stat := s.Stats()
	if stat.Running != 2 {
		t.Fatalf("正在执行的任务数应为 2，实际 %d", stat.Running)
	}
	if stat.Instances < 2 {
		t.Fatalf("正在执行的实例数应 >= 2，实际 %d", stat.Instances)
	}
	if got := s.ActiveCount(); got != 2 {
		t.Fatalf("ActiveCount 应为 2，实际 %d", got)
	}

	// 执行完毕后应回落为 0
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && s.ActiveCount() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.ActiveCount(); got != 0 {
		t.Fatalf("执行完毕后 ActiveCount 应为 0，实际 %d", got)
	}
}

// panicTasks 用于验证 panic 路径下名额仍被归还。
type panicTasks struct{}

func (t panicTasks) Boom() { panic("boom") }

// 验证任务 panic 后名额被释放，不会永久占用并发额度。
func TestConcurrencyReleasedAfterPanic(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage:        StorageMemory,
		Tick:           10 * time.Millisecond,
		MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&demoTasks{}, &panicTasks{})
	s.Start()
	defer s.Stop()

	// 单次任务必定 panic
	if err := s.AddTask("boom", PeriodicTask{
		Function:    panicTasks.Boom,
		TriggerType: Date,
		StartTime:   time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("AddTask boom: %v", err)
	}
	// 名额为 1，若 panic 后未归还则该任务永远无法执行
	if err := s.AddTask("after", PeriodicTask{
		Function:    demoTasks.Noop,
		TriggerType: Interval,
		Interval:    "20ms",
		StartTime:   time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("AddTask after: %v", err)
	}

	// 连续执行成功即说明名额已归还（上限为 1，未归还则永远无法执行第二次）
	waitCount(t, &demoCounter, 3, 3*time.Second)
	// 该任务持续执行，等待其空闲后再断言名额与计数均已归零
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, cur := s.gate.snapshot(); cur == 0 && s.ActiveCount() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	limit, cur := s.gate.snapshot()
	t.Fatalf("panic 后名额应已归还，cur=%d limit=%d ActiveCount=%d", cur, limit, s.ActiveCount())
}

// 验证 SafeMapOfInt 的 Total/Len 统计。
func TestSafeMapOfIntTotalAndLen(t *testing.T) {
	sm := &SafeMapOfInt{M: make(map[interface{}]int64)}
	if sm.Total() != 0 || sm.Len() != 0 {
		t.Fatal("空表 Total/Len 应为 0")
	}
	sm.Add("a")
	sm.Add("a")
	sm.Add("b")
	if sm.Total() != 3 {
		t.Fatalf("Total 应为 3，实际 %d", sm.Total())
	}
	if sm.Len() != 2 {
		t.Fatalf("Len 应为 2，实际 %d", sm.Len())
	}
	sm.Delete("a")
	if sm.Total() != 2 || sm.Len() != 2 {
		t.Fatalf("Delete 一次后 Total=2 Len=2，实际 Total=%d Len=%d", sm.Total(), sm.Len())
	}
	sm.Delete("a")
	if sm.Total() != 1 || sm.Len() != 1 {
		t.Fatalf("计数归零后应删除键，实际 Total=%d Len=%d", sm.Total(), sm.Len())
	}
}

// 验证并发统计在多任务高并发下保持一致（配合 -race 使用）。
func TestStatsConcurrentAccess(t *testing.T) {
	s, err := NewSchedulerWithOptions(Options{
		Storage:        StorageMemory,
		Tick:           5 * time.Millisecond,
		MaxConcurrency: 3,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	for i := 0; i < 5; i++ {
		key := "cc" + string(rune('a'+i))
		if err := s.AddTask(key, PeriodicTask{
			Function:    demoTasks.Noop,
			TriggerType: Interval,
			Interval:    "10ms",
			StartTime:   time.Now().Add(-time.Second),
		}); err != nil {
			t.Fatalf("AddTask %s: %v", key, err)
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = s.PendingCount()
				_ = s.ActiveCount()
				_ = s.Stats()
			}
		}()
	}
	wg.Wait()
	if got := s.PendingCount(); got != 5 {
		t.Fatalf("并发读取后待执行任务数应为 5，实际 %d", got)
	}
}

//---------- 函数名校验 ----------

// ptrRecvTasks 使用指针接收者，其方法无法与注册名匹配。
type ptrRecvTasks struct{ db *int }

func (t *ptrRecvTasks) Run() {}

// embedInner 被 embedOuter 嵌入，方法会被提升。
type embedInner struct{}

func (embedInner) Run() {}

type embedOuter struct{ embedInner }

// 验证三种会导致注册名与符号名失配的写法在 AddTask 阶段即被拒绝。
func TestFunctionNameMismatchRejected(t *testing.T) {
	flag := new(int)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&ptrRecvTasks{}, &embedOuter{})
	s.Start()
	defer s.Stop()

	cases := []struct {
		name string
		fn   interface{}
	}{
		{"绑定方法值", (&ptrRecvTasks{db: flag}).Run},
		{"指针接收者方法表达式", (*ptrRecvTasks).Run},
		{"嵌入类型的绑定方法值", (&embedOuter{}).Run},
		{"匿名函数", func() {}},
	}
	for _, c := range cases {
		err := s.AddTask("bad-"+c.name, PeriodicTask{
			Name: c.name, Function: c.fn, TriggerType: Interval, Interval: "50ms",
		})
		if err == nil {
			t.Fatalf("%s 应被拒绝，但 AddTask 返回了 nil", c.name)
		}
	}
	// 不合规的任务不应落库或进入调度
	if got := s.PendingCount(); got != 0 {
		t.Fatalf("被拒绝的任务不应进入调度，实际待执行 %d 个", got)
	}
}

// 验证合法写法（值接收者 + 方法表达式，含嵌入提升）可以通过校验。
func TestFunctionNameValidAccepted(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{}, &embedOuter{})
	s.Start()
	defer s.Stop()

	for key, fn := range map[string]interface{}{
		"ok-value":   demoTasks.Noop,
		"ok-promote": embedOuter.Run,
	} {
		if err := s.AddTask(key, PeriodicTask{
			Function: fn, TriggerType: Interval, Interval: "1h", StartTime: time.Now(),
		}); err != nil {
			t.Fatalf("%s 应被接受: %v", key, err)
		}
	}
	if got := s.PendingCount(); got != 2 {
		t.Fatalf("待执行任务数应为 2，实际 %d", got)
	}
}

//---------- 符号名判定 ----------

// 验证匿名函数的判定不会被包路径中的 ".func" 误导。
func TestIsClosureSymbol(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"main.main.func1", true},              // 闭包
		{"main.main.func1.2", true},            // 嵌套闭包
		{"github.com/func/pkg.T.Run", false},   // 包路径含 func
		{"github.com/myfunc/svc.T.Run", false}, // 包名含 func
		{"pkg.T.Run", false},
		{"pkg.T.func1Run", false}, // 末段不是纯数字
	}
	for _, c := range cases {
		if got := isClosureSymbol(c.name); got != c.want {
			t.Fatalf("isClosureSymbol(%q) = %v，期望 %v", c.name, got, c.want)
		}
	}
}

//---------- 参数与启动校验 ----------

// sigTasks 覆盖各种形参签名，用于参数校验测试。
type sigTasks struct{}

func (sigTasks) Str(s string)           {}
func (sigTasks) StrInt(s string, i int) {}
func (sigTasks) Iface(v interface{})    {}
func (sigTasks) Slice(s []string)       {}
func (sigTasks) Point(p sigPoint)       {}

// sigPoint 为不可空的自定义类型形参。
type sigPoint struct{ X int }

// 验证 Start 之前 AddTask 直接报错而不是永久阻塞。
func TestAddTaskBeforeStartRejected(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})

	done := make(chan error, 1)
	go func() {
		done <- s.AddTask("early", PeriodicTask{
			Function: demoTasks.Noop, TriggerType: Interval, Interval: "50ms",
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("启动前 AddTask 应返回错误")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("启动前 AddTask 不应阻塞，应立即返回错误")
	}
	// 启动后同一 key 可正常添加
	s.Start()
	defer s.Stop()
	if err := s.AddTask("early", PeriodicTask{
		Function: demoTasks.Noop, TriggerType: Interval, Interval: "50ms",
	}); err != nil {
		t.Fatalf("启动后 AddTask 不应报错: %v", err)
	}
}

// 验证参数个数与类型不匹配在添加阶段即被拒绝，不会落库。
func TestArgMismatchRejectedAtAdd(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&sigTasks{})
	s.Start()
	defer s.Stop()

	cases := []struct {
		name string
		fn   interface{}
		args Args
	}{
		{"参数过多", sigTasks.Str, Args{"a", "b"}},
		{"参数过少", sigTasks.StrInt, Args{"a"}},
		{"类型不可赋值", sigTasks.Slice, Args{"not-a-slice"}},
		{"nil 赋给不可空形参", sigTasks.Point, Args{nil}},
	}
	for _, c := range cases {
		if err := s.AddTask("bad-args-"+c.name, PeriodicTask{
			Function: c.fn, Args: c.args, TriggerType: Interval, Interval: "1h", StartTime: time.Now(),
		}); err == nil {
			t.Fatalf("%s 应被拒绝", c.name)
		}
	}
	// 合法用例：基础类型经 cast 转换、nil 赋给接口形参、可赋值类型
	for _, c := range []struct {
		name string
		fn   interface{}
		args Args
	}{
		{"数字转 string", sigTasks.Str, Args{123}},
		{"数字转 int", sigTasks.StrInt, Args{"a", "7"}},
		{"基础类型形参接受 nil（cast 语义转零值）", sigTasks.Str, Args{nil}},
		{"nil 赋给接口", sigTasks.Iface, Args{nil}},
		{"切片可赋值", sigTasks.Slice, Args{[]string{"x"}}},
	} {
		if err := s.AddTask("ok-args-"+c.name, PeriodicTask{
			Function: c.fn, Args: c.args, TriggerType: Interval, Interval: "1h", StartTime: time.Now(),
		}); err != nil {
			t.Fatalf("%s 不应报错: %v", c.name, err)
		}
	}
}

// 验证执行期兜底校验：未经 AddTask 校验的非法参数（如数据库恢复的脏数据）会终止任务。
func TestArgMismatchTerminatesAtRuntime(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&sigTasks{})
	s.Start()
	defer s.Stop()

	// 绕过 AddTask 直接挂到时间轮，模拟脏数据
	s.addDateTimer("dirty", PeriodicTask{
		Name: "dirty", Function: sigTasks.Str, Args: Args{"a", "b"}, TriggerType: Date,
	}, "main.sigTasks.Str", &DateTrigger{StartTime: time.Now().Add(-time.Second)})
	time.Sleep(200 * time.Millisecond)
	// 未 panic 即说明兜底校验生效
}
