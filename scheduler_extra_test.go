package scheduler

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type demoTasks struct{}

var demoCounter int64

func (t demoTasks) Noop() {
	atomic.AddInt64(&demoCounter, 1)
}

func (t demoTasks) Echo(s string, i int) {
	atomic.AddInt64(&demoCounter, 1)
}

// ghostTasks 使用独立计数器，避免与其它用例的执行计数相互影响
type ghostTasks struct{}

var ghostCounter int64

func (t ghostTasks) Run() {
	atomic.AddInt64(&ghostCounter, 1)
}

// dateTasks / expiredTasks 同样使用独立计数器，保证精确断言不受其它用例影响
type dateTasks struct{}

var dateCounter int64

func (t dateTasks) Run() { atomic.AddInt64(&dateCounter, 1) }

type expiredTasks struct{}

var expiredCounter int64

func (t expiredTasks) Run() { atomic.AddInt64(&expiredCounter, 1) }

// stepTrigger 固定步长触发器：零值原型用于注册，Build 产出真实例
type stepTrigger struct {
	Step time.Duration
}

func (t *stepTrigger) Type() string { return "step" }

func (t *stepTrigger) Next(prev time.Time) time.Time { return prev.Add(t.Step) }

func (t *stepTrigger) Build(p PeriodicTask) (Trigger, error) {
	d, err := time.ParseDuration(p.TriggerConfig)
	if err != nil {
		return nil, err
	}
	return &stepTrigger{Step: d}, nil
}

var stepFactoryOnce sync.Once

// registerSeq 为重复注册测试生成唯一类型名（-count>1 时同一进程会跑多轮）。
var registerSeq atomic.Int64

// registerTriggerOnce 注册触发器类型；已注册时视为成功。
// protoTable 是包级全局状态，go test -count>1 会在同一进程内重复执行用例，
// 直接 RegisterTrigger 会在第二轮报"已注册"。
func registerTriggerOnce(t *testing.T, name string, proto Trigger) {
	t.Helper()
	if err := RegisterTrigger(name, proto); err != nil &&
		!strings.Contains(err.Error(), "已注册") {
		t.Fatalf("RegisterTrigger %s: %v", name, err)
	}
}

func registerStepTrigger(t *testing.T) {
	t.Helper()
	stepFactoryOnce.Do(func() {
		if err := RegisterTrigger("step", &stepTrigger{}); err != nil {
			t.Errorf("RegisterTrigger: %v", err)
		}
	})
}

// neverTrigger 始终无下次执行时间的触发器
type neverTrigger struct{}

func (t *neverTrigger) Type() string { return "never" }

func (t *neverTrigger) Next(time.Time) time.Time { return time.Time{} }

func (t *neverTrigger) Build(PeriodicTask) (Trigger, error) { return t, nil }

func waitCounter(t *testing.T, want int64, timeout time.Duration) {
	t.Helper()
	waitCount(t, &demoCounter, want, timeout)
}

func waitCount(t *testing.T, counter *int64, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && atomic.LoadInt64(counter) < want {
		time.Sleep(10 * time.Millisecond)
	}
	if n := atomic.LoadInt64(counter); n < want {
		t.Fatalf("任务执行计数不足：期望 >= %d，实际 %d", want, n)
	}
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("打开内存数据库失败: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1) // :memory: 模式必须单连接，否则各连接表相互隔离
	return db
}

//---------- 自定义触发器 ----------

// 验证注册的自定义触发器类型可正常调度。
func TestCustomTriggerRepeats(t *testing.T) {
	registerStepTrigger(t)
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("custom-iv", PeriodicTask{
		Name:          "custom",
		Function:      demoTasks.Noop,
		Args:          Args{},
		TriggerType:   "step",
		TriggerConfig: "50ms",
	})
	if err != nil {
		t.Fatalf("AddTask custom trigger: %v", err)
	}
	waitCounter(t, 3, 5*time.Second)
}

// 验证未注册的触发器类型被拒绝，且不落库。
func TestUnknownTriggerRejected(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	db := newTestDB(t)
	s := NewScheduler(db, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("unknown", PeriodicTask{
		Name:        "unknown",
		Function:    demoTasks.Noop,
		Args:        Args{},
		TriggerType: "no-such-type",
	})
	if err == nil {
		t.Fatal("未注册的触发器类型应返回错误")
	}
	var count int64
	db.Model(&periodicTask{}).Where("`key` = ?", "unknown").Count(&count)
	if count != 0 {
		t.Fatalf("校验失败的任务不应落库，实际 %d 条", count)
	}
}

// 验证自定义触发器任务持久化，重启后自动恢复。
func TestCustomTriggerPersistence(t *testing.T) {
	registerStepTrigger(t)
	atomic.StoreInt64(&demoCounter, 0)

	db := newTestDB(t)
	s1 := NewScheduler(db, 10*time.Millisecond, 60)
	s1.AddTaskFunction(&demoTasks{})
	s1.Start()
	err := s1.AddTask("persist-custom", PeriodicTask{
		Name:          "persist",
		Function:      demoTasks.Noop,
		Args:          Args{},
		TriggerType:   "step",
		TriggerConfig: "1h", // 步长 1 小时，重启前不会触发
	})
	if err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	// 验证 trigger 列存自定义类型名、trigger_config 存配置
	var row periodicTask
	if err := db.Where("`key` = ?", "persist-custom").First(&row).Error; err != nil {
		t.Fatalf("查询任务记录: %v", err)
	}
	if row.TriggerType != "step" || row.TriggerConfig != "1h" {
		t.Fatalf("落库值不符：trigger=%q trigger_config=%q", row.TriggerType, row.TriggerConfig)
	}
	s1.Stop()

	// 模拟服务重启：按注册名重建触发器并调度
	s2 := NewScheduler(db, 10*time.Millisecond, 60)
	s2.AddTaskFunction(&demoTasks{})
	s2.Start()
	defer s2.Stop()

	if _, ok := s2.timerList.Load("persist-custom"); !ok {
		t.Fatal("自定义触发器任务未从数据库恢复")
	}
	if err := s2.StartTaskNow("persist-custom"); err != nil {
		t.Fatalf("StartTaskNow: %v", err)
	}
	waitCounter(t, 1, 5*time.Second)
}

// 验证自定义触发器配置非法时工厂报错，任务不落库。
func TestCustomTriggerBadConfig(t *testing.T) {
	registerStepTrigger(t)
	db := newTestDB(t)
	s := NewScheduler(db, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("bad-cfg", PeriodicTask{
		Name:          "bad",
		Function:      demoTasks.Noop,
		Args:          Args{},
		TriggerType:   "step",
		TriggerConfig: "not-a-duration",
	})
	if err == nil {
		t.Fatal("非法配置应返回错误")
	}
	var count int64
	db.Model(&periodicTask{}).Where("`key` = ?", "bad-cfg").Count(&count)
	if count != 0 {
		t.Fatalf("校验失败的任务不应落库，实际 %d 条", count)
	}
}

// 验证触发器无下次执行时间时任务被拒绝。
func TestNeverTriggerRejected(t *testing.T) {
	registerTriggerOnce(t, "never", &neverTrigger{})
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("never", PeriodicTask{
		Name:        "never",
		Function:    demoTasks.Noop,
		Args:        Args{},
		TriggerType: "never",
	})
	if err == nil {
		t.Fatal("触发器无下次执行时间时 AddTask 应返回错误")
	}
}

// 验证 ResetTask 可将任务替换为自定义触发器。
func TestCustomTriggerReset(t *testing.T) {
	registerStepTrigger(t)
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	p := PeriodicTask{Function: demoTasks.Noop, Args: Args{}, StartTime: time.Now().Add(time.Hour), TriggerType: Date}
	if err := s.AddTask("to-custom", p); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	err := s.ResetTask("to-custom", PeriodicTask{
		Name:          "custom",
		Function:      demoTasks.Noop,
		Args:          Args{},
		TriggerType:   "step",
		TriggerConfig: "50ms",
	})
	if err != nil {
		t.Fatalf("ResetTask to custom trigger: %v", err)
	}
	waitCounter(t, 1, 5*time.Second)
}

// 验证可同时注册并使用多个不同的自定义触发器。
func TestMultipleCustomTriggers(t *testing.T) {
	registerTriggerOnce(t, "fast", &stepTrigger{})
	registerTriggerOnce(t, "slow", &stepTrigger{})
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	if err := s.AddTask("fast-1", PeriodicTask{Function: demoTasks.Noop, TriggerType: "fast", TriggerConfig: "50ms"}); err != nil {
		t.Fatalf("AddTask fast: %v", err)
	}
	if err := s.AddTask("slow-1", PeriodicTask{Function: demoTasks.Noop, TriggerType: "slow", TriggerConfig: "50ms"}); err != nil {
		t.Fatalf("AddTask slow: %v", err)
	}
	waitCounter(t, 2, 5*time.Second)
}

func TestRegisterTriggerValidation(t *testing.T) {
	if err := RegisterTrigger("", nil); err == nil {
		t.Fatal("空类型名应返回错误")
	}
	if err := RegisterTrigger("nil-proto", nil); err == nil {
		t.Fatal("空原型应返回错误")
	}
	if err := RegisterTrigger(Date, &stepTrigger{}); err == nil {
		t.Fatal("重复注册内置类型应返回错误")
	}
	// 类型名带上轮次，保证 -count>1 时每次都是"首次注册"
	name := fmt.Sprintf("dup-type-%d", registerSeq.Add(1))
	if err := RegisterTrigger(name, &stepTrigger{}); err != nil {
		t.Fatalf("首次注册不应报错: %v", err)
	}
	if err := RegisterTrigger(name, &stepTrigger{}); err == nil {
		t.Fatal("重复注册应返回错误")
	}
}

// 验证直接注入触发器实例：无需注册，类型名自动取实例 Type()，且可落库恢复。
func TestTriggerInstanceInjection(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	db := newTestDB(t)
	s := NewScheduler(db, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()

	// 注入内置触发器实例（零值壳即可）
	if err := s.AddTask("inject", PeriodicTask{
		Name:        "inject",
		Function:    demoTasks.Noop,
		Trigger:     &IntervalTrigger{},
		Interval:    "50ms",
		StartTime:   time.Now(),
		TriggerType: "",
	}); err != nil {
		t.Fatalf("AddTask injected trigger: %v", err)
	}
	waitCounter(t, 2, 5*time.Second)

	// 类型名应自动补全为实例的 Type() 并落库
	var row periodicTask
	if err := db.Where("`key` = ?", "inject").First(&row).Error; err != nil {
		t.Fatalf("查询任务记录: %v", err)
	}
	if row.TriggerType != Interval {
		t.Fatalf("类型名应自动补全为 %q，实际 %q", Interval, row.TriggerType)
	}
}

//---------- 存储模式（内存 / 落库）----------

// 验证启动时显式选择内存模式：即使传入 DB 也不落库。
func TestStorageModeMemoryIgnoresDB(t *testing.T) {
	db := newTestDB(t)
	s, err := NewSchedulerWithOptions(Options{
		Storage:   StorageMemory,
		DB:        db, // 内存模式下应被忽略
		Tick:      10 * time.Millisecond,
		WheelSize: 60,
	})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	if s.Mode() != "memory" {
		t.Fatalf("模式应为 memory，实际 %q", s.Mode())
	}
	if s.DB != nil {
		t.Fatal("内存模式下 DB 应被忽略为 nil")
	}
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	if err := s.AddTask("mem-only", PeriodicTask{Function: demoTasks.Noop, TriggerType: Interval, Interval: "50ms"}); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	var count int64
	db.Model(&periodicTask{}).Where("`key` = ?", "mem-only").Count(&count)
	if count != 0 {
		t.Fatalf("内存模式不应落库，实际 %d 条", count)
	}
}

// 验证启动时显式选择落库模式：任务写入数据库并可恢复。
func TestStorageModeDB(t *testing.T) {
	db := newTestDB(t)
	s, err := NewSchedulerWithOptions(Options{Storage: StorageDB, DB: db, Tick: 10 * time.Millisecond, WheelSize: 60})
	if err != nil {
		t.Fatalf("NewSchedulerWithOptions: %v", err)
	}
	if s.Mode() != "db" {
		t.Fatalf("模式应为 db，实际 %q", s.Mode())
	}
	if !db.Migrator().HasTable("periodic_task") {
		t.Fatal("落库模式应自动建表")
	}
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	if err := s.AddTask("persisted", PeriodicTask{Name: "p", Function: demoTasks.Noop, TriggerType: Date, StartTime: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	var row periodicTask
	if err := db.Where("`key` = ?", "persisted").First(&row).Error; err != nil {
		t.Fatalf("落库模式任务应写入数据库: %v", err)
	}
}

// 验证配置校验与默认值。
func TestStorageModeOptionsValidation(t *testing.T) {
	if _, err := NewSchedulerWithOptions(Options{Storage: StorageDB}); err == nil {
		t.Fatal("落库模式缺少 DB 应返回错误")
	}
	// Tick/WheelSize 缺省时使用默认值，不 panic
	s, err := NewSchedulerWithOptions(Options{Storage: StorageMemory})
	if err != nil {
		t.Fatalf("缺省配置应可用: %v", err)
	}
	if s.tw.tick != int64(defaultTick/time.Millisecond) {
		t.Fatalf("tick 应默认为 %v，实际 %d", defaultTick, s.tw.tick)
	}
	if s.tw.wheelSize != defaultWheelSize {
		t.Fatalf("wheelSize 应默认为 %d，实际 %d", defaultWheelSize, s.tw.wheelSize)
	}
	// 旧签名保持：db=nil 为内存模式
	if s := NewScheduler(nil, time.Second, 24); s.Mode() != "memory" {
		t.Fatalf("NewScheduler(nil) 应为 memory 模式，实际 %q", s.Mode())
	}
}

//---------- 内置触发器与基础行为 ----------

// 验证 date 任务立即执行且只执行一次；同时验证重复 Start 不产生副作用。
func TestDateTaskRunsOnce(t *testing.T) {
	atomic.StoreInt64(&dateCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&dateTasks{})
	s.Start()
	s.Start() // 幂等
	defer s.Stop()

	err := s.AddTask("date-once", PeriodicTask{
		Name:        "date",
		Function:    dateTasks.Run,
		Args:        Args{},
		StartTime:   time.Now().Add(-time.Second),
		TriggerType: Date,
	})
	if err != nil {
		t.Fatalf("AddTask date: %v", err)
	}
	waitCount(t, &dateCounter, 1, 5*time.Second)
	time.Sleep(200 * time.Millisecond)
	if n := atomic.LoadInt64(&dateCounter); n != 1 {
		t.Fatalf("date 任务应恰好执行 1 次，实际 %d 次", n)
	}
}

func TestIntervalTaskRepeats(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("iv", PeriodicTask{
		Name:        "interval",
		Function:    demoTasks.Noop,
		Args:        Args{},
		StartTime:   time.Now(),
		TriggerType: Interval,
		Interval:    "50ms",
	})
	if err != nil {
		t.Fatalf("AddTask interval: %v", err)
	}
	waitCounter(t, 3, 5*time.Second)
}

func TestCronTaskRuns(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("cron", PeriodicTask{
		Name:        "cron",
		Function:    demoTasks.Noop,
		Args:        Args{},
		StartTime:   time.Now(),
		TriggerType: Cron,
		Cron:        "*/1 * * * * * *", // 每秒
	})
	if err != nil {
		t.Fatalf("AddTask cron: %v", err)
	}
	waitCounter(t, 2, 5*time.Second)
}

// 修复验证：结束时间已过的任务不应被执行。
func TestExpiredIntervalTaskRejected(t *testing.T) {
	atomic.StoreInt64(&expiredCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&expiredTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("expired", PeriodicTask{
		Name:        "expired",
		Function:    expiredTasks.Run,
		Args:        Args{},
		StartTime:   time.Now().Add(-2 * time.Hour),
		EndTime:     time.Now().Add(-time.Hour),
		TriggerType: Interval,
		Interval:    "1h",
	})
	if err == nil {
		t.Fatal("过期任务应返回错误而不是被调度执行")
	}
	time.Sleep(100 * time.Millisecond)
	if n := atomic.LoadInt64(&expiredCounter); n != 0 {
		t.Fatalf("过期任务不应执行，实际执行 %d 次", n)
	}
}

// 修复验证：Interval<=0 应返回错误，而不是同一时刻无限循环触发。
func TestZeroIntervalRejected(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("zero-iv", PeriodicTask{
		Name:        "zero",
		Function:    demoTasks.Noop,
		Args:        Args{},
		StartTime:   time.Now(),
		TriggerType: Interval,
		Interval:    "0s",
	})
	if err == nil {
		t.Fatal("Interval 为 0 应返回错误")
	}
}

// 修复验证：对不存在的任务 ResetTask 等价于新增，不 panic。
func TestResetNonExistentTask(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	err := s.ResetTask("fresh", PeriodicTask{
		Name:        "fresh",
		Function:    demoTasks.Noop,
		Args:        Args{},
		StartTime:   time.Now().Add(time.Hour),
		TriggerType: Date,
	})
	if err != nil {
		t.Fatalf("ResetTask 不存在的任务应等价于新增: %v", err)
	}
	if _, ok := s.timerList.Load("fresh"); !ok {
		t.Fatal("ResetTask 新增的任务应存在于 timerList")
	}
	if err := s.RemoveTask("fresh"); err != nil {
		t.Fatalf("RemoveTask: %v", err)
	}
}

// 修复验证：无效的 Function 入参应返回错误而非 panic。
func TestAddTaskInvalidFunction(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.Start()
	defer s.Stop()

	if err := s.AddTask("nil-fn", PeriodicTask{TriggerType: Date, StartTime: time.Now()}); err == nil {
		t.Fatal("Function 为 nil 应返回错误")
	}
	if err := s.AddTask("not-fn", PeriodicTask{Function: 123, TriggerType: Date, StartTime: time.Now()}); err == nil {
		t.Fatal("Function 非函数应返回错误")
	}
}

func TestAddTaskDuplicateKey(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	p := PeriodicTask{Function: demoTasks.Noop, Args: Args{}, StartTime: time.Now().Add(time.Hour), TriggerType: Date}
	if err := s.AddTask("dup", p); err != nil {
		t.Fatalf("首次添加: %v", err)
	}
	if err := s.AddTask("dup", p); err == nil {
		t.Fatal("重复 key 应返回错误")
	}
}

func TestStopTwiceAndStartOnce(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	s.Start()
	s.Stop()
	s.Stop() // 修复验证：重复 Stop 不 panic
}

//---------- 内存模式：不落库 ----------

// 验证 DB 为 nil 时任务正常调度。
func TestMemoryModeNoDB(t *testing.T) {
	atomic.StoreInt64(&demoCounter, 0)
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	if s.DB != nil {
		t.Fatal("内存模式 DB 应为 nil")
	}
	s.AddTaskFunction(&demoTasks{})
	s.Start()
	defer s.Stop()

	if err := s.AddTask("mem", PeriodicTask{Function: demoTasks.Noop, TriggerType: Interval, Interval: "50ms"}); err != nil {
		t.Fatalf("AddTask: %v", err)
	}
	waitCounter(t, 2, 5*time.Second)
	if err := s.StopTask("mem"); err != nil {
		t.Fatalf("StopTask: %v", err)
	}
}

//---------- 工具与数据结构 ----------

func TestSafeMapOfIntConcurrent(t *testing.T) {
	sm := &SafeMapOfInt{M: make(map[interface{}]int64)}
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sm.Add("k")
		}()
	}
	wg.Wait()
	if v := sm.GetValue("k"); v != n {
		t.Fatalf("并发 Add 后计数应为 %d，实际 %d", n, v)
	}
	for i := 0; i < n; i++ {
		sm.Delete("k")
	}
	if v := sm.GetValue("k"); v != 0 {
		t.Fatalf("并发 Delete 后计数应为 0，实际 %d", v)
	}
	sm.Delete("k") // 不存在的 key 删除应为 no-op
}

func TestArgsJSONRoundTrip(t *testing.T) {
	a := Args{"x", 1, 1.5, true, nil}
	v, err := a.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	var b Args
	if err := b.Scan(v); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	aj, _ := json.Marshal([]interface{}(a))
	bj, _ := json.Marshal([]interface{}(b))
	if !reflect.DeepEqual(aj, bj) {
		t.Fatalf("round-trip 不一致: %s vs %s", aj, bj)
	}
	var c Args
	if err := c.Scan(nil); err != nil {
		t.Fatalf("Scan(nil) 不应报错: %v", err)
	}
	if len(c) != 0 {
		t.Fatalf("Scan(nil) 应得到空切片，实际 %v", c)
	}
	if err := (&Args{}).Scan(123); err == nil {
		t.Fatal("Scan 非法类型应返回错误")
	}
}

func TestTriggerNext(t *testing.T) {
	now := time.Now()
	// 结束时间已过应返回零值
	iv := &IntervalTrigger{Interval: time.Second, StartTime: now.Add(-2 * time.Hour), EndTime: now.Add(-time.Hour)}
	if !iv.Next(now).IsZero() {
		t.Fatal("过期的 IntervalTrigger 应返回零值时间")
	}
	// 非法 cron 表达式应返回零值而不是 panic
	ct := &CronTrigger{Cron: "not a cron"}
	if !ct.Next(now).IsZero() {
		t.Fatal("非法 CronTrigger 应返回零值时间")
	}
	if _, err := Parse("bad"); err == nil {
		t.Fatal("非法 cron 表达式 Parse 应返回错误")
	}
	if _, err := Parse("*/5 * * * * * *"); err != nil {
		t.Fatalf("合法 cron 表达式 Parse 不应报错: %v", err)
	}
}

func TestFormatTimeAndFunctionName(t *testing.T) {
	if got := formatTime(time.Date(2026, 10, 6, 16, 0, 0, 0, time.UTC)); got != "2026-10-06 16:00:00" {
		t.Fatalf("formatTime: %s", got)
	}
	if _, err := functionName(nil); err == nil {
		t.Fatal("functionName(nil) 应返回错误")
	}
	// 匿名函数无法被 AddTaskFunction 注册，应在 AddTask 阶段即被拒绝
	if _, err := functionName(func() {}); err == nil {
		t.Fatal("functionName(匿名函数) 应返回错误")
	}
	// 具名的值接收者方法是合法写法
	if _, err := functionName(demoTasks.Noop); err != nil {
		t.Fatalf("functionName(合法方法) 不应报错: %v", err)
	}
}

func TestAddTaskFunctionValidation(t *testing.T) {
	s := NewScheduler(nil, 10*time.Millisecond, 60)
	// 非指针参数不应 panic
	s.AddTaskFunction(demoTasks{}, nil, 123)
	s.Start()
	defer s.Stop()
	if len(s.Any) != 0 {
		t.Fatalf("非法参数不应注册任何函数，实际注册 %d 个", len(s.Any))
	}
	s.AddTaskFunction(&demoTasks{}, &ghostTasks{})
	if len(s.Any) != 3 {
		t.Fatalf("应注册 3 个方法，实际 %d 个", len(s.Any))
	}
	// 未注册的函数在执行时应被安全终止而不是 panic（用独立计数器避免跨用例污染）
	atomic.StoreInt64(&ghostCounter, 0)
	trig := &DateTrigger{StartTime: time.Now().Add(-time.Second)}
	s.addDateTimer("ghost", PeriodicTask{Name: "ghost"}, "no.such.Function", trig)
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt64(&ghostCounter); n != 0 {
		t.Fatalf("未注册函数不应被执行，实际 %d 次", n)
	}
}
