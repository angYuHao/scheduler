// Package scheduler 是一个基于层级时间轮的定时任务调度器，支持任务持久化与自定义触发器。
// Package scheduler is a cron-style job scheduler built on a hierarchical timing wheel,
// with optional database persistence and pluggable custom triggers.
package scheduler

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cast"
	"gorm.io/gorm"
)

// ---------- 任务定义 / Task definition ----------

// PeriodicTask 描述一个计划任务。
// TriggerType 取内置类型（date/interval/cron）或 RegisterTrigger 注册的自定义类型名；
// TriggerConfig 为自定义类型的配置串，由对应 Trigger.Build 解析。
// 也可直接注入 Trigger 实例（内存模式免注册），此时类型名自动取其 Type()。
// 启用数据库模式时，落库的是类型名与配置串，服务重启后可自动恢复。
//
// PeriodicTask describes a scheduled job. TriggerType is either a built-in type
// (date/interval/cron) or a custom type registered via RegisterTrigger, and
// TriggerConfig is the configuration string passed to Trigger.Build.
// A Trigger instance may be injected directly to skip registration.
type PeriodicTask struct {
	Name              string      // 任务名称 / job name
	Function          interface{} // 待调用的函数或方法 / function to invoke
	Args              Args        // 调用参数 / call arguments
	StartTime         time.Time   // 起始时间 / start time
	EndTime           time.Time   // 结束时间，零值表示不限 / end time, zero means unlimited
	Cron              string      // cron 表达式（TriggerType=cron） / cron expression
	Interval          string      // 间隔字符串，如 "5s"（TriggerType=interval） / interval string
	LimitedAliveCount int64       // 并发存活上限，<=0 不限 / max concurrent runs, <=0 means unlimited
	TriggerType       string      // 触发器类型名 / trigger type name
	TriggerConfig     string      // 触发器配置串 / trigger configuration string
	Trigger           Trigger     `gorm:"-"` // 可选的触发器实例（优先于 TriggerType）/ optional trigger instance
}

// periodicTask 为 PeriodicTask 的数据库模型。
// 接口实例不能作为数据库列，故落库 TriggerType 与 TriggerConfig。
//
// periodicTask is the database model. Trigger instances cannot be stored as
// columns, so TriggerType and TriggerConfig are persisted instead.
type periodicTask struct {
	Id                uint64 `gorm:"primaryKey;AUTO_INCREMENT"`
	Key               string `gorm:"unique;index"` // 任务唯一标识 / unique job key
	Name              string
	Function          string
	Args              Args
	StartTime         time.Time
	EndTime           time.Time
	NextRunTime       time.Time // 下次执行时间 / next fire time
	TriggerType       string    `gorm:"column:trigger;type:varchar(64)"`
	TriggerConfig     string
	Cron              string
	Interval          string
	TotalRunCount     int64 // 累计执行次数 / total run count
	LimitedAliveCount int64
	Stopped           bool // 是否已停止 / whether the job is stopped
}

func (*periodicTask) TableName() string {
	return "periodic_task"
}

// toModelStruct 将任务转换为数据库模型。
func (p PeriodicTask) toModelStruct(key, f string) periodicTask {
	return periodicTask{
		Key:               key,
		Name:              p.Name,
		Function:          f,
		Args:              p.Args,
		StartTime:         p.StartTime,
		EndTime:           p.EndTime,
		TriggerType:       p.TriggerType,
		TriggerConfig:     p.TriggerConfig,
		Cron:              p.Cron,
		Interval:          p.Interval,
		LimitedAliveCount: p.LimitedAliveCount,
	}
}

// toStruct 将数据库模型还原为任务（触发器实例在调度时重建）。
func (p periodicTask) toStruct() PeriodicTask {
	return PeriodicTask{
		Name:              p.Name,
		Args:              p.Args,
		StartTime:         p.StartTime,
		EndTime:           p.EndTime,
		TriggerType:       p.TriggerType,
		TriggerConfig:     p.TriggerConfig,
		Cron:              p.Cron,
		Interval:          p.Interval,
		LimitedAliveCount: p.LimitedAliveCount,
	}
}

// normalizeTriggerType 注入实例时补全类型名，保证任务可落库恢复。
// Fills TriggerType from the injected instance so the job stays restorable.
func (p *PeriodicTask) normalizeTriggerType() {
	if p.TriggerType == "" && p.Trigger != nil {
		p.TriggerType = p.Trigger.Type()
	}
}

// ---------- 内部事件 / Internal events ----------

// 任务执行事件的类型 / kinds of job lifecycle events.
const (
	actRun     = "run"           // 任务开始 / job started
	actNextRun = "next_run_time" // 更新下次执行时间 / next fire time refreshed
	actFinish  = "finish"        // 任务结束 / job finished
)

// action 为任务执行过程中的内部事件，由消费循环统一处理与落库。
// action is an internal lifecycle event handled and persisted by the event loop.
type action struct {
	active      string
	key         string
	stop        bool // 是否将任务标记为停止 / mark the job as stopped
	nextRunTime time.Time
}

// ---------- 调度器 / Scheduler ----------

// StorageMode 决定计划任务是否落库，启动时选定。
// StorageMode decides whether jobs are persisted; chosen at startup.
type StorageMode int

const (
	StorageMemory StorageMode = iota // 内存模式：不落库 / memory mode: no persistence
	StorageDB                        // 落库模式：写入 periodic_task 表 / DB mode: persisted
)

// String 返回存储模式名称。Returns the mode name.
func (m StorageMode) String() string {
	if m == StorageDB {
		return "db"
	}
	return "memory"
}

const (
	defaultTick      = time.Second // 默认时间轮精度 / default tick
	defaultWheelSize = 24          // 默认时间轮槽位 / default wheel size
)

// Options 为调度器配置，Storage 显式决定任务是否落库：
// StorageDB 时必须提供 DB；StorageMemory 时忽略 DB，任务一律不落库。
// MaxConcurrency 为全局同时执行任务数上限，<=0 表示不限制。
// ConcurrencyPolicy 决定达到上限时的行为（等待或跳过）。
// Options configures the scheduler; Storage explicitly selects memory or DB mode.
type Options struct {
	Storage           StorageMode
	DB                *gorm.DB          // 仅 DB 模式使用 / used in DB mode only
	Tick              time.Duration     // 0 表示默认 1s / 0 means 1s
	WheelSize         int64             // 0 表示默认 24 / 0 means 24
	MaxConcurrency    int               // 全局并发上限，<=0 不限 / global concurrency limit, <=0 unlimited
	ConcurrencyPolicy ConcurrencyPolicy // 并发满时的策略 / policy when the limit is reached
}

// Scheduler 为调度器门面。
// 存储模式在启动时选定：内存模式任务不落库，DB 模式可在重启后恢复。
// Scheduler is the scheduler facade. The storage mode is fixed at startup.
type Scheduler struct {
	Any          map[string]interface{} // 函数名 -> 反射方法 / function name to reflect.Value
	DB           *gorm.DB
	storage      StorageMode
	tw           *TimingWheel
	timerList    *sync.Map // key -> *Timer
	RunningCount *SafeMapOfInt
	do           chan action
	exitC        chan struct{}
	waitGroup    waitGroupWrapper
	// gate 限制同一时刻正在执行的任务数；policy 为满载时的行为。
	// gate caps the number of concurrently running jobs; policy defines the
	// behaviour once the cap is reached.
	gate   *limiter
	policy ConcurrencyPolicy
	// skipped 累计因并发满而跳过的执行次数 / skipped runs caused by a full gate
	skipped atomic.Int64
	// mu 保护 Any 的并发读写（注册函数与任务执行 goroutine 可能并发访问）。
	// mu guards concurrent access to Any from registration and task goroutines.
	mu        sync.RWMutex
	startOnce sync.Once
	stopOnce  sync.Once

	// started 标记调度器是否已启动：AddTask 在启动前会永久阻塞在事件通道上，
	// 必须在入口拦截。/ started guards against AddTask deadlocking before Start.
	started atomic.Bool
	// fnTable 是 Any 的不可变快照，任务执行路径无锁读取，
	// 每次 AddTaskFunction 后整体重建。
	// fnTable is an immutable snapshot of Any, rebuilt on each registration.
	fnTable atomic.Pointer[map[string]reflect.Value]
}

// NewScheduler 创建调度器，按 DB 是否为 nil 隐式选择存储模式（等价于显式指定 Storage）。
// db 为 nil 即内存模式；tick 为时间轮切片精度；wheelSize 为时间轮槽位数。
// 注意：添加任务前必须先调用 Start。
// NewScheduler creates a scheduler, inferring the storage mode from db.
func NewScheduler(db *gorm.DB, tick time.Duration, wheelSize int64) *Scheduler {
	mode := StorageMemory
	if db != nil {
		mode = StorageDB
	}
	s, err := NewSchedulerWithOptions(Options{Storage: mode, DB: db, Tick: tick, WheelSize: wheelSize})
	if err != nil {
		// 与时间轮保持一致：配置非法直接 panic / panic on invalid configuration
		panic(err)
	}
	return s
}

// NewSchedulerWithOptions 按配置创建调度器，在启动时显式选择内存或落库模式。
// Tick/WheelSize 为 0 时使用默认值。
// NewSchedulerWithOptions creates a scheduler with an explicit storage mode.
func NewSchedulerWithOptions(opt Options) (*Scheduler, error) {
	if opt.Storage == StorageDB && opt.DB == nil {
		return nil, errors.New("存储模式为 db 时必须提供 DB 连接")
	}
	tick := opt.Tick
	if tick <= 0 {
		tick = defaultTick
	}
	wheelSize := opt.WheelSize
	if wheelSize <= 0 {
		wheelSize = defaultWheelSize
	}
	db := opt.DB
	if opt.Storage == StorageMemory {
		db = nil // 内存模式：即使传入 DB 也不落库 / memory mode never persists
	} else if err := db.AutoMigrate(&periodicTask{}); err != nil {
		return nil, fmt.Errorf("创建数据库失败：%v", err)
	}
	return &Scheduler{
		Any:          make(map[string]interface{}, 1),
		DB:           db,
		storage:      opt.Storage,
		tw:           NewTimingWheel(tick, wheelSize),
		timerList:    &sync.Map{},
		RunningCount: &SafeMapOfInt{M: make(map[interface{}]int64)},
		do:           make(chan action),
		exitC:        make(chan struct{}),
		gate:         newLimiter(opt.MaxConcurrency),
		policy:       opt.ConcurrencyPolicy,
		// fnTable 保持零值即可：快照为空时 lookupFunction 会回退读 Any。
		// A zero fnTable is fine; lookups then fall back to Any.
	}, nil
}

// Mode 返回当前存储模式名称（memory / db）。
// Mode returns the current storage mode name.
func (s *Scheduler) Mode() string {
	return s.storage.String()
}

// ---------- 并发控制 / Concurrency control ----------

// ConcurrencyPolicy 决定达到全局并发上限时的行为。
// ConcurrencyPolicy defines what happens when the concurrency limit is reached.
type ConcurrencyPolicy int

const (
	// ConcurrencyBlock 等待空闲名额（默认）：任务排队，不会丢失。
	// ConcurrencyBlock waits for a free slot (default); jobs queue instead of being lost.
	ConcurrencyBlock ConcurrencyPolicy = iota
	// ConcurrencySkip 直接跳过本次执行：适合允许丢弃的轮询类任务。
	// ConcurrencySkip drops the run; suitable for polling jobs that may be skipped.
	ConcurrencySkip
)

// MaxConcurrency 返回当前全局并发上限，<=0 表示不限制。
// MaxConcurrency returns the current global concurrency limit; <=0 means unlimited.
func (s *Scheduler) MaxConcurrency() int {
	limit, _ := s.gate.snapshot()
	return limit
}

// SetMaxConcurrency 动态调整全局并发上限，<=0 表示不限制，立即生效。
// 缩限时已在执行的任务不会被中断，仅阻止新的执行。
// SetMaxConcurrency resizes the global concurrency limit at runtime.
// Shrinking does not interrupt running jobs; it only blocks new ones.
func (s *Scheduler) SetMaxConcurrency(n int) {
	s.gate.setLimit(n)
	logrus.Infof("调度器并发上限已调整为：%d", n)
}

// Stats 返回调度器当前的运行统计。
// Stats returns a snapshot of the scheduler runtime counters.
type Stats struct {
	Total          int   // 内存中的任务总数（含已暂停）/ total jobs in memory, including paused
	Pending        int   // 待执行任务数（已启用、未暂停）/ pending jobs, enabled and not paused
	Stopped        int   // 已暂停任务数 / paused jobs
	Running        int   // 正在执行的任务数（按任务去重）/ running jobs, deduplicated by key
	Instances      int64 // 正在执行的实例总数（含同一任务的多次并发）/ running executions
	MaxConcurrency int   // 全局并发上限，0 表示不限 / global limit, 0 means unlimited
	Skipped        int64 // 因并发满被跳过的执行次数 / runs dropped because the limit was reached
}

// Stats 返回调度器运行统计快照。
// Stats returns a snapshot of the scheduler counters.
func (s *Scheduler) Stats() Stats {
	limit, _ := s.gate.snapshot()
	running, instances := s.RunningCount.counts()
	st := Stats{
		Running:        running,
		Instances:      instances,
		MaxConcurrency: limit,
		Skipped:        s.skipped.Load(),
	}
	s.timerList.Range(func(_, v interface{}) bool {
		st.Total++
		if v.(*Timer).isStopped() {
			st.Stopped++
		} else {
			st.Pending++
		}
		return true
	})
	return st
}

// PendingCount 返回待执行任务数（已启用、未暂停）。
// PendingCount returns the number of enabled, non-paused jobs.
func (s *Scheduler) PendingCount() int { return s.Stats().Pending }

// RunningCount 返回正在执行的任务数（按任务去重）。
// 注意：与字段 RunningCount 同名冲突，故命名为 ActiveCount。
// RunningCount of distinct jobs currently running.
// Named ActiveCount to avoid clashing with the RunningCount field.
func (s *Scheduler) ActiveCount() int { return s.RunningCount.Len() }

// acquireSlot 申请一个全局执行名额。
// ConcurrencyBlock 策略下会阻塞等待空闲名额，调度器停止时返回 false；
// ConcurrencySkip 策略下名额已满则跳过并计数，返回 false。
// Acquires a global execution slot, honoring the configured policy.
func (s *Scheduler) acquireSlot() bool {
	if s.policy == ConcurrencySkip && s.gate.saturated() {
		s.skipped.Add(1)
		return false
	}
	return s.gate.acquire(s.exitC)
}

// Start 启动调度器（幂等）：启动时间轮与事件消费循环，并从数据库恢复任务。
// 注意：必须先 Start 再添加任务。
// Start starts the scheduler (idempotent). Add tasks only after Start.
func (s *Scheduler) Start() {
	s.startOnce.Do(func() {
		logrus.Infof("调度器启动：运行模式=%s", s.storage)
		s.started.Store(true)
		s.tw.start()
		s.waitGroup.Wrap(func() {
			for {
				select {
				case a := <-s.do:
					s.handleAction(a)
				case <-s.exitC:
					s.tw.stop()
					return
				}
			}
		})
		s.InitFromDB()
	})
}

// Stop 停止调度器（幂等）。正在执行的任务不会被等待。
// Stop stops the scheduler (idempotent). Running jobs are not awaited.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.exitC)
	})
	s.waitGroup.Wait()
}

// InitFromDB 从数据库恢复所有未停止的任务到内存（仅 DB 模式有效）。
// InitFromDB restores all non-stopped jobs from the database (DB mode only).
func (s *Scheduler) InitFromDB() {
	if s.DB == nil {
		return
	}
	var pTasks []periodicTask
	// 必须用 Find 拉取全部记录，First 只会返回第一条。
	// Find (not First) is required to load all rows.
	err := s.DB.Model(&periodicTask{}).Where("stopped = ?", false).Find(&pTasks).Error
	if err != nil {
		logrus.Warn("初始化数据库中的计划任务告警：", err)
		return
	}
	for _, pt := range pTasks {
		p := pt.toStruct()
		trig, err := BuildTrigger(p)
		if err != nil {
			logrus.Warn(fmt.Sprintf("任务 [%s:%s] 恢复失败：%v", pt.Key, p.Name, err))
			continue
		}
		if err := s.scheduleTask(pt.Key, p, pt.Function, trig, "恢复告警"); err != nil {
			logrus.Warn(err)
		}
	}
}

// handleAction 处理任务生命周期事件并落库。
// 注意：运行计数不在此处更新——事件循环是异步的，会导致统计滞后，
// 因此 RunningCount 由 callFunction 在执行路径上同步维护。
// 落库失败仅记录日志，不影响任务执行。
// handleAction processes lifecycle events and persists them. Running counters
// are updated synchronously in callFunction so that statistics stay accurate.
func (s *Scheduler) handleAction(a action) {
	switch a.active {
	case actRun:
		logErr(s.dbUpdateColumns(a.key, map[string]interface{}{
			"total_run_count": gorm.Expr("total_run_count + ?", 1),
		}, "total_run_count"))
	case actNextRun:
		logErr(s.dbUpdateColumns(a.key, map[string]interface{}{
			"next_run_time": a.nextRunTime,
		}, "next_run_time"))
	case actFinish:
		if a.stop {
			logErr(s.dbUpdateColumns(a.key, map[string]interface{}{
				"stopped":       true,
				"next_run_time": nil,
			}, "stopped"))
		}
	}
}

// logErr 记录非致命错误。Logs a non-fatal error.
func logErr(err error) {
	if err != nil {
		logrus.Error(err)
	}
}

// sendAction 将事件发送到消费循环；调度器已 Stop 时直接丢弃，避免 goroutine 永久阻塞。
// Drops the event when the scheduler is stopped to avoid blocked goroutines.
func (s *Scheduler) sendAction(a action) {
	select {
	case s.do <- a:
	case <-s.exitC:
	}
}

// ---------- 对外 API / Public API ----------

// AddTaskFunction 注册结构体的所有方法，使其可被任务按函数名调用。
// 参数必须为结构体指针。
// AddTaskFunction registers all methods of the given struct pointers so that
// jobs can invoke them by function name.
func (s *Scheduler) AddTaskFunction(structs ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range structs {
		getType := reflect.TypeOf(st)
		if getType == nil || getType.Kind() != reflect.Ptr || getType.Elem().Kind() != reflect.Struct {
			logrus.Error("AddTaskFunction 参数必须为结构体指针，已跳过：", st)
			continue
		}
		getValue := reflect.ValueOf(st)
		structName := fmt.Sprintf("%s.%s", getType.Elem().PkgPath(), getType.Elem().Name())
		for i := 0; i < getType.NumMethod(); i++ {
			m := getType.Method(i)
			s.Any[fmt.Sprintf("%s.%s", structName, m.Name)] = getValue.Method(i)
		}
	}
	// 重建不可变快照，任务执行路径即可无锁读取。
	// Rebuild the immutable snapshot so the run path reads it lock-free.
	tbl := make(map[string]reflect.Value, len(s.Any))
	for k, v := range s.Any {
		if fv, ok := v.(reflect.Value); ok {
			tbl[k] = fv
		}
	}
	s.fnTable.Store(&tbl)
}

// AddTask 添加任务；触发器校验失败时不会落库。
// AddTask adds a job. The job is not persisted when trigger validation fails.
func (s *Scheduler) AddTask(key string, p PeriodicTask) error {
	return s.addOrReset(key, p, false)
}

// ResetTask 重置任务；任务不存在时等价于新增。
// ResetTask updates a job, or adds it when it does not exist yet.
func (s *Scheduler) ResetTask(key string, p PeriodicTask) error {
	return s.addOrReset(key, p, true)
}

// addOrReset 是 AddTask / ResetTask 的共同实现。
// replace 为 true 时允许覆盖已存在的任务（ResetTask），否则要求 key 不冲突（AddTask）。
// addOrReset is the shared implementation of AddTask and ResetTask.
func (s *Scheduler) addOrReset(key string, p PeriodicTask, replace bool) error {
	op := "添加异常"
	if replace {
		op = "更新异常"
	} else if _, ok := s.timerList.Load(key); ok {
		return fmt.Errorf("内存中已存在标志 {key: %s} 的计划，请使用不一样的 key", key)
	}
	// 未启动时事件通道没有消费者，继续执行会永久阻塞，必须在此拦截。
	// The event channel has no consumer before Start; proceeding would deadlock.
	if !s.started.Load() {
		return fmt.Errorf("调度器尚未启动，请先调用 Start 再%s任务（key: %s）",
			map[bool]string{true: "重置", false: "添加"}[replace], key)
	}
	f, err := functionName(p.Function)
	if err != nil {
		return wrapTaskErr(key, p.Name, "无效的 Function", err)
	}
	s.warnUnregistered(key, p, f)
	// 函数已注册时提前校验参数个数与类型，把执行期的 panic 前移到添加阶段。
	// When the function is registered, validate arity and types up front.
	if err := s.validateArgs(key, p, f); err != nil {
		return err
	}
	p.normalizeTriggerType()
	// 先构造触发器做参数校验，通过后再落库，避免留下无法调度的脏记录。
	// Build the trigger first so invalid jobs never reach the database.
	trig, err := BuildTrigger(p)
	if err != nil {
		return wrapTaskErr(key, p.Name, op, err)
	}
	t, loaded := s.timerList.Load(key)
	if !loaded {
		// 内存中不存在：ResetTask 需清除库中可能的历史残留记录后按新增处理。
		// AddTask 不做删除，若库中已存在同 key 记录会由唯一索引直接报错，
		// 避免静默覆盖他人数据。
		// On reset, stale rows are removed first. AddTask keeps them so a
		// duplicate key surfaces as a unique-index error instead of an overwrite.
		if replace && s.DB != nil {
			s.DB.Where("`key` = ?", key).Delete(&periodicTask{})
		}
		if err := s.dbCreateTask(key, p, f); err != nil {
			return err
		}
		return s.scheduleTask(key, p, f, trig, "添加异常")
	}
	if err := s.dbUpdateTask(key, p, f); err != nil {
		return err
	}
	if !t.(*Timer).stop() {
		return fmt.Errorf("更新任务[%s]失败：任务已触发或已不存在", key)
	}
	// 旧 timer 随即被 scheduleTask 新建的 timer 取代，无需重置其停止标记。
	// The old timer is replaced by a fresh one, so its flag needs no reset.
	s.timerList.Delete(key)
	return s.scheduleTask(key, p, f, trig, op)
}

// warnUnregistered 在函数尚未注册时告警。
// 允许先 AddTask 后 AddTaskFunction 的用法，故不阻断流程，
// 但缺失后任务每次执行都会失败，需尽早暴露。
// Warns when the function is not registered yet without blocking the call.
func (s *Scheduler) warnUnregistered(key string, p PeriodicTask, fname string) {
	if !s.lookupFunction(fname).IsValid() {
		logrus.Warn(fmt.Sprintf("任务 [%s:%s] 的函数 %q 尚未通过 AddTaskFunction 注册，"+
			"若注册前触发将无法执行", key, p.Name, fname))
	}
}

// validateArgs 在函数已注册的前提下校验参数个数与类型可赋值性。
// 仅注册后才可能完整校验；未注册的函数在执行期仍会兜底校验。
// Validates arity and assignability when the function is registered.
func (s *Scheduler) validateArgs(key string, p PeriodicTask, fname string) error {
	fv := s.lookupFunction(fname)
	if !fv.IsValid() {
		return nil // 未注册，交由执行期校验 / not registered yet, checked at run time
	}
	if _, err := buildInputs(fv, p.Args); err != nil {
		return wrapTaskErr(key, p.Name, "无效的 Args", err)
	}
	return nil
}

// lookupFunction 无锁读取函数快照表；快照未命中时回退读 Any，
// 兼容直接向公开字段 s.Any 写入的用法。
// Looks up the snapshot first, falling back to Any for direct field writes.
func (s *Scheduler) lookupFunction(name string) reflect.Value {
	if tbl := s.fnTable.Load(); tbl != nil {
		if fv, ok := (*tbl)[name]; ok {
			return fv
		}
	}
	s.mu.RLock()
	v := s.Any[name]
	s.mu.RUnlock()
	fv, _ := v.(reflect.Value)
	return fv
}

// RemoveTask 删除任务（内存 + 数据库）。
// RemoveTask removes a job from memory and the database.
func (s *Scheduler) RemoveTask(key string) error {
	t, ok := s.timerList.Load(key)
	if !ok {
		return errTaskNotFound(key)
	}
	if err := s.dbDeleteTask(key); err != nil {
		return err
	}
	if !t.(*Timer).stop() {
		return fmt.Errorf("删除任务[%s]失败：任务已触发或已不存在", key)
	}
	s.timerList.Delete(key)
	return nil
}

// StartTaskNow 立即执行一次任务，不改变原调度计划。
// StartTaskNow runs a job immediately without changing its schedule.
func (s *Scheduler) StartTaskNow(key string) error {
	t, ok := s.timerList.Load(key)
	if !ok {
		return errTaskNotFound(key)
	}
	go t.(*Timer).function(true)
	return nil
}

// StopTask 暂停任务（内存 + 数据库）。
// StopTask pauses a job.
func (s *Scheduler) StopTask(key string) error {
	t, ok := s.timerList.Load(key)
	if !ok {
		return errTaskNotFound(key)
	}
	if err := s.dbUpdateColumns(key, map[string]interface{}{"stopped": true}, "暂停"); err != nil {
		return err
	}
	if !t.(*Timer).stop() {
		return fmt.Errorf("暂停任务[%s]失败：任务已触发或已处于暂停状态", key)
	}
	t.(*Timer).setStopped(true)
	return nil
}

// ContinueTask 恢复已暂停的任务。
// ContinueTask resumes a paused job.
func (s *Scheduler) ContinueTask(key string) error {
	t, ok := s.timerList.Load(key)
	if !ok {
		return errTaskNotFound(key)
	}
	if err := s.dbUpdateColumns(key, map[string]interface{}{"stopped": false}, "继续"); err != nil {
		return err
	}
	s.tw.addOrRun(t.(*Timer))
	t.(*Timer).setStopped(false)
	return nil
}

// ---------- 任务执行 / Job execution ----------

// functionName 通过反射获取函数的可注册名称，并校验入参。
// Returns the registrable function name via reflection.
func functionName(f interface{}) (string, error) {
	if f == nil {
		return "", errors.New("Function 不能为 nil")
	}
	v := reflect.ValueOf(f)
	if v.Kind() != reflect.Func || v.IsNil() {
		return "", errors.New("Function 必须为非 nil 的函数")
	}
	fn := runtime.FuncForPC(v.Pointer())
	if fn == nil {
		return "", errors.New("无法解析 Function 的名称")
	}
	name := fn.Name()
	if err := checkFunctionName(name); err != nil {
		return "", err
	}
	return name, nil
}

// checkFunctionName 校验函数的运行时符号名能否与 AddTaskFunction 注册的 key 匹配。
// 注册 key 由 PkgPath + Elem().Name() + MethodName 拼接而成，
// 与编译器符号名（runtime.FuncForPC）在以下情形会分叉，必须提前拦截，
// 否则任务会正常落库却在每次执行时静默失败。
//
// checkFunctionName rejects symbol names that cannot match the keys written by
// AddTaskFunction, so that the mismatch surfaces at AddTask time instead of
// silently failing on every run.
func checkFunctionName(name string) error {
	// 绑定方法值 tk.Run：编译器生成的包装符号带 -fm 后缀，
	// 应改用方法表达式 T.Run。/ Bound method value: use the method expression instead.
	if strings.HasSuffix(name, "-fm") {
		return fmt.Errorf("Function 是绑定方法值（符号名 %s），无法匹配已注册的函数名；"+
			"请改用方法表达式，例如 T.Run 代替 tk.Run", name)
	}
	// 指针接收者 (*T).Run：注册名不含括号包裹的 *，无法匹配。
	// Pointer receiver: the registered key never contains the parenthesized star.
	if i := strings.LastIndex(name, ")."); i > 0 && strings.Contains(name[:i], "(*") {
		return fmt.Errorf("Function 是指针接收者方法（符号名 %s），无法匹配已注册的函数名；"+
			"请改为值接收者定义方法：func (t T) Run()", name)
	}
	// 匿名函数无法被 AddTaskFunction 注册；闭包符号名的末段形如 func1 / func1.2。
	// Anonymous functions can never be registered; closures are named ...func1 / func1.2.
	if isClosureSymbol(name) {
		return fmt.Errorf("Function 是匿名函数（符号名 %s），无法被 AddTaskFunction 注册；"+
			"请定义具名方法后再使用", name)
	}
	return nil
}

// isClosureSymbol 判断符号名是否为编译器生成的匿名函数（末段为 func + 数字/点）。
// 不能用 strings.Contains(name, ".func") 判断——包路径中含 ".func"
// （如 github.com/func/pkg）会造成误判。
// Reports whether the symbol is a compiler-generated closure. A plain
// Contains(".func") check would misfire on package paths such as
// github.com/func/pkg.
func isClosureSymbol(name string) bool {
	// 逐段检查：闭包符号为 <外层符号>.funcN[.M]，
	// 只要某一段是 "func"+纯数字即可判定（跳过首段，首段是包名）。
	// Scan every dot-separated segment: a closure symbol looks like
	// <outer>.funcN[.M], so any "func"+digits segment marks it.
	for _, seg := range strings.Split(name, ".")[1:] {
		rest, ok := strings.CutPrefix(seg, "func")
		if !ok || rest == "" {
			continue
		}
		digits := true
		for _, c := range rest {
			if c < '0' || c > '9' {
				digits = false
				break
			}
		}
		if digits {
			return true
		}
	}
	return false
}

// errTaskNotFound 统一的任务不存在错误。
// The shared "job not found" error.
func errTaskNotFound(key string) error {
	return fmt.Errorf("内存中不存在标志 {key: %s} 的计划任务，请确认该 key 值，或者重启服务再尝试", key)
}

// wrapTaskErr 统一的任务错误包装。
// Wraps a job error with consistent context.
func wrapTaskErr(key, name, op string, err error) error {
	if name == "" {
		return fmt.Errorf("任务 [%s] %s：%v", key, op, err)
	}
	return fmt.Errorf("任务 [%s:%s] %s：%v", key, name, op, err)
}

// argCasters 将任务参数转换为目标形参的基础类型。
// JSON 反序列化后数字为 float64，这里统一转换后再调用。
// argCasters converts job arguments to the parameter's basic type.
var argCasters = map[string]func(interface{}) reflect.Value{
	"int":     func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToInt(v)) },
	"int8":    func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToInt8(v)) },
	"int16":   func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToInt16(v)) },
	"int32":   func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToInt32(v)) },
	"int64":   func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToInt64(v)) },
	"uint":    func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToUint(v)) },
	"uint8":   func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToUint8(v)) },
	"uint16":  func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToUint16(v)) },
	"uint32":  func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToUint32(v)) },
	"uint64":  func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToUint64(v)) },
	"float32": func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToFloat32(v)) },
	"float64": func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToFloat64(v)) },
	"string":  func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToString(v)) },
	"bool":    func(v interface{}) reflect.Value { return reflect.ValueOf(cast.ToBool(v)) },
}

// callFunction 组装参数并调用任务函数，同时上报开始/结束事件。
// 执行前先申请全局并发名额：ConcurrencyBlock 策略下排队等待，
// ConcurrencySkip 策略下名额已满则直接返回（不计入执行次数）。
// 运行计数与结束事件统一由 defer 维护，保证 panic 路径下也能正确回收名额。
// Builds the arguments, invokes the job and emits start/finish events.
// It acquires a global slot first; the deferred cleanup releases the slot and
// emits the finish event even when the job panics.
func (s *Scheduler) callFunction(key string, p PeriodicTask, f string, stop bool) {
	if !s.acquireSlot() {
		return
	}
	// terminated 表示任务是否应终止，除越过 EndTime 外，
	// 函数缺失或参数不匹配也会置为 true（与历史行为一致）。
	terminated := stop
	defer func() {
		s.gate.release()
		s.RunningCount.Delete(key)
		// 只在任务需要终止时上报：handleAction 的 actFinish 分支仅在 stop 为真时落库，
		// 其余情况发送事件纯属空转（原先每次执行都会多一次无意义的通道往返）。
		// Only report when the job terminates: the actFinish branch persists data
		// solely when stop is true, so any other send is pure overhead.
		if terminated {
			s.sendAction(action{active: actFinish, key: key, stop: true})
		}
	}()
	s.RunningCount.Add(key)

	logrus.Info(fmt.Sprintf("执行任务 [%s:%s]", key, p.Name))
	s.sendAction(action{active: actRun, key: key})

	fv := s.lookupFunction(f)
	if !fv.IsValid() {
		logrus.Error(fmt.Sprintf("任务 [%s:%s] 找不到已注册的函数 %q，任务终止", key, p.Name, f))
		terminated = true
		return
	}
	// 参数个数与类型在执行期兜底校验：任务可能从数据库恢复，
	// 其 Args 经过 JSON 反序列化，类型未必与添加时一致。
	// Args restored from JSON may differ in type from what was added,
	// so arity/assignability are re-checked here as a safety net.
	inputs, err := buildInputs(fv, p.Args)
	if err != nil {
		logrus.Error(wrapTaskErr(key, p.Name, "参数异常，任务终止", err))
		terminated = true
		return
	}

	fv.Call(inputs)

	logrus.Info(fmt.Sprintf("完成任务 [%s:%s]", key, p.Name))
}

// buildInputs 校验参数个数与类型，并组装反射调用入参。
// 基础类型经 argCasters 转换（JSON 反序列化后数字统一为 float64），
// 其余参数要求可赋值给形参；nil 实参仅允许赋给可空类型形参。
// 注意：不处理可变参数函数——与历史行为一致，超出固定形参个数即报错。
// buildInputs validates arity and assignability, then builds call arguments.
// Basic types go through argCasters (JSON numbers arrive as float64); other
// arguments must be assignable, and nil is only allowed for nilable params.
// Variadic functions are not supported, matching the historical behaviour.
func buildInputs(fv reflect.Value, args Args) ([]reflect.Value, error) {
	fnType := fv.Type()
	if len(args) > fnType.NumIn() {
		return nil, fmt.Errorf("参数个数(%d)超出函数 %q 的形参个数(%d)", len(args), fnType, fnType.NumIn())
	}
	if len(args) < fnType.NumIn() {
		return nil, fmt.Errorf("参数个数(%d)少于函数 %q 的形参个数(%d)", len(args), fnType, fnType.NumIn())
	}
	inputs := make([]reflect.Value, len(args))
	for i, v := range args {
		pt := fnType.In(i)
		if c, ok := argCasters[pt.Name()]; ok {
			inputs[i] = c(v)
			continue
		}
		if v == nil {
			if isNilable(pt) {
				inputs[i] = reflect.Zero(pt)
				continue
			}
			return nil, fmt.Errorf("第 %d 个参数为 nil，无法赋给不可空的形参 %s", i+1, pt)
		}
		at := reflect.TypeOf(v)
		if !at.AssignableTo(pt) {
			return nil, fmt.Errorf("第 %d 个参数类型 %s 无法赋给形参 %s", i+1, at, pt)
		}
		inputs[i] = reflect.ValueOf(v)
	}
	return inputs, nil
}

// isNilable 判断类型是否可接受 nil 实参。
// Reports whether a nil argument can be assigned to the given type.
func isNilable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface,
		reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		return true
	}
	return false
}

// logNextRun 记录任务的下次预计执行时间。
// Logs the next fire time of a job.
func logNextRun(key, name, label string, t time.Time) {
	logrus.Info(fmt.Sprintf("任务 [%s:%s] %s：%s", key, name, label, formatTime(t)))
}

// scheduleTask 将已构造好的任务挂到时间轮：date 执行一次，其余触发器循环执行。
// Schedules a job on the timing wheel: date fires once, others repeat.
func (s *Scheduler) scheduleTask(key string, p PeriodicTask, f string, trig Trigger, op string) error {
	if dt, ok := trig.(*DateTrigger); ok {
		s.addDateTimer(key, p, f, dt)
		return nil
	}
	if err := s.addRepeatingTimer(key, p, f, trig); err != nil {
		return wrapTaskErr(key, p.Name, op, err)
	}
	return nil
}

// addDateTimer 添加单次任务：到期执行一次后终止。
// Adds a one-shot job that terminates after its first run.
func (s *Scheduler) addDateTimer(key string, p PeriodicTask, f string, trig *DateTrigger) {
	expiration := trig.Next(time.Now())
	logNextRun(key, p.Name, "预计执行时间", expiration)

	var t *Timer
	t = &Timer{
		expiration: expiration.UnixMilli(),
		function: func(bool) {
			defer s.recoverTaskPanic(key)
			// 单次任务执行完毕即结束，标记为已停止使其不再计入待执行数量。
			// The one-shot job is finished; flag it so it no longer counts as pending.
			defer t.setStopped(true)
			s.callFunction(key, p, f, true)
		},
	}
	s.tw.addOrRun(t)
	s.timerList.Store(key, t)
}

// addRepeatingTimer 添加循环任务：每次执行后按触发器计算并调度下一次。
// Adds a repeating job, rescheduling itself after each run.
func (s *Scheduler) addRepeatingTimer(key string, p PeriodicTask, f string, trig Trigger) error {
	expiration := trig.Next(time.Now())
	if expiration.IsZero() {
		return errors.New("无有效的下次执行时间（起始/结束时间配置或触发表达式无效）")
	}
	s.sendAction(action{active: actNextRun, key: key, nextRunTime: expiration})
	logNextRun(key, p.Name, "预计执行时间", expiration)

	// t 需先声明后赋值：function 闭包内引用 t 自身以重新调度。
	var t *Timer
	t = &Timer{
		expiration: expiration.UnixMilli(),
		function: func(startNow bool) {
			stop := false
			defer s.recoverTaskPanic(key)
			// 任务终止后标记为已停止，使其不再计入待执行数量。
			// Flag the job once it terminates so it stops counting as pending.
			defer func() {
				if stop {
					t.setStopped(true)
				}
			}()

			if !startNow {
				expiration := trig.Next(time.Now())
				logNextRun(key, p.Name, "下次预计执行时间", expiration)
				if !expiration.IsZero() {
					t.expiration = expiration.UnixMilli()
					s.tw.addOrRun(t)
					s.sendAction(action{active: actNextRun, key: key, nextRunTime: expiration})
				} else {
					stop = true // 已越过结束时间，执行本次后终止
				}
			}

			// 任务自身并发存活数达上限则跳过本次执行。
			if p.LimitedAliveCount <= 0 || s.RunningCount.GetValue(key) < p.LimitedAliveCount {
				s.callFunction(key, p, f, stop)
			} else {
				logrus.Info(fmt.Sprintf("跳过任务 [%s:%s]", key, p.Name))
			}
		},
	}
	s.tw.addOrRun(t)
	s.timerList.Store(key, t)
	return nil
}

// recoverTaskPanic 统一处理任务 panic：仅记录日志。
// 名额释放、运行计数回收与结束事件均由 callFunction 的 defer 负责，
// 因此这里不需要（也不能重复）发送 actFinish。
//
// recoverTaskPanic logs a job panic. Slot release, counter cleanup and the
// finish event are handled by the deferred cleanup in callFunction.
func (s *Scheduler) recoverTaskPanic(key string) {
	if err := recover(); err != nil {
		logrus.Error("任务：", key, "异常退出：", err)
	}
}

// ---------- 持久化（内存模式下均为空操作）----------
// Persistence helpers; all no-ops in memory mode ----------

// dbCreateTask 写入任务记录。Writes a job row.
func (s *Scheduler) dbCreateTask(key string, p PeriodicTask, f string) error {
	if s.DB == nil {
		return nil
	}
	model := p.toModelStruct(key, f)
	if err := s.DB.Model(&periodicTask{}).Create(&model).Error; err != nil {
		return wrapTaskErr(key, p.Name, "写入数据库异常", err)
	}
	return nil
}

// dbUpdateTask 全量更新任务记录。
// 用 map 显式更新，避免 gorm struct 更新忽略零值字段（如 stopped=false、Cron=""）。
// Updates all job columns; a map is used because gorm skips zero-valued struct fields.
func (s *Scheduler) dbUpdateTask(key string, p PeriodicTask, f string) error {
	if s.DB == nil {
		return nil
	}
	updates := map[string]interface{}{
		"name":                p.Name,
		"function":            f,
		"args":                p.Args,
		"start_time":          p.StartTime,
		"end_time":            p.EndTime,
		"trigger":             p.TriggerType,
		"trigger_config":      p.TriggerConfig,
		"cron":                p.Cron,
		"interval":            p.Interval,
		"limited_alive_count": p.LimitedAliveCount,
		"stopped":             false,
	}
	if err := s.DB.Model(&periodicTask{}).Where("`key` = ?", key).Updates(updates).Error; err != nil {
		return wrapTaskErr(key, p.Name, "更新数据库异常", err)
	}
	return nil
}

// dbDeleteTask 删除任务记录。Deletes a job row.
func (s *Scheduler) dbDeleteTask(key string) error {
	if s.DB == nil {
		return nil
	}
	if err := s.DB.Where("`key` = ?", key).Delete(&periodicTask{}).Error; err != nil {
		return fmt.Errorf("删除任务[%s]异常：%v", key, err)
	}
	return nil
}

// dbUpdateColumns 更新任务行指定列，what 仅用于错误信息定位。
// Updates the given columns; what is only used for error context.
func (s *Scheduler) dbUpdateColumns(key string, cols map[string]interface{}, what string) error {
	if s.DB == nil {
		return nil
	}
	if err := s.DB.Model(&periodicTask{}).Where("`key` = ?", key).Updates(cols).Error; err != nil {
		return fmt.Errorf("任务[%s] %s 异常：%v", key, what, err)
	}
	return nil
}
