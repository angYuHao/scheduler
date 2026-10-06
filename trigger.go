package scheduler

import (
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

// 内置触发器类型名 / Built-in trigger type names.
const (
	Date     = "date"     // 单次任务 / one-shot job
	Interval = "interval" // 固定间隔 / fixed interval
	Cron     = "cron"     // cron 表达式 / cron expression
)

// Trigger 定义触发器接口。
// 任务每次执行后调用 Next 计算下次执行时间，返回零值 time.Time{} 表示任务终止；
// Build 由 PeriodicTask 构造真实例；Type 返回类型名（内置名或 RegisterTrigger 注册名），
// 用于持久化与重启恢复。
//
// Trigger decides when a job runs. Next returns the zero time.Time to stop.
// Build constructs a configured instance from a PeriodicTask, and Type returns
// the type name used for persistence and restart recovery.
type Trigger interface {
	Type() string                        // 类型名 / type name
	Next(time.Time) time.Time            // 下次执行时间，零值表示终止 / next fire time, zero to stop
	Build(PeriodicTask) (Trigger, error) // 由任务配置构造实例 / build an instance from the job
}

// protoTable 保存各类型的零值原型，内置三种已预置。
// protoTable holds zero-value prototypes keyed by trigger type name.
var (
	protoMu    sync.RWMutex
	protoTable = map[string]Trigger{
		Date:     &DateTrigger{},
		Interval: &IntervalTrigger{},
		Cron:     &CronTrigger{},
	}
)

// RegisterTrigger 注册自定义触发器类型，PeriodicTask.TriggerType 填同名即可使用；
// 服务重启后从数据库恢复任务时按此名查找原型并重建。
// proto 只需零值原型（其 Build 方法负责产出真实例），重复注册返回错误。
//
// RegisterTrigger registers a custom trigger type. proto only needs to be a
// zero-value prototype; its Build method produces the real instance.
func RegisterTrigger(name string, proto Trigger) error {
	if name == "" || proto == nil {
		return errors.New("触发器类型名与原型不能为空")
	}
	protoMu.Lock()
	defer protoMu.Unlock()
	if _, ok := protoTable[name]; ok {
		return fmt.Errorf("触发器类型 %q 已注册", name)
	}
	protoTable[name] = proto
	return nil
}

// lookupTriggerProto 按类型名查找原型。Looks up a prototype by type name.
func lookupTriggerProto(name string) (Trigger, bool) {
	protoMu.RLock()
	proto, ok := protoTable[name]
	protoMu.RUnlock()
	return proto, ok
}

// BuildTrigger 构造任务的触发器实例：
// ① PeriodicTask.Trigger 已注入实例时，直接由其 Build 产出（内存模式免注册）；
// ② 否则按 TriggerType 查内置/注册的类型原型，由原型 Build 产出（支持持久化恢复）。
//
// BuildTrigger constructs the trigger of a job: an injected instance wins,
// otherwise the registered prototype for TriggerType builds it.
func BuildTrigger(p PeriodicTask) (Trigger, error) {
	if p.Trigger != nil {
		return p.Trigger.Build(p)
	}
	if p.TriggerType == "" {
		return nil, errors.New("Trigger 未指定类型（内置类型为 date/interval/cron，自定义类型需先 RegisterTrigger）")
	}
	proto, ok := lookupTriggerProto(p.TriggerType)
	if !ok {
		return nil, fmt.Errorf("未知触发器类型 %q（内置类型为 date/interval/cron，自定义类型需先 RegisterTrigger）", p.TriggerType)
	}
	return proto.Build(p)
}

//---------- 内置触发器 / Built-in triggers ----------

// DateTrigger 定义单次任务：到期执行一次后任务终止。
// DateTrigger fires once at StartTime (immediately if already past).
type DateTrigger struct {
	StartTime time.Time
}

// Type 返回内置类型名。Returns the built-in type name.
func (t *DateTrigger) Type() string { return Date }

// Next 起始时间已过则立即执行，否则等待到起始时刻。
// Fires immediately when StartTime is in the past.
func (t *DateTrigger) Next(prev time.Time) time.Time {
	if t.StartTime.Before(prev) {
		return time.Now()
	}
	return t.StartTime
}

// Build 由任务的 StartTime 构造实例。Builds an instance from the job.
func (t *DateTrigger) Build(p PeriodicTask) (Trigger, error) {
	return &DateTrigger{StartTime: p.StartTime}, nil
}

// IntervalTrigger 定义定时任务：按固定间隔循环执行。
// IntervalTrigger fires repeatedly with a fixed interval.
type IntervalTrigger struct {
	Interval  time.Duration
	StartTime time.Time
	EndTime   time.Time // 零值表示不限 / zero means no end
}

// Type 返回内置类型名。Returns the built-in type name.
func (t *IntervalTrigger) Type() string { return Interval }

// Next 越过 EndTime 返回零值（任务终止），否则按间隔递推。
// Returns the zero time once EndTime has passed, otherwise prev+Interval.
func (t *IntervalTrigger) Next(prev time.Time) time.Time {
	if !t.EndTime.IsZero() && !t.EndTime.After(prev) {
		return time.Time{}
	}
	if t.StartTime.After(prev) {
		return t.StartTime
	}
	return prev.Add(t.Interval)
}

// Build 解析 Interval 字符串并构造实例；间隔非法时返回错误。
// Parses the interval string; returns an error when it is not a positive duration.
func (t *IntervalTrigger) Build(p PeriodicTask) (Trigger, error) {
	pd, err := time.ParseDuration(p.Interval)
	if err != nil {
		return nil, fmt.Errorf("解析：%s 发生异常：%v", p.Interval, err)
	}
	// 间隔必须为正，否则任务会在同一时刻无限循环触发。
	if pd <= 0 {
		return nil, fmt.Errorf("解析：%s 发生异常：间隔必须大于 0", p.Interval)
	}
	return &IntervalTrigger{Interval: pd, StartTime: p.StartTime, EndTime: p.EndTime}, nil
}

// CronTrigger 定义计划任务：按 cron 表达式循环执行。
// CronTrigger fires according to a cron expression.
type CronTrigger struct {
	Cron      string
	StartTime time.Time
	EndTime   time.Time // 零值表示不限 / zero means no end
}

// Type 返回内置类型名。Returns the built-in type name.
func (t *CronTrigger) Type() string { return Cron }

// Next 求出下一次匹配 cron 表达式的时间；表达式为空、越过 EndTime 或解析失败时返回零值。
// Returns the next matching time, or the zero time on empty expression,
// passed EndTime or parse failure.
func (t *CronTrigger) Next(prev time.Time) time.Time {
	if t.Cron == "" {
		return time.Time{}
	}
	if !t.EndTime.IsZero() && !t.EndTime.After(prev) {
		return time.Time{}
	}
	from := prev
	if t.StartTime.After(prev) {
		from = t.StartTime
	}
	expr, err := Parse(t.Cron)
	if err != nil {
		logrus.Error(fmt.Sprintf("解析 [CRON:%s] 异常：%v", t.Cron, err))
		return time.Time{}
	}
	return expr.Next(from)
}

// Build 校验 cron 表达式非空并构造实例。
// Validates that the cron expression is present and builds an instance.
func (t *CronTrigger) Build(p PeriodicTask) (Trigger, error) {
	if p.Cron == "" {
		return nil, errors.New("cron 表达式不能为空")
	}
	return &CronTrigger{Cron: p.Cron, StartTime: p.StartTime, EndTime: p.EndTime}, nil
}
