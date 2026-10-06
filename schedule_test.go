package scheduler

import (
	"fmt"
	"testing"
	"time"
)

type T struct{}

func (t T) A() {
	fmt.Println("A")
}

type T2 struct{}

func (t T2) B(b string, i int) {
	fmt.Println("Calling Method : B")
	fmt.Println(b, i)
}

func (t T2) B2(b string, i int, c interface{}) {
	fmt.Println("Calling Method : B2")
	fmt.Println(b, i, c)
}

func (t T2) SecondMethod(s string) {
	fmt.Println("Calling Method : SecondMethod")
	fmt.Println(s)
}

// TestNewScheduler 手工演示脚本，耗时较长，`go test -short` 时跳过。
func TestNewScheduler(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过长耗时的手工演示测试（go test -short）")
	}
	s := NewScheduler(nil, time.Second, 24)
	s.AddTaskFunction(&T{}, &T2{})
	s.Start()
	defer s.Stop()

	err := s.AddTask("test1", PeriodicTask{
		Name:              "测试任务",
		Function:          T2.B2,
		Args:              Args{"test", 1, 2},
		StartTime:         time.Now(),
		TriggerType:       Cron,
		Cron:              "*/5 * * * * * *",
		LimitedAliveCount: 1,
	})
	if err != nil {
		fmt.Println(err)
	}
	err = s.AddTask("test2", PeriodicTask{
		Name:              "测试任务222",
		Function:          T2.B2,
		Args:              Args{"test", 1, 2},
		StartTime:         time.Now(),
		TriggerType:       Interval,
		Interval:          "5s",
		Cron:              "*/5 * * * * * *",
		LimitedAliveCount: 1,
	})
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(15 * time.Second)
	fmt.Println("更新")
	err = s.ResetTask("test1", PeriodicTask{
		Name:        "测试任务2",
		Function:    T2.SecondMethod,
		Args:        Args{"test2"},
		TriggerType: Interval,
		Interval:    "5s",
		StartTime:   time.Now(),
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("停止")
	err = s.StopTask("test1")
	if err != nil {
		fmt.Println(err)
	}
	time.Sleep(30 * time.Second)
	fmt.Println("继续")
	err = s.ContinueTask("test1")
	if err != nil {
		fmt.Println(err)
	}

	time.Sleep(30 * time.Second)

	err = s.StartTaskNow("test1")
	if err != nil {
		fmt.Println(err)
	}
}

func TestJSON(t *testing.T) {
	fmt.Println(time.Unix(-62135596800, 0).IsZero())
}
