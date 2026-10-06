package scheduler

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
)

// Args 为任务调用参数，以 JSON 形式存入数据库。
// Args holds the job arguments and is persisted as JSON.
type Args []interface{}

// Scan 实现 sql.Scanner，从数据库读取 JSON 参数。
// Scan implements sql.Scanner to read JSON arguments.
func (a *Args) Scan(val interface{}) error {
	if val == nil {
		*a = []interface{}{}
		return nil
	}
	var ba []byte
	switch v := val.(type) {
	case []byte:
		ba = v
	case string:
		ba = []byte(v)
	default:
		return fmt.Errorf("failed to unmarshal Args value: %v", val)
	}
	var t []interface{}
	if err := json.Unmarshal(ba, &t); err != nil {
		return err
	}
	*a = t
	return nil
}

// Value 实现 driver.Valuer，将参数序列化为 JSON。
// Value implements driver.Valuer to serialize arguments as JSON.
func (a Args) Value() (driver.Value, error) {
	marshal, err := json.Marshal(([]interface{})(a))
	if err != nil {
		return nil, err
	}
	return string(marshal), nil
}

// SafeMapOfInt 是并发安全的计数表（键为任务 key，值为当前存活执行数）。
// SafeMapOfInt is a concurrency-safe counter map keyed by job key.
type SafeMapOfInt struct {
	sync.RWMutex
	M map[interface{}]int64
}

// Add 将计数加一，读改写原子完成。
// Add increments the counter atomically.
func (sm *SafeMapOfInt) Add(s interface{}) {
	sm.Lock()
	sm.M[s]++
	sm.Unlock()
}

// Delete 将计数减一，减到 0 时删除该键。
// Delete decrements the counter, removing the key when it reaches zero.
func (sm *SafeMapOfInt) Delete(s interface{}) {
	sm.Lock()
	if v, ok := sm.M[s]; ok {
		if v <= 1 {
			delete(sm.M, s)
		} else {
			sm.M[s] = v - 1
		}
	}
	sm.Unlock()
}

// GetValue 返回计数，不存在时返回 0。
// GetValue returns the counter, or 0 when the key is absent.
func (sm *SafeMapOfInt) GetValue(s interface{}) int64 {
	sm.RLock()
	v := sm.M[s]
	sm.RUnlock()
	return v
}

// Total 返回所有键的计数之和，即当前正在执行的实例总数。
// Total returns the sum of all counters, i.e. the number of running executions.
func (sm *SafeMapOfInt) Total() int64 {
	_, total := sm.counts()
	return total
}

// Len 返回计数非零的键数量，即当前正在执行的任务数。
// Len returns the number of keys with a positive counter, i.e. running jobs.
func (sm *SafeMapOfInt) Len() int {
	n, _ := sm.counts()
	return n
}

// counts 一次加锁同时统计键数量与计数总和，避免 Len+Total 两次加锁两次遍历。
// Counts keys and the total in a single locked pass.
func (sm *SafeMapOfInt) counts() (n int, total int64) {
	sm.RLock()
	n = len(sm.M)
	for _, v := range sm.M {
		total += v
	}
	sm.RUnlock()
	return
}
