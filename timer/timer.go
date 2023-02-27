package timer

import (
	"sync"
	"time"
)

type Manager struct {
	seq          uint64                // 定时器id
	cronCount    uint64                // taskMap 大小
	timerCronMap map[uint64]*timerCron // 根据seq分片的timerCron
	mutex        sync.Mutex
}

func NewManager(cronCount uint64) *Manager {
	m := new(Manager)
	m.seq = 0
	m.cronCount = cronCount
	m.timerCronMap = make(map[uint64]*timerCron)
	for i := uint64(0); i < cronCount; i++ {
		m.timerCronMap[i] = newTimerCron()
	}
	return m
}

func (m *Manager) SetCronCount(cronCount uint64) {
	m.cronCount = cronCount
}

func (m *Manager) AddTimerTask(d time.Duration, immediately bool, f interface{}, args ...interface{}) uint64 {
	m.mutex.Lock()
	seq := m.seq
	m.seq++
	m.mutex.Unlock()
	cron := m.timerCronMap[seq%m.cronCount]
	cron.AddTimerTask(seq, d, immediately, f, args...)
	return seq
}

func (m *Manager) DelTimerTask(seq uint64) {
	cron, exist := m.timerCronMap[seq%m.cronCount]
	if !exist {
		return
	}
	cron.DelTimerTask(seq)
}

func (m *Manager) Stop() {
	for _, v := range m.timerCronMap {
		v.Stop()
	}
}
