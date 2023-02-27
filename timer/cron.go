package timer

import (
	"container/list"
	"reflect"
	"time"

	"github.com/jiangdamalong/go-common/log"
)

type timerTask struct {
	seq      uint64
	tick     time.Duration
	taskF    reflect.Value   // 定时任务执行体
	args     []reflect.Value // 定时任务执行参数
	tickTime time.Time       // 定时器触发的时间
	del      bool            // 是否已经删除
}

type timerCron struct {
	timeTaskMap     map[uint64]*list.List // 时间对应的list,list表示同样时间间隔的有先后顺序
	taskMap         map[uint64]*timerTask // seq->对应的task信息
	taskChan        chan *timerTask
	immediatelyChan chan *timerTask
	addChan         chan *timerTask
	delChan         chan uint64
	ticker          *time.Ticker
	closeChan       chan bool
}

func newTimerCron() *timerCron {
	cron := &timerCron{
		timeTaskMap:     make(map[uint64]*list.List),
		taskMap:         make(map[uint64]*timerTask),
		taskChan:        make(chan *timerTask, 64),
		immediatelyChan: make(chan *timerTask, 64),
		addChan:         make(chan *timerTask),
		delChan:         make(chan uint64),
		ticker:          time.NewTicker(10 * time.Millisecond),
		closeChan:       make(chan bool),
	}
	cron.Run()
	return cron
}

func (c *timerCron) Stop() {
	c.ticker.Stop()
	close(c.closeChan)
}

func (c *timerCron) Run() {

	go c.loop()

	go c.doTask()
}

func (c *timerCron) loop() {
	for {
		select {
		case <-c.ticker.C:
			c.checkTimer()
		case task := <-c.addChan:
			c.addTimerTask(task)
		case seq := <-c.delChan:
			c.delTimerTask(seq)
		case <-c.closeChan:
			log.Debugf("loop receive close")
			return
		}
	}
}

func (c *timerCron) checkTimer() {

	for _, v := range c.timeTaskMap {
		for {
			ele := v.Front()
			if ele == nil {
				break
			}
			task := ele.Value.(*timerTask)
			if task.del {
				v.Remove(ele)
				continue
			}
			// 已超时
			if time.Since(task.tickTime).Nanoseconds() >= 0 {
				c.taskChan <- task
				task.tickTime = time.Now().Add(task.tick)
				v.MoveToBack(ele)
			} else {
				// 意味着剩下的都没超时了
				break
			}
		}
	}

}

func (c *timerCron) doTask() {
	for {
		select {
		case task := <-c.immediatelyChan:
			task.taskF.Call(task.args)
		case task := <-c.taskChan:
			task.taskF.Call(task.args)
		case <-c.closeChan:
			log.Debugf("doTask receive close")
			return
		}
	}
}

func (c *timerCron) AddTimerTask(seq uint64, d time.Duration, immediately bool, f interface{}, args ...interface{}) {

	task := &timerTask{
		seq:      seq,
		tick:     d,
		taskF:    reflect.ValueOf(f),
		args:     make([]reflect.Value, 0, len(args)),
		tickTime: time.Now().Add(d),
		del:      false,
	}

	for _, v := range args {
		task.args = append(task.args, reflect.ValueOf(v))
	}

	if immediately {
		c.immediatelyChan <- task
	}

	c.addChan <- task

}

func (c *timerCron) addTimerTask(task *timerTask) {

	index := uint64(task.tick)
	if _, exist := c.timeTaskMap[index]; !exist {
		c.timeTaskMap[index] = &list.List{}
	}
	taskList := c.timeTaskMap[index]
	taskList.PushBack(task)
	c.taskMap[task.seq] = task

	//log.Debugf("seq:%+v tick:%+v", task.seq, task.tick)
	//log.Debugf("timeTaskMap:%+v", c.timeTaskMap)
	//log.Debugf("taskMap:%+v", c.taskMap)
	//log.Debugf("taskList:%+v", taskList)
}

func (c *timerCron) DelTimerTask(seq uint64) {

	c.delChan <- seq
}

func (c *timerCron) delTimerTask(seq uint64) {

	if task, exist := c.taskMap[seq]; exist {
		task.del = true
		delete(c.taskMap, seq)
	}
}
