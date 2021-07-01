package mr

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	Filename  string
	Seq       int
	StartTime time.Time
}

type ReduceTask struct {
	Seq       int
	StartTime time.Time
}

type Master struct {
	mutex sync.Mutex

	ctx        context.Context
	cancelFunc context.CancelFunc

	pendingMapTasks    []*MapTask
	pendingReduceTasks []*ReduceTask
	nMap               int
	nReduce            int
	runningMapTask     map[int]*MapTask
	runningReduceTask  map[int]*ReduceTask
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	reduceTaskNo := make([]int, nReduce, nReduce)
	for i, _ := range reduceTaskNo {
		reduceTaskNo[i] = i
	}

	ctx, cancel := context.WithCancel(context.Background())

	var mapTasks []*MapTask
	for seq, filename := range files {
		mapTasks = append(
			mapTasks, &MapTask{
				Filename: filename,
				Seq:      seq,
			},
		)
	}

	var reduceTasks []*ReduceTask
	for seq := 0; seq < nReduce; seq++ {
		reduceTasks = append(
			reduceTasks, &ReduceTask{
				Seq: seq,
			},
		)
	}

	m := Master{
		ctx:                ctx,
		cancelFunc:         cancel,
		pendingMapTasks:    mapTasks,
		pendingReduceTasks: reduceTasks,
		nMap:               len(files),
		nReduce:            nReduce,
		runningMapTask:     make(map[int]*MapTask),
		runningReduceTask:  make(map[int]*ReduceTask),
	}

	m.timeoutRunningTasks()
	m.server()
	return &m
}

func (m *Master) timeoutRunningTasks() {
	expiredMap := make(chan *MapTask, m.nMap)
	expiredReduce := make(chan *ReduceTask, m.nReduce)

	scanTasks := func() {
		for {
			select {
			case <-m.ctx.Done():
				fmt.Println("stop scanning tasks")
				return
			default:
				m.scanRunningMap(expiredMap)
				m.scanRunningReduce(expiredReduce)
				time.Sleep(time.Millisecond * 50)
			}
		}
	}
	go scanTasks()

	cancelTasks := func() {
		for {
			select {
			case <-m.ctx.Done():
				return
			case task := <-expiredMap:
				m.cancelMapTask(task)
			case task := <-expiredReduce:
				m.cancelReduceTask(task)
			}
		}
	}
	go cancelTasks()
}

func (m *Master) scanRunningMap(c chan *MapTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, task := range m.runningMapTask {
		if time.Now().Sub(task.StartTime) > time.Second*10 {
			c <- task
		}
	}
}

func (m *Master) cancelMapTask(task *MapTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Println("cancel map:", task.Seq)

	delete(m.runningMapTask, task.Seq)
	m.pendingMapTasks = append(m.pendingMapTasks, task)
}

func (m *Master) scanRunningReduce(c chan *ReduceTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, task := range m.runningReduceTask {
		if time.Now().Sub(task.StartTime) > time.Second*10 {
			c <- task
		}
	}
}

// master会认为reduce还在运行，其实已经结束
func (m *Master) cancelReduceTask(task *ReduceTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	fmt.Println("cancel reduce:", task.Seq)

	delete(m.runningReduceTask, task.Seq)
	m.pendingReduceTasks = append(m.pendingReduceTasks, task)
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()

	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	res := len(m.runningReduceTask) == 0 && len(m.runningMapTask) == 0 &&
		len(m.pendingReduceTasks) == 0 && len(m.pendingMapTasks) == 0

	if res {
		m.cancelFunc()
	}
	return res
}

func (m *Master) AskTask(req *AskTaskReq, rsp *AskTaskRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.pendingMapTasks) != 0 {
		m.dispatchMap(rsp)
		return nil
	}
	if len(m.runningMapTask) != 0 {
		rsp.Type = "WAIT"
		return nil
	}

	if len(m.pendingReduceTasks) != 0 {
		m.dispatchReduce(rsp)
		return nil
	}

	rsp.Type = "DONE"
	return nil
}

func (m *Master) dispatchMap(rsp *AskTaskRsp) {
	if len(m.pendingMapTasks) == 0 {
		rsp.Type = "WAIT"
		return
	}

	task := m.pendingMapTasks[0]
	m.pendingMapTasks = m.pendingMapTasks[1:]

	rsp.Type = "MAP"
	rsp.Filename = task.Filename
	rsp.MapSeq = task.Seq
	rsp.ReduceSeq = m.nReduce

	task.StartTime = time.Now()
	m.runningMapTask[task.Seq] = task

	fmt.Println("dispatch map:", task.Seq)
}

func (m *Master) dispatchReduce(rsp *AskTaskRsp) {
	if len(m.pendingReduceTasks) == 0 {
		rsp.Type = "DONE"
		return
	}

	task := m.pendingReduceTasks[0]
	m.pendingReduceTasks = m.pendingReduceTasks[1:]

	rsp.Type = "REDUCE"
	rsp.MapSeq = m.nMap
	rsp.ReduceSeq = task.Seq

	task.StartTime = time.Now()
	m.runningReduceTask[task.Seq] = task

	fmt.Println("dispatch reduce:", task.Seq)
}

func (m *Master) MapDone(req *MapDoneReq, rsp *MapDoneRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.runningMapTask[req.Seq]
	if !ok {
		fmt.Printf("map %d already done\n", req.Seq)
		return nil
	}

	fmt.Printf("map %d done\n", req.Seq)
	delete(m.runningMapTask, req.Seq)
	return nil
}

func (m *Master) ReduceDone(req *ReduceDoneReq, rsp *ReduceDoneRsp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.runningReduceTask[req.Seq]
	if !ok {
		fmt.Printf("reduce %d already done\n", req.Seq)
		return nil
	}

	fmt.Printf("reduce %d done\n", req.Seq)
	delete(m.runningReduceTask, req.Seq)
	return nil
}
