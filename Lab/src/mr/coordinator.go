package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// master工作状态：闲置，进行中，完成
type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

// worker工作状态：map，reduce，exit，wait
type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

// master工作的信息
type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}

type Master struct {
	TaskQueue     chan *Task          // 等待执行的task队列
	TaskInfo      map[int]*MasterTask // 当前所有task的信息
	MasterPhase   State               // Master的阶段
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map任务产生的R个中间文件的信息
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskInfo:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 切成16MB-64MB的文件
	// 创建map任务
	m.createMapTask()

	// 一个程序成为master，其他成为worker
	//这里就是启动master 服务器就行了，
	//拥有master代码的就是master，别的发RPC过来的都是worker
	m.server()
	// 启动一个goroutine 检查超时的任务
	go m.catchTimeOut()

	return &m
}
func (m *Master) catchTimeOut() {
	for {
		time.Sleep(1 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskInfo {
			//超时判定。如果超时则将任务取回并闲置
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				//将当前任务的指针防到通道中，并将状态改为空闲
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

// 创建master时就创建MapTask
func (m *Master) createMapTask() {
	// 根据传入的filename，每个文件对应一个map task
	for idx, filename := range m.InputFiles {
		//创建新Task
		NewTask := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: idx,
		}
		m.TaskQueue <- &NewTask
		m.TaskInfo[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &NewTask,
		}
	}
}

func (m *Master) createReduceTask() {
	m.TaskInfo = make(map[int]*MasterTask)
	for idx, files := range m.Intermediates {
		TaskInfo := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		m.TaskQueue <- &TaskInfo
		m.TaskInfo[idx] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &TaskInfo,
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// master等待worker调用
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	// assignTask就看看自己queue里面还有没有task
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		//若任务队列不空，则将reply指向任务结构
		*reply = *<-m.TaskQueue
		// 记录task的启动时间
		m.TaskInfo[reply.TaskNumber].TaskStatus = InProgress
		m.TaskInfo[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		//master已退出，返回空任务
		*reply = Task{TaskState: Exit}
	} else {
		// 没有task就让worker 等待
		*reply = Task{TaskState: Wait}
	}
	return nil
}

func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != m.MasterPhase || m.TaskInfo[task.TaskNumber].TaskStatus == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}
	m.TaskInfo[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil
}

func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			//获得所以map task后，进入reduce阶段
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			//获得所以reduce task后，进入exit阶段
			m.MasterPhase = Exit
		}
	}
}

func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskInfo {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
