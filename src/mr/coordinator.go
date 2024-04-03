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

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProcess
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	TaskQueue        chan *Task               // 等待执行的Task
	TaskMeta         map[int]*CoordinatorTask // 当前所有task的信息
	CoordinatorPhase State
	NReduce          int
	InputFiles       []string
	Intermediates    [][]string //Map任务产生的R个中间文件的信息
}

// master存储任务的信息, diff: 未保存wokerID.因为master不主动心跳检测
type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus
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

// 大锁保平安
var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Coordinator) server() {
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
func (m *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := m.CoordinatorPhase == Exit
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	m := Coordinator{
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskMeta:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}
	// 创建map 任务
	m.createMapTask()

	// Your code here.
	m.server()

	// 启动一个携程检测超时任务
	go m.catchTimeOut()
	return &m
}

func (m *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.CoordinatorPhase == Exit {
			mu.Unlock()
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.TaskStatus == InProcess && time.Now().Sub(masterTask.StartTime) > 10*time.Second {
				// 超时: 将任务置为闲置 , 加入到等待队列
				// 目的想要超时任务即换个工作节点执行
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// 创建map 任务
func (m *Coordinator) createMapTask() {
	// 根据传入的filename , 每个文件对应一个map task

	for index, filename := range m.InputFiles {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: index,
		}
		// 将任务传入到任务队列
		m.TaskQueue <- &taskMeta
		// 记录该任务执行信息
		m.TaskMeta[index] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

// master 等待 worker 调用
func (m *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	// 查看queue内是否有任务  注意线程安全
	mu.Lock()
	defer mu.Unlock()
	if len(m.TaskQueue) > 0 {
		// 任务队列中存在任务就发送给worker
		*reply = *<-m.TaskQueue
		// 记录启动时间
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProcess
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.CoordinatorPhase == Exit {
		// mr任务已完成
		*reply = Task{TaskState: Exit}
	} else {
		// 暂时无分片任务
		*reply = Task{TaskState: Wait}
	}
	return nil
}

// rpc
func (m *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	if task.TaskState != m.CoordinatorPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 任务已完成
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	mu.Unlock()
	defer m.processTaskResult(task)
	return nil
}

func (m *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		// 收集中间结果信息
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			// map任务全部完成 进入reduce
			m.createReduceTask()
			m.CoordinatorPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			// 任务结束
			m.CoordinatorPhase = Exit
		}

	}
}

func (m *Coordinator) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func (m *Coordinator) createReduceTask() {
	m.TaskMeta = make(map[int]*CoordinatorTask)
	for idx, files := range m.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    idx,
			Intermediates: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &CoordinatorTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}
