package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
// worker  与 master 的rpc通信
// 1. worker 在空闲时向 coordinator 发起task 请求, coordinator响应一个分配给该worker的Task
// 2. Worker 运行完task后向coordinator 汇报

const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	WAIT   = "WAIT"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 任务结构体
// type Task struct {
// 	Type         string // 任务类型 map reduce wait
// 	Index        int
// 	MapInputFile string

// 	WorkerId string
// 	Deadline time.Time
// }

// type AppluForTaskArgs struct {
// 	WorkerId      string
// 	LastTaskType  string
// 	LastTaskIndex int
// }

// type ApplyForTaskReply struct {
// 	TaskType  string
// 	TaskIndex int
// 	MapNum    int
// 	ReduceNum int
// }

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
