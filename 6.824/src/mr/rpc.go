package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"


type TaskStatus string
type TaskPhase string
type TimeDuration time.Duration

// 任务状态常量
const (
	// 就绪
	TaskStatusReady   TaskStatus = "ready"
	// 队列中
	TaskStatusQueue   TaskStatus = "queue"
	// 执行中
	TaskStatusRunning TaskStatus = "running"
	// 已完成
	TaskStatusFinish  TaskStatus = "finish"
	// 任务错误
	TaskStatusErr     TaskStatus = "error"
)

//任务阶段常量
const (
	MapPhase    TaskPhase = "map"
	ReducePhase TaskPhase = "reduce"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 请求任务参数
type ReqTaskArgs struct {
	// 当前worker存活,可以执行任务
	WorkerStatus bool
}

// 请求任务返回值
type ReqTaskReply struct {
	// 返回一个任务
	Task RPCTask
	// 是否完成所有任务
	TaskDone bool
}

// 报告任务参数
type ReportTaskArgs struct {
	// 当前worker存活,可以执行任务
	WorkerStatus bool
	// 任务序号
	TaskIndex int
	// 是否完成
	IsDone bool
}

// 报告任务返回值
type ReportTaskReply struct {
	// Coordinator响应是否处理成功
	CoordinatorAck bool
}


type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RPCTask struct {
	// 操作阶段：map/reduce
	TaskPhase TaskPhase
	// map个数
	MapNum int
	// reduce个数
	ReduceNum int
	// 任务序号
	TaskIndex int
	// 文件名
	FileName string
	// 是否完成
	IsDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
