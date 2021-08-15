package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 设置超时时间为 5 min
const MaxTaskRunTime = time.Minute * 5

type TaskState struct {
	Status TaskStatus
	// 开始执行时间
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	// 任务队列
	TaskChan chan RPCTask
	// 输入文件
	Files []string
	// map数目
	MapNum int
	// reduce数目
	ReduceNum int
	// 任务阶段
	TaskPhase TaskPhase
	// 任务状态
	TaskState []TaskState
	// 互斥锁
	Mutex sync.Mutex
	// 是否完成
	IsDone bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 处理任务请求
func (c *Coordinator) HandleTaskReq(args *ReqTaskArgs, reply *ReqTaskReply) error {
	fmt.Println("开始处理任务请求...")
	if !args.WorkerStatus {
		return errors.New("当前worker已下线")
	}
	// 任务出队列
	task, ok := <-c.TaskChan
	if ok == true {
		reply.Task = task
		// 任务状态置为执行中
		c.TaskState[task.TaskIndex].Status = TaskStatusRunning

	} else {
		// 若队列中已经没有任务，则任务全部完成，结束
		reply.TaskDone = true
	}
	return nil
}

// 处理任务报告
func (c *Coordinator) HandleTaskReport(args *ReportTaskArgs, reply *ReportTaskReply) error {
	fmt.Println("开始处理任务报告...")
	if !args.WorkerStatus {
		reply.CoordinatorAck = false
		return errors.New("当前worker已下线")
	}
	if args.IsDone == true {
		// 任务已完成
		c.TaskState[args.TaskIndex].Status = TaskStatusFinish
	} else {
		// 任务执行错误
		c.TaskState[args.TaskIndex].Status = TaskStatusErr
	}
	reply.CoordinatorAck = true
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	fmt.Println(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	finished := true
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	for key, ts := range c.TaskState {
		switch ts.Status {
		case TaskStatusReady:
			// 任务就绪
			finished = false
			c.addTask(key)
		case TaskStatusQueue:
			// 任务队列中
			finished = false
		case TaskStatusRunning:
			// 任务执行中
			finished = false
			c.checkTask(key)
		case TaskStatusFinish:
			// 任务已完成
		case TaskStatusErr:
			// 任务错误
			finished = false
			c.addTask(key)
		default:
			panic("任务状态异常...")
		}
	}
	// 任务完成
	if finished {
		// 判断阶段
		// map则初始化reduce阶段
		// reduce则结束
		if c.TaskPhase == MapPhase {
			c.initReduceTask()
		} else {
			c.IsDone = true
			close(c.TaskChan)
		}
	} else {
		c.IsDone = false
	}
	ret = c.IsDone


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}


	// Your code here.
	c.IsDone = false
	c.Files = files
	c.MapNum = len(files)
	c.ReduceNum = nReduce
	c.TaskPhase = MapPhase
	c.TaskState = make([]TaskState, c.MapNum)
	c.TaskChan = make(chan RPCTask, 10)
	for k := range c.TaskState {
		c.TaskState[k].Status = TaskStatusReady
	}

	c.server()
	return &c
}

// 初始化reduce阶段
func (c *Coordinator) initReduceTask() {
	c.TaskPhase = ReducePhase
	c.IsDone = false
	c.TaskState = make([]TaskState, c.ReduceNum)
	for k := range c.TaskState {
		c.TaskState[k].Status = TaskStatusReady
	}
}

// 将任务放入任务队列中
func (c *Coordinator) addTask(taskIndex int) {
	// 构造任务信息
	c.TaskState[taskIndex].Status = TaskStatusQueue
	task := RPCTask{
		FileName:  "",
		MapNum:    len(c.Files),
		ReduceNum: c.ReduceNum,
		TaskIndex: taskIndex,
		TaskPhase: c.TaskPhase,
		IsDone:    false,
	}
	if c.TaskPhase == MapPhase {
		task.FileName = c.Files[taskIndex]
	}
	// 放入任务队列
	c.TaskChan <- task
}

// 检查任务处理是否超时
func (c *Coordinator) checkTask(taskIndex int) {
	//timeDuration := time.Now().Sub(c.TaskState[taskIndex].StartTime)
	//if timeDuration > MaxTaskRunTime {
	//	// 任务超时重新加入队列
	//	c.addTask(taskIndex)
	//}
}
