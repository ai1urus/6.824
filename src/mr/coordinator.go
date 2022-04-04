package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var debugCoordinator bool = false

type Coordinator struct {
	// Your definitions here.
	NReduce int
	// map任务相关状态
	NInput              int
	mapTaskList         []string
	mapTaskState        []int
	mapTaskWorker       []int
	mapTaskIntermediate [][]string
	muMap               sync.Mutex
	// reduce任务相关状态
	reduceTaskList   [][]string
	reduceTaskState  []int
	reduceTaskWorker []int
	muReduce         sync.Mutex
	// worker相关状态
	globalTimeout int64      // 配置全局worker失效时间，默认为10s
	workerTimeout []int64    // 记录每个worker的超时时间
	workerState   []int      // 记录每个worker的状态 0:idle 1:in-progress
	muWorker      sync.Mutex // 控制workerTimeout和workerState的互斥修改
}

// RPC handlers

// worker注册，申请id
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	// MapReduce任务已结束则直接返回
	if c.Done() {
		reply.WorkerId = -1
		return nil
	}
	// 否则分配新id
	c.muWorker.Lock()
	nextId := len(c.workerTimeout)
	reply.WorkerId = nextId
	c.workerTimeout = append(c.workerTimeout, time.Now().Unix()+c.globalTimeout)
	c.workerState = append(c.workerState, 0)
	c.muWorker.Unlock()
	return nil
}

func (c *Coordinator) DispatchTask(args *DispatchTaskArgs, reply *DispatchTaskReply) error {
	// 查询map任务是否全部完成
	retflag := false
	c.muMap.Lock()
	for i := 0; i < len(c.mapTaskList); i++ {
		if c.mapTaskState[i] == 0 {
			c.mapTaskState[i] = 1
			c.mapTaskWorker[i] = args.WorkerId
			reply.TaskId = i
			reply.TaskType = "map"
			reply.TaskInput = append(reply.TaskInput, c.mapTaskList[i])
			reply.NReduce = c.NReduce
			retflag = true
			break
		}
	}
	c.muMap.Unlock()
	if retflag {
		return nil
	}
	// 查询reduce任务是否全部完成
	c.muReduce.Lock()
	for i := 0; i < len(c.reduceTaskList); i++ {
		if c.reduceTaskState[i] == 0 && len(c.reduceTaskList[i]) == c.NInput {
			c.reduceTaskState[i] = 1
			c.reduceTaskWorker[i] = args.WorkerId
			reply.TaskId = i
			reply.TaskType = "reduce"
			reply.TaskInput = c.reduceTaskList[i]
			retflag = true
			break
		}
	}
	c.muReduce.Unlock()
	if retflag {
		return nil
	}
	// reply.TaskType = "break"
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	// 更新任务状态
	if args.TaskType == "map" {
		c.muMap.Lock()
		c.mapTaskIntermediate[args.TaskId] = args.TaskOutput
		c.mapTaskState[args.TaskId] = 2

		c.muReduce.Lock()
		for i := 0; i < len(args.TaskOutput); i++ {
			c.reduceTaskList[i] = append(c.reduceTaskList[i], args.TaskOutput[i])
		}
		c.muReduce.Unlock()

		c.muMap.Unlock()
	} else if args.TaskType == "reduce" {
		c.muReduce.Lock()
		c.reduceTaskState[args.TaskId] = 2
		c.muReduce.Unlock()
	}
	// 更新worker状态
	c.muWorker.Lock()
	c.workerState[args.WorkerId] = 0
	c.muWorker.Unlock()

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
	ret := true

	// Your code here.
	ss := make([]int, 10)
	c.muReduce.Lock()
	for i := 0; i < len(c.reduceTaskState); i++ {
		state := c.reduceTaskState[i]
		if state != 2 {
			ret = false
			// break
		}
		ss[i] = state
	}

	if debugCoordinator {
		fmt.Printf("任务完成状态: %v Reduce状态: %v\n", ret, ss)
	}

	c.muReduce.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce, globalTimeout: 10, mapTaskList: files, NInput: len(files)}
	c.mapTaskWorker = make([]int, c.NInput)
	c.mapTaskState = make([]int, c.NInput)
	c.mapTaskIntermediate = make([][]string, c.NInput)
	c.reduceTaskList = make([][]string, nReduce)
	c.reduceTaskState = make([]int, nReduce)
	c.reduceTaskWorker = make([]int, nReduce)
	// Your code here.
	if debugCoordinator {
		fmt.Printf("Coordinator created with %v map tasks!\n", len(c.mapTaskList))
	}
	// read each input file

	c.server()
	return &c
}

// worker向coordiantor发送请求，coordinator更新worker的超时timestamp
// func (c *Coordinator) KeepWorkerAlive(args *KeepAliveArgs, reply *KeepAliveReply) error {
// 	if c.Done() {
// 		reply.IsDone = true
// 		return nil
// 	}
// 	c.muworkerTimeout.Lock()
// 	workerId := args.Id
// 	c.workerTimeout[workerId] = time.Now().Unix() + c.globalTimeout
// 	// 当前机器为空闲状态则分配任务
// 	if c.workerState[workerId] == 0 {

// 	}
// 	c.muworkerTimeout.Unlock()
// 	reply.IsDone = false
// 	return nil
// }
