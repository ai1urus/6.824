package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var debugCoordinator bool = false

type Coordinator struct {
	// Your definitions here.
	NReduce int
	IsDone  bool
	muDone  sync.Mutex
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
	// 查询已有worker是否失效
	nextId := -1
	for i := 1; i < len(c.workerState); i++ {
		if c.workerState[i] == 2 {
			nextId = i
			reply.WorkerId = nextId
			c.workerTimeout[nextId] = time.Now().Unix() + c.globalTimeout
			c.workerState[nextId] = 0

			if debugCoordinator {
				fmt.Printf("工作节点%v连接\n", nextId)
			}

			break
		}
	}
	if nextId == -1 {
		nextId = len(c.workerTimeout)
		reply.WorkerId = nextId
		c.workerTimeout = append(c.workerTimeout, time.Now().Unix()+c.globalTimeout)
		c.workerState = append(c.workerState, 0)
	}

	if debugCoordinator {
		fmt.Printf("工作节点%v连接\n", nextId)
	}
	c.muWorker.Unlock()

	return nil
}

func (c *Coordinator) DispatchTask(args *DispatchTaskArgs, reply *DispatchTaskReply) error {
	// 查询map任务是否全部完成
	retflag := false
	c.muMap.Lock()
	c.muReduce.Lock()

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

	// 查询reduce任务是否全部完成
	if !retflag {
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
	}
	c.muReduce.Unlock()
	c.muMap.Unlock()

	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	// 更新worker状态
	c.muWorker.Lock()
	c.muMap.Lock()
	c.muReduce.Lock()

	// 暂未实现状态1
	if c.workerState[args.WorkerId] == 1 {
		c.workerState[args.WorkerId] = 0
	}

	// 更新任务状态
	if args.TaskType == "map" {
		c.mapTaskIntermediate[args.TaskId] = args.TaskOutput
		if c.mapTaskState[args.TaskId] == 1 {
			c.mapTaskState[args.TaskId] = 2
		}
		for i := 0; i < len(args.TaskOutput); i++ {
			c.reduceTaskList[i] = append(c.reduceTaskList[i], args.TaskOutput[i])
		}
	} else if args.TaskType == "reduce" {
		if c.reduceTaskState[args.TaskId] == 1 {
			c.reduceTaskState[args.TaskId] = 2
		}
	}
	c.muReduce.Unlock()
	c.muMap.Unlock()
	c.muWorker.Unlock()

	return nil
}

// worker向coordiantor发送请求，coordinator更新worker的超时timestamp
func (c *Coordinator) KeepWorkerAlive(args *KeepAliveArgs, reply *KeepAliveReply) error {
	if c.Done() {
		reply.IsDone = true
		return nil
	}

	c.muWorker.Lock()
	workerId := args.WorkerId
	c.workerTimeout[workerId] = time.Now().Unix() + c.globalTimeout
	c.muWorker.Unlock()

	reply.IsDone = c.Done()
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

	c.muDone.Lock()
	defer c.muDone.Unlock()

	if c.IsDone {
		return true
	}

	c.muMap.Lock()
	c.muReduce.Lock()
	for i := 0; i < len(c.reduceTaskState); i++ {
		state := c.reduceTaskState[i]
		if state != 2 {
			ret = false
			// break
		}
	}
	if debugCoordinator {
		fmt.Printf("任务完成状态: %v 节点状态: %v Map状态: %v Map节点: %v Reduce状态: %v Reduce节点: %v\n", ret, c.workerState, c.mapTaskState, c.mapTaskWorker, c.reduceTaskState, c.reduceTaskWorker)
		// fmt.Printf("任务完成状态: %v Map状态: %v Map节点: %v Worker超时: %v\n", ret, c.mapTaskState, c.mapTaskWorker, c.workerTimeout)
	}

	c.muReduce.Unlock()
	c.muMap.Unlock()

	if ret {
		c.IsDone = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce, globalTimeout: 5, mapTaskList: files, NInput: len(files), IsDone: false}

	c.mapTaskState = make([]int, c.NInput)
	c.mapTaskWorker = make([]int, c.NInput)
	c.mapTaskIntermediate = make([][]string, c.NInput)

	c.reduceTaskList = make([][]string, nReduce)
	c.reduceTaskState = make([]int, nReduce)
	c.reduceTaskWorker = make([]int, nReduce)

	c.workerState = append(c.workerState, 2)
	c.workerTimeout = append(c.workerTimeout, time.Now().Unix())

	for i := 0; i < c.NInput; i++ {
		c.mapTaskWorker[i] = -1
	}

	for i := 0; i < c.NReduce; i++ {
		c.reduceTaskWorker[i] = -1
	}

	if debugCoordinator {
		fmt.Printf("Coordinator created with %v map tasks!\n", len(c.mapTaskList))
	}
	// 开始心跳检测
	go func() {
		for {
			cIsDone := false
			c.muDone.Lock()
			cIsDone = c.IsDone
			c.muDone.Unlock()

			if cIsDone {
				break
			}

			c.muWorker.Lock()
			c.muMap.Lock()
			c.muReduce.Lock()
			currTimestamp := time.Now().Unix()

			if debugCoordinator {
				// fmt.Printf("当前节点连接状态: %v 当前时间: %v 当前节点超时时间: % v\n", c.workerState, currTimestamp, c.workerTimeout)
				fmt.Printf("当前节点连接状态: %v 当前Map任务状态: %v 当前Reduce任务状态: % v\n", c.workerState, c.mapTaskState, c.reduceTaskState)
			}

			for i := 1; i < len(c.workerTimeout); i++ {
				// 判断当前已连接的节点是否超时
				if c.workerState[i] == 2 || currTimestamp >= c.workerTimeout[i] {

					if debugCoordinator {
						fmt.Printf("工作节点%v断开\n", i)
					}

					c.workerState[i] = 2
					// 遍历map任务进行处理

					for j := 0; j < len(c.mapTaskState); j++ {
						// 这里不需要对已完成的map进行处理
						if c.mapTaskWorker[j] == i && c.mapTaskState[j] != 2 {

							c.mapTaskState[j] = 0
							c.mapTaskWorker[j] = -1

							if debugCoordinator {
								fmt.Printf("Map任务 %v重置\n", j)
							}

							for _, filename := range c.mapTaskIntermediate[j] {
								err := os.Remove(filename)
								if err != nil {
									if debugCoordinator {
										fmt.Println("中间文件无法删除，正在使用中?")
									}
								}
							}

							c.mapTaskIntermediate[j] = c.mapTaskIntermediate[j][0:0]

							// 还需要删除reduce中该map对应的内容
							pattern := ".*mr-" + strconv.Itoa(j) + "-.*"
							mapfilter, _ := regexp.Compile(pattern)
							for k := 0; k < len(c.reduceTaskList); k++ {
								for ki := 0; ki < len(c.reduceTaskList[k]); ki++ {
									if mapfilter.MatchString(c.reduceTaskList[k][ki]) {
										// 删除对应元素
										c.reduceTaskList[k] = append(c.reduceTaskList[k][:ki], c.reduceTaskList[k][ki+1:]...)
									}
								}
							}
						}
					}

					// 遍历reduce任务进行处理
					for j := 0; j < c.NReduce; j++ {
						if c.reduceTaskWorker[j] == i {
							// 对于已经完成的reduce任务不做处理
							if c.reduceTaskState[j] == 2 {
								continue
							}
							c.reduceTaskState[j] = 0
						}
					}

				}
			}

			c.muReduce.Unlock()
			c.muMap.Unlock()
			c.muWorker.Unlock()

			time.Sleep(time.Duration(c.globalTimeout) * time.Second)
		}
	}()

	c.server()
	return &c
}
