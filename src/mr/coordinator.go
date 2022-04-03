package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	NReduce      int
	intermediate []KeyValue
	maptasks     map[string][]int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Printf("receive rpc from worker %v\n", args.X)
	return nil
}

func (c *Coordinator) IsDone(args *ExampleArgs, reply *CoordinatorDoneReply) error {
	reply.IsDone = c.Done()
	return nil
}

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *MapTaskReply) error {
	// 使用乐观锁实现task队列的锁定？
	// golang hashmap 迭代及修改
	for file, task := range c.maptasks {
		if len(task) == 0 {
			reply.filename = file
			// task0 - 完成状态 task1 - 执行workerid
			task = append(task, 0, 1)
			break
		}
	}
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
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{NReduce: nReduce}

	// Your code here.
	fmt.Printf("Coordinator created!\n")
	// read each input file

	c.server()
	return &c
}
