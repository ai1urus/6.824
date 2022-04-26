package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type CoordinatorDoneReply struct {
	IsDone bool
}

type MapTaskReply struct {
	filename string
}

// RegisterWorker Handler

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerId int
}

// DispathTask Handler

type DispatchTaskArgs struct {
	WorkerId int
}

type DispatchTaskReply struct {
	TaskId    int
	TaskType  string
	TaskInput []string
	NReduce   int
}

// SubmitTask Handler

type SubmitTaskArgs struct {
	WorkerId   int
	TaskId     int
	TaskType   string
	TaskOutput []string
}

type SubmitTaskReply struct {
}

// KeepAlive Handler

type KeepAliveArgs struct {
	WorkerId int
}

type KeepAliveReply struct {
	IsDone    bool
	TaskType  string
	TaskInput string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
