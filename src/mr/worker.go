package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var debugWorker bool = false
var debugWithCoordinator bool = false

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 注册当前worker，申请id
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		if debugWorker {
			fmt.Printf("当前Worker ID: %v\n", reply.WorkerId)
		}
	} else {
		if debugWorker {
			fmt.Println("任务已结束")
		}
	}

	flag := false
	// 进入周期心跳汇报（开一个goroutine来做）
	go func() {
		for {
			argsKeepAlive := KeepAliveArgs{}
			replyKeepAlive := KeepAliveReply{}
			ok := call("Coordinator.KeepWorkerAlive", &argsKeepAlive, &replyKeepAlive)
			if ok {
				if replyKeepAlive.IsDone {
					// flag = true
					break
				}
			} else {
				// flag = true
				break
			}
			time.Sleep(time.Second)
		}
	}()

	for {
		argsReqTask := DispatchTaskArgs{WorkerId: reply.WorkerId}
		replyReqTask := DispatchTaskReply{}
		ok := call("Coordinator.DispatchTask", &argsReqTask, &replyReqTask)

		if ok {
			switch replyReqTask.TaskType {
			case "map":
				okmap := MapHandler(mapf, reducef, &replyReqTask, reply.WorkerId)
				if !okmap {
					// flag = true
				}
			case "reduce":
				okreduce := ReduceHandler(reducef, &replyReqTask, reply.WorkerId)
				if !okreduce {
					// flag = true
				}
				// case "break":
				// 	break
			}
		} else {
			flag = true
		}
		if flag {
			break
		}
	}
}

// Map Handler
func MapHandler(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	replyReqTask *DispatchTaskReply, WorkerId int) bool {

	if debugWithCoordinator {
		fmt.Printf("工作节点%v 获取Map任务%v\n", WorkerId, replyReqTask.TaskId)
	}

	// 读取Map输入txt的内容
	file, err := os.Open(replyReqTask.TaskInput[0])
	if err != nil {
		log.Fatalf("cannot open %v", replyReqTask.TaskInput[0])
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", replyReqTask.TaskInput[0])
	}
	file.Close()

	// 在内容上执行map
	kva := mapf(replyReqTask.TaskInput[0], string(content))
	// sort.Sort(ByKey(kva))

	if debugWorker {
		fmt.Println("Map KV对数量为:" + strconv.Itoa(len(kva)))
	}

	// 创建临时文件
	tmpfile := make([]*os.File, replyReqTask.NReduce)
	enc := make([]*json.Encoder, replyReqTask.NReduce)
	for i := 0; i < replyReqTask.NReduce; i++ {
		tmpfile[i], err = ioutil.TempFile(".", fmt.Sprintf("mr-%v-%v-tmp", replyReqTask.TaskId, i))
		enc[i] = json.NewEncoder(tmpfile[i])
	}

	// 将map结果写入对应的临时文件(这里不需要执行reduce，否则会出错)
	for i := 0; i < len(kva); i++ {

		toReduce := ihash(kva[i].Key) % replyReqTask.NReduce
		// this is the correct format for each line of Reduce output.
		enc[toReduce].Encode(kva[i])

	}

	// 将临时文件位置加入参数
	argsSubmitTask := SubmitTaskArgs{WorkerId: WorkerId, TaskId: replyReqTask.TaskId, TaskType: "map"}
	replySubmitTask := SubmitTaskReply{}

	for i := 0; i < len(tmpfile); i++ {
		newName := fmt.Sprintf("mr-%v-%v.json", replyReqTask.TaskId, i)
		_, ferr := os.Stat(newName)
		if ferr == nil {
			os.Remove(newName)
		}
		os.Rename(tmpfile[i].Name(), newName)
		argsSubmitTask.TaskOutput = append(argsSubmitTask.TaskOutput, newName)
		tmpfile[i].Close()
	}

	if debugWorker {
		fmt.Println(argsSubmitTask.TaskOutput)
	}

	// 提交任务
	ok := call("Coordinator.SubmitTask", &argsSubmitTask, &replySubmitTask)

	if debugWithCoordinator {
		fmt.Printf("工作节点%v 提交Map任务%v\n", WorkerId, replyReqTask.TaskId)
	}

	return ok
}

// Reduce Handler
func ReduceHandler(reducef func(string, []string) string, replyReqTask *DispatchTaskReply, WorkerId int) bool {

	if debugWithCoordinator {
		fmt.Printf("工作节点%v 获取Reduce任务%v\n", WorkerId, replyReqTask.TaskId)
	}
	// 读取Reduce输入json的内容
	kva := make([]KeyValue, 0)

	if debugWorker {
		fmt.Println(replyReqTask.TaskInput)
	}

	for _, filename := range replyReqTask.TaskInput {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var readTo KeyValue
			if err := dec.Decode(&readTo); err != nil {
				break
			}
			kva = append(kva, readTo)
		}
		file.Close()
	}

	// 在内容上执行map
	sort.Sort(ByKey(kva))
	if debugWorker {
		fmt.Println("Reduce KV对数量为:" + strconv.Itoa(len(kva)))
	}

	// 创建临时文件
	tmpfile, _ := ioutil.TempFile(".", fmt.Sprintf("mr-out-%v-tmp", replyReqTask.TaskId))

	// 将map结果写入对应的临时文件
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// 将临时文件位置加入参数
	argsSubmitTask := SubmitTaskArgs{WorkerId: WorkerId, TaskId: replyReqTask.TaskId, TaskType: "reduce"}
	replySubmitTask := SubmitTaskReply{}

	newName := fmt.Sprintf("mr-out-%v", replyReqTask.TaskId)
	_, ferr := os.Stat(newName)
	if ferr == nil {
		os.Remove(newName)
	}
	os.Rename(tmpfile.Name(), newName)
	argsSubmitTask.TaskOutput = append(argsSubmitTask.TaskOutput, newName)
	tmpfile.Close()

	// 提交任务
	ok := call("Coordinator.SubmitTask", &argsSubmitTask, &replySubmitTask)
	if debugWithCoordinator {
		fmt.Printf("工作节点%v 提交Reduce任务%v\n", WorkerId, replyReqTask.TaskId)
	}
	return ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
