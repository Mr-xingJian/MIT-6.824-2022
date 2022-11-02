package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int {
    return len(a)
}

func (a ByKey) Swap(i, j int) {
    a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less (i, j int) bool {
    return a[i].Key < a[j].Key
}

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
// get a workerid.
//
func GetWorkerId() string {
    return fmt.Sprintf("%v-%d", time.Now().UnixNano(), rand.Intn(10000000))
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

    rpcRet := true
    ret := 0

    // 1.get workerid for log.
    workerId := GetWorkerId()
    DPrintln("INFO: Worker start", workerId)

    // 1.get task.
    // 2.store task result to file.
    // 3.submit task.
    for {
        if ret != 0 {
            time.Sleep(time.Second)
            ret = 0
        }

        // 1.get task.
        getTaskReq := GetTaskReq{}
        getTaskRsp := GetTaskRsp{}
        rpcRet := call("Coordinator.GetTask", &getTaskReq, &getTaskRsp)
        if !rpcRet || getTaskRsp.Ret == -1{
            DPrintln("ERR: GetTask error", workerId)
            ret = -1
            continue
        }

        if getTaskRsp.Ret == 1 {
            break
        }

        // process task
        file := getTaskRsp.File
        f, err := os.Open(file)
        if err != nil {
            DPrintln("ERR: Open error", workerId, file)
            ret = -1
            continue
        }
        defer f.Close()

        data, err := ioutil.ReadAll(f)
        if err != nil {
            DPrintln("ERR: ReadAll error", workerId)
            continue
        }
        tmpKvs := mapf(file, string(data))

        // output to a file
        tmpname := fmt.Sprintf("mr-tmp-%s-%v", workerId, time.Now().UnixNano())
        tmpfile, err := ioutil.TempFile(".", tmpname)
        if err != nil {
            DPrintln("ERR: TmpFile error", workerId)
            continue
        }
        enc := json.NewEncoder(tmpfile)
        for _, kv := range tmpKvs {
            enc.Encode(kv)
        }
        tname := tmpfile.Name()
        tmpfile.Close()

        err = os.Rename(tname, tmpname)
        if err != nil {
            DPrintln("ERR: Rename error", tname, tmpname)
            continue
        }

        // submit task
        submitTaskReq := SubmitTaskReq{}
        submitTaskRsp := SubmitTaskRsp{}
        submitTaskReq.File = file
        submitTaskReq.TmpFile = tmpname
        rpcRet = call("Coordinator.SubmitTask", &submitTaskReq, &submitTaskRsp)
        if !rpcRet {
            DPrintln("ERR: SubmitTask error", tname, tmpname)
            continue
        }

        ret = 0
        DPrintln("INFO: Process task succ", workerId, file)
    }

    ret = 0
    // 1.get reduce task
    // 2.submit reduce task
    for {
        if ret != 0 {
            time.Sleep(time.Second)
            ret = 0
        }

        allKvs := make([]KeyValue, 0)

        getReduceTaskReq := GetReduceTaskReq{}
        getReduceTaskRsp := GetReduceTaskRsp{}
        rpcRet = call("Coordinator.GetReduceTask", &getReduceTaskReq, &getReduceTaskRsp)
        if !rpcRet || getReduceTaskRsp.Ret == -1 {
            DPrintln("ERR: GetReduceTask error", workerId)
            ret = -1
            continue
        }

        if getReduceTaskRsp.Ret == 1 {
            DPrintln("INFO: All reduce task done", workerId)
            break
        }

        reduceTotal := getReduceTaskRsp.ReduceTotal
        reduceN := getReduceTaskRsp.ReduceN
        for _, file := range getReduceTaskRsp.Files {
            f, err := os.Open(file)
            if err != nil {
                DPrintln("ERR: Open error", workerId, file)
                continue
            }

            dec := json.NewDecoder(f)
            for {
                kv := KeyValue{}
                err := dec.Decode(&kv)
                if err != nil {
                    break
                }
                if ihash(kv.Key) % reduceTotal == reduceN {
                    allKvs = append(allKvs, kv)
                }
            }
        }

        // copy from sequential
        sort.Sort(ByKey(allKvs))

        oname := fmt.Sprintf("mr-out-tmp-", reduceN)
        ofile, _ := os.Create(oname)
        i := 0
        for i < len(allKvs) {
            j := i + 1
            for j < len(allKvs) && allKvs[j].Key == allKvs[i].Key {
                j++
            }
            values := []string{}
            for k := i; k < j; k++ {
                values = append(values, allKvs[k].Value)
            }
            output := reducef(allKvs[i].Key, values)

            // this is the correct format for each line of Reduce output.
            fmt.Fprintf(ofile, "%v %v\n", allKvs[i].Key, output)

            i = j
        }
        ofile.Close()

        submitReduceTaskReq := SubmitReduceTaskReq{}
        submitReduceTaskRsp := SubmitReduceTaskRsp{}
        submitReduceTaskReq.ReduceN = reduceN
        rpcRet = call("Coordinator.SubmitReduceTask", &submitReduceTaskReq, &submitReduceTaskRsp)
        if !rpcRet {
            DPrintln("ERR: SubmitReduceTask error", workerId)
            continue
        }

        finalName := fmt.Sprintf("mr-out-%d", reduceN)
        err := os.Rename(oname, finalName)
        if err != nil {
            DPrintln("ERR: Rename error", workerId)
            continue
        }

        DPrintln("INFO: Process reduce task succ", workerId, reduceN)
    }
    DPrintln("INFO: Worker done")
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
