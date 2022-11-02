package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

func Equals(s1 string, s2 string) bool {
    return strings.Compare(s1, s2) == 0
}

type Task struct {
    File string
    Doing bool
    StartTime int64
    Done bool
    DoneTime int64
}

type ReduceTask struct {
    ReduceN int
    Doing bool
    StartTime int64
    Done bool
    DoneTime int64
}

type Coordinator struct {
	// Your definitions here.
    Tasks []Task
    TaskMutex sync.Mutex
    AllTaskDone bool

    ReduceCount int
    ReduceTasks []ReduceTask
    ReduceTaskMutex sync.Mutex
    AllReduceTaskDone bool
    ReduceFiles []string
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

//
// worker get a task.
//
func (c *Coordinator) GetTask(req *GetTaskReq, rsp *GetTaskRsp) error {
    c.TaskMutex.Lock()
    defer c.TaskMutex.Unlock()
    ret := -1
    file := ""
    for idx, task := range c.Tasks {
        if task.Done {
            continue
        }
        if task.Doing && (time.Now().Unix() - task.StartTime <= 10) {
            continue
        }
        ret = 0
        c.Tasks[idx].Doing = true
        c.Tasks[idx].StartTime = time.Now().Unix()
        file = task.File
        break
    }
    if ret == 0 {
        rsp.File = file
    }
    if c.IsAllTaskDone() {
        ret = 1
    }
    rsp.Ret = ret
    return nil
}

//
// worker submit a task.
//
func (c *Coordinator) SubmitTask(req *SubmitTaskReq, rsp *SubmitTaskRsp) error {
    c.TaskMutex.Lock()
    defer c.TaskMutex.Unlock()
    for idx, task := range c.Tasks {
        if Equals(task.File, req.File) {
            if !task.Done {
                c.Tasks[idx].Doing = false
                c.Tasks[idx].Done = true
                c.Tasks[idx].DoneTime = time.Now().Unix()
                c.ReduceFiles = append(c.ReduceFiles, req.TmpFile)
            }
        }
    }
    return nil
}

//
// worker get a reduce task.
//
func (c *Coordinator) GetReduceTask(req *GetReduceTaskReq, rsp *GetReduceTaskRsp) error {
    c.ReduceTaskMutex.Lock()
    defer c.ReduceTaskMutex.Unlock()
    ret := -1
    reduceN := -1
    for idx, task := range c.ReduceTasks {
        if task.Done {
            continue
        }
        if task.Doing && (time.Now().Unix() - task.StartTime <= 10) {
            continue
        }
        ret = 0
        c.ReduceTasks[idx].Doing = true
        c.ReduceTasks[idx].StartTime = time.Now().Unix()
        reduceN = c.ReduceTasks[idx].ReduceN
        break
    }
    if ret == 0 {
        rsp.ReduceN = reduceN
        rsp.ReduceTotal = c.ReduceCount
        rsp.Files = append(rsp.Files, c.ReduceFiles...)
    }
    if c.IsAllReduceTaskDone() {
        ret = 1
    }
    rsp.Ret = ret
    return nil
}

//
// worker submit a reduce task.
//
func (c *Coordinator) SubmitReduceTask(req *SubmitReduceTaskReq, rsp *SubmitReduceTaskRsp) error {
    c.ReduceTaskMutex.Lock()
    defer c.ReduceTaskMutex.Unlock()
    for idx, task := range c.ReduceTasks {
        if req.ReduceN == task.ReduceN && !task.Done {
            c.ReduceTasks[idx].Doing = false
            c.ReduceTasks[idx].Done = true
            c.ReduceTasks[idx].DoneTime = time.Now().Unix()
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
    c.ReduceTaskMutex.Lock()
    defer c.ReduceTaskMutex.Unlock()
    ret = c.AllReduceTaskDone
    if ret {
        for _, file := range c.ReduceFiles {
            os.Remove(file)
        }
    }
	return ret
}

//
// check if all task done
//
func (c *Coordinator) IsAllTaskDone() bool {
    if c.AllTaskDone {
        return c.AllTaskDone
    }
    for _, task := range c.Tasks {
        if !task.Done {
            return false
        }
    }
    c.AllTaskDone = true
    return c.AllTaskDone
}

//
// check if all reduce task done
//
func (c *Coordinator) IsAllReduceTaskDone() bool {
    if c.AllReduceTaskDone {
        return c.AllReduceTaskDone
    }
    for _, task := range c.ReduceTasks {
        if !task.Done {
            return false
        }
    }
    c.AllReduceTaskDone = true
    return c.AllReduceTaskDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
    DPrintln("INFO: Coordinator start")
    c.AllTaskDone = false
    c.ReduceCount = nReduce
    for _, file := range files {
        task := Task {
            File: file,
            Doing: false,
            StartTime: 0,
            Done: false,
            DoneTime: 0,
        }
        c.Tasks = append(c.Tasks, task)
    }

    c.AllReduceTaskDone = false
    for i := 0; i < nReduce; i++ {
        reduceTask := ReduceTask {
            ReduceN: i,
            Doing: false,
            StartTime: 0,
            Done: false,
            DoneTime: 0,
        }
        c.ReduceTasks = append(c.ReduceTasks, reduceTask)
    }

	c.server()
	return &c
}
