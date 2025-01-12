package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	TIMEOUT int = 10
)

type taskStat struct {
	FilePaths []string
	Pid       int // no process: -1
	Type      int // map or reduce
	Seq       int
}

func (c *Coordinator) PrintTask(info string, task taskStat) {
	c.Log.logMessage(LevelInfo, "info:", info,
		" pid:", task.Pid, " type:", task.Type, " seq:", task.Seq, " filepaths:", task.FilePaths)
}

type taskStatQueue struct {
	items []taskStat
}

func (s *taskStatQueue) Push(item taskStat) {
	s.items = append(s.items, item)
}

func (s *taskStatQueue) Pop() (taskStat, bool) {
	if len(s.items) == 0 {
		return taskStat{}, false
	}
	item := s.items[0]
	s.items = s.items[1:]
	return item, true
}

func (s *taskStatQueue) Peek() (taskStat, bool) {
	if len(s.items) == 0 {
		return taskStat{}, false
	}
	return s.items[0], true
}

type Coordinator struct {
	IdleQueue         taskStatQueue
	WorkingTaskMap    map[int]chan []string
	IntermediateMap   [][]string
	NMap              int
	NReduce           int
	CompleteMapNum    int
	CompleteReduceNum int
	IsReduceTaskInit  bool
	IsDone						bool
	mu                sync.Mutex
	Log               MRLog
}

func (c *Coordinator) initReduceTask() {
	for seq, filePaths := range c.IntermediateMap {
		c.IdleQueue.Push(
			taskStat{
				FilePaths: filePaths,
				Pid:       -1,
				Type:      REDUCE,
				Seq:       seq,
			},
		)
	}
}

func (c *Coordinator) RequestWork(args *RequestArgs, reply *RequestReply) error {
	c.Log.logMessage(LevelInfo, "request start ", args.Pid)
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduce = c.NReduce
	// process task
	if task, ok := c.IdleQueue.Peek(); ok {
		c.IdleQueue.Pop()
		reply.FilePaths = task.FilePaths
		reply.RequestSeq = task.Seq
		reply.RequestType = task.Type
		task.Pid = args.Pid
		ch := make(chan []string)
		c.WorkingTaskMap[task.Pid] = ch
		c.PrintTask("push task", task)
		go c.taskHandler(task, ch)
	} else {
		reply.RequestType = NONE
		c.Log.logMessage(LevelInfo, "no tasks")
	}
	c.Log.logMessage(LevelInfo, "request end ", args.Pid)
	return nil
}

func (c *Coordinator) ResponseWork(args *ResponseArgs, reply *ResponseReply) error {
	c.Log.logMessage(LevelInfo, "response start ", args.Pid)
	c.mu.Lock()
	if ch, ok := c.WorkingTaskMap[args.Pid]; ok {
		ch <- args.Response
	}
	c.mu.Unlock()
	c.Log.logMessage(LevelInfo, "response end ", args.Pid)
	return nil
}

func (c *Coordinator) taskHandler(task taskStat, ch chan []string) {
	var filePaths []string
	isTimeOut := false
	// collect reply
	c.PrintTask("pending start", task)
	select {
	case response := <-ch:
		if len(response) > 0 {
			if response[0] == RESOK {
				filePaths = response[1:]
				c.Log.logMessage(LevelInfo, "OK:", response)
			} else if response[0] == RESERROR {
				return
			} else {
				return
			}
		} else {
			return
		}
	case <-time.After(time.Duration(TIMEOUT) * time.Second):
		isTimeOut = true
	}

	c.PrintTask("pending end", task)
	// add idle_Queue
	c.mu.Lock()
	if isTimeOut {
		c.PrintTask("timeout", task)
		c.IdleQueue.Push(task)
	} else {
		if task.Type == MAP {
			for _, filePath := range filePaths {
				name := filepath.Base(filePath)
				var (
					mapSeq    int
					reduceSeq int
				)
				fmt.Sscanf(name, imFilePattern, &mapSeq, &reduceSeq)
				c.IntermediateMap[reduceSeq] = append(c.IntermediateMap[reduceSeq], filePath)
			}
			c.CompleteMapNum++
			c.PrintTask("complete map num ++", task)
		} else if task.Type == REDUCE {
			c.CompleteReduceNum++
			c.PrintTask("complete reduce num ++", task)
		}
	}
	delete(c.WorkingTaskMap, task.Pid)
	// push reduce task
	if c.CompleteMapNum >= c.NMap && !c.IsReduceTaskInit {
		c.initReduceTask()
		c.IsReduceTaskInit = true
	}
	c.IsDone = (c.CompleteMapNum == c.NMap && c.CompleteReduceNum == c.NReduce)
	c.mu.Unlock()
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		c.Log.logMessage(LevelError, "listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	return c.IsDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int, file *os.File) *Coordinator {
	c := Coordinator{}

	// init coordinator
	c.NMap = len(files)
	c.NReduce = nReduce
	c.CompleteMapNum = 0
	c.CompleteReduceNum = 0
	c.IsReduceTaskInit = false
	c.IsDone = false
	for i := 0; i < c.NMap; i++ {
		task := taskStat{
			FilePaths: []string{files[i]},
			Pid:       -1,
			Type:      MAP,
			Seq:       i,
		}
		c.IdleQueue.Push(task)
	}
	c.WorkingTaskMap = make(map[int]chan []string)
	c.IntermediateMap = make([][]string, nReduce)
	for i := range c.IntermediateMap {
		c.IntermediateMap[i] = make([]string, 0)
	}
	c.server()
	// log
	c.Log = MRLog{}
	c.Log.logSetLevel(os.Getenv("LOG_LEVEL"))
	c.Log.logSetOutput(file)

	c.Log.logMessage(LevelInfo, "begin listening workers")
	return &c
}
