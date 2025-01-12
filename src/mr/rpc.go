package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"io"
	"log"
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

// Add your RPC definitions here.
type RequestArgs struct {
	Pid int
}

type ResponseArgs struct {
	Pid int
	Response []string
}

const (
	MAP int = iota
	REDUCE
	NONE
)

const (
	RESOK string = "OK"
	RESERROR string = "ERROR"
)

type RequestReply struct {
	RequestType int
	RequestSeq  int
	NReduce  int
	FilePaths []string
}

type ResponseReply struct {

}

// 定义日志级别
const (
	LevelInfo  = iota
	LevelWarn
	LevelError
)

type MRLog struct {
	LogLevel int
}

// 根据不同的级别输出日志
func (mrLog *MRLog) logMessage(level int, message ...interface{}) {
	if level >= mrLog.LogLevel {
		switch level {
		case LevelInfo:
			log.Println("INFO:", message)
		case LevelWarn:
			log.Println("WARN:", message)
		case LevelError:
			log.Fatalln("ERROR:", message)
		}
	}
}

func (mrLog *MRLog) logSetOutput(w io.Writer) {
	if w != nil {
		log.SetOutput(w)
	}
}

func (mrLog *MRLog) logSetLevel(s string) {
	switch s {
	case "INFO":
		mrLog.LogLevel = LevelInfo
	case "WARN":
		mrLog.LogLevel = LevelWarn
	case "ERROR":
		mrLog.LogLevel = LevelError
	default:
		mrLog.LogLevel = LevelInfo
	}
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
