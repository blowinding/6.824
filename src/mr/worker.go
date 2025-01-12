package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	imPrefixPath      string = ""
	imFilePattern     string = `mr-%d-%d`
	outputFilePattern string = `mr-out-%d`
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	pid := os.Getpid()
	name := fmt.Sprintf("/home/dujinnuo/6.824/log/mr/%s_worker_%d", os.Getenv("TASK"), pid)
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	// log
	mrLog := MRLog{}
	defer file.Close()
	mrLog.logSetOutput(file)
	mrLog.logSetLevel(os.Getenv("LOG_LEVEL"))
	for {
		time.Sleep(time.Duration(20) * time.Millisecond)
		args := RequestArgs{}
		args.Pid = pid
		reply := RequestReply{}
		ok := call("Coordinator.RequestWork", &args, &reply)
		if ok {
			mrLog.logMessage(LevelInfo, "pid:", args.Pid, " type:", reply.RequestType, " path:", reply.FilePaths)
			response := []string{}
			filePaths := []string{}
			var err error
			if reply.RequestType == MAP {
				// map task only has one path
				filePaths, err = workerMap(mapf, reply.FilePaths[0], reply.RequestSeq, reply.NReduce)
			} else if reply.RequestType == REDUCE {
				err = workerReduce(reducef, reply.FilePaths, reply.RequestSeq)
			} else if reply.RequestType == NONE {
				continue
			}

			if err == nil {
				response = append(response, RESOK)
				response = append(response, filePaths...)
			} else {
				response = append(response, RESERROR)
				response = append(response, err.Error())
			}
			mrLog.logMessage(LevelInfo, "response:", response)
			args := ResponseArgs{}
			args.Pid = pid
			args.Response = response
			reply := ResponseReply{}
			if ok := call("Coordinator.ResponseWork", &args, &reply); !ok {
				mrLog.logMessage(LevelInfo, "call error")
				break
			}
		} else {
			mrLog.logMessage(LevelInfo, "worker done")
			break
		}
	}

}

func workerMap(mapf func(string, string) []KeyValue, filePath string, mapSeq int, nReduce int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("cannot open %v", filePath)
		return nil, err
	}
	defer file.Close()
	// read file content
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filePath)
		return nil, err
	}
	// partition
	kva := mapf(filePath, string(content))
	kvaMap := partition(kva, nReduce)
	// output intermediate file
	filePaths := []string{}
	for key, value := range kvaMap {
		file, err := os.CreateTemp(imPrefixPath, "")
		if err != nil {
			log.Fatalf("cannot create %v", file.Name())
			return nil, err
		}

		enc := json.NewEncoder(file)
		err = enc.Encode(value)
		if err != nil {
			log.Fatalf("Error encoding JSON: %v", err)
			return nil, err
		}

		intermediate := fmt.Sprintf(imFilePattern, mapSeq, key)
		if err := os.Rename(file.Name(), filepath.Join(imPrefixPath, intermediate)); err != nil {
			log.Fatalf("rename error:%v", err.Error())
		}
		filePaths = append(filePaths, filepath.Join(imPrefixPath, intermediate))
	}
	return filePaths, nil
}

func partition(kva []KeyValue, nReduce int) map[int][]KeyValue {
	kvaMap := make(map[int][]KeyValue)
	for _, kv := range kva {
		key := ihash(kv.Key)
		key = key % nReduce
		kvaMap[key] = append(kvaMap[key], kv)
	}
	return kvaMap
}

func workerReduce(reducef func(string, []string) string, filePaths []string, reduceSeq int) error {
	// read intermediate json file
	kva_all := []KeyValue{}
	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalln("Error Open ", filePath)
			return err
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		kva := []KeyValue{}
		err = dec.Decode(&kva)
		if err != nil {
			log.Fatalln("Error Decoding JSON: ", err)
			return err
		}
		kva_all = append(kva_all, kva...)
		// os.Remove(filePath)
	}

	// emit
	sort.Sort(ByKey(kva_all))
	oname := fmt.Sprintf(outputFilePattern, reduceSeq)
	ofile, err := os.CreateTemp(imPrefixPath, "")
	if err != nil {
		log.Fatalf("cannot create %v", ofile.Name())
		return err
	}

	i := 0
	for i < len(kva_all) {
		j := i + 1
		for j < len(kva_all) && kva_all[j].Key == kva_all[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva_all[k].Value)
		}
		output := reducef(kva_all[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva_all[i].Key, output)

		i = j
	}
	if err := os.Rename(ofile.Name(), filepath.Join(imPrefixPath, oname)); err != nil {
		log.Fatalf("rename error:%v", err.Error())
	}
	ofile.Close()
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
