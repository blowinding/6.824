package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"6.5840/mr"
)

func main() {
	go func ()  {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	name := fmt.Sprintf("../../../log/mr/%s_coordinator", os.Getenv("TASK"))
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		os.Exit(1)
	}
	defer file.Close() // 确保文件在退出时被关闭
	m := mr.MakeCoordinator(os.Args[1:], 10, file)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
