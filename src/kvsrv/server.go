package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type serverReply struct {
	Version int
	Value   string
}

type KVServer struct {
	mu         sync.Mutex
	IdentifyMap map[int64]serverReply
	Store      map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("SERVER version:%d", kv.IdentifyMap[args.Identify].Version)
	DPrintf("GET key:%s", args.Key)
	reply.Value = kv.Store[args.Key]
	reply.Version = kv.IdentifyMap[args.Identify].Version
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("SERVER version:%d", kv.IdentifyMap[args.Identify].Version)
	DPrintf("PUT version:%d key:%s value:%s identify:%d", args.Version, args.Key, args.Value, args.Identify)
	kv.Store[args.Key] = args.Value
	reply.Value = kv.Store[args.Key]
	reply.Version = kv.IdentifyMap[args.Identify].Version
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("SERVER version:%d", kv.IdentifyMap[args.Identify].Version)
	DPrintf("APPEND version:%d key:%s value:%s identify:%d", args.Version, args.Key, args.Value, args.Identify)
	sReply := kv.IdentifyMap[args.Identify]
	if args.Version == sReply.Version {
		sReply.Value = kv.Store[args.Key]
		kv.Store[args.Key] += args.Value
		sReply.Version++
		kv.IdentifyMap[args.Identify] = sReply
		DPrintf("APPEND OK")
	} else {
		DPrintf("APPEND DUPLICATE")
	}
	reply.Value = sReply.Value
	reply.Version = sReply.Version
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.Store = make(map[string]string)
	kv.IdentifyMap = make(map[int64]serverReply)
	return kv
}
