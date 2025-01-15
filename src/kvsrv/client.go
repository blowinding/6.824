package kvsrv

import (
	"crypto/rand"
	// "log"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server   *labrpc.ClientEnd
	version  int // data version clerk hold
	identify int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.version = 0
	ck.identify = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		Identify: ck.identify,
	}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			ck.version = reply.Version
			break
		}
		time.Sleep(time.Duration(DURATION) * time.Millisecond)
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	res := ""
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Version:  ck.version,
		Identify: ck.identify,
	}
	for {
		reply := PutAppendReply{}
		args.Version = ck.version
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			// log.Printf("CLIENT GOT RESPONSE:OK %d,version %d, value %s", reply.Response, reply.Version, reply.Value)
			ck.version = reply.Version
			res = reply.Value
			// log.Println("CLIENT OK")
			break

		}
		time.Sleep(time.Duration(DURATION) * time.Millisecond)
	}
	return res
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
