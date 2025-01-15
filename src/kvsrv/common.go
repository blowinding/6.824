package kvsrv

const (
	OK int = iota
	FAIL
)

const (
	DURATION int = 10 //ms
)

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Identify int64
	Version  int
}

type PutAppendReply struct {
	Value    string
	Version  int
}

type GetArgs struct {
	Key      string
	Identify int64
}

type GetReply struct {
	Value   string
	Version int
}
