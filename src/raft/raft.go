package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// for voteFor init
const (
	NONESERVER int = -1
)

// for states
const (
	FOLLOWER int32 = iota
	CANDIDATE
	LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// raft log entry structure
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

func (rf *Raft) println(v ...interface{}) {
	if rf.mode {
		state := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
		l := fmt.Sprintf("[me:%d] [term:%d] [state:%s]", rf.me, rf.currentTerm, state[rf.state])
		rf.rfLogger.Println(l, v)
	}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers
	currentTerm int32
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other state saved for Raft
	// has server received HeartBeat from leader within timeout
	hasRecvHB   int32
	hasRecvTerm int32
	state       int32

	// channel for decide to enter a new election
	electDecider chan bool

	// logger
	mode     bool
	rfLogger *log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int32
	var isleader bool
	// Your code here (3A).

	term = atomic.LoadInt32(&rf.currentTerm)
	isleader = atomic.LoadInt32(&rf.state) == LEADER

	return int(term), isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int32
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

type decideFunc func(*Raft, *RequestVoteArgs) (bool, string)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.yield(args.Term)
	rf.decideNewElection(args.Term)
	rf.mu.Lock()
	// define decide func array
	funcArray := make([]decideFunc, 5)
	funcArray[0] = func(rf *Raft, args *RequestVoteArgs) (res bool, reason string) {
		// reply false if term < currentTerm
		if res = rf.currentTerm <= args.Term; !res {
			reason = "rf.currentTerm > args.Term"
		}
		return res, reason
	}
	funcArray[1] = func(rf *Raft, args *RequestVoteArgs) (res bool, reason string) {
		if res = rf.state == FOLLOWER; !res {
			reason = "rf.state != FOLLOWER"
		}
		return res, reason
	}
	funcArray[2] = func(rf *Raft, args *RequestVoteArgs) (res bool, reason string) {
		if res = (rf.votedFor == NONESERVER || rf.votedFor == args.CandidateId); !res {
			reason = "votedFor is not null or candidateId"
		}
		return res, reason
	}
	funcArray[3] = func(rf *Raft, args *RequestVoteArgs) (res bool, reason string) {
		// If votedFor is null or candidateId(has voted to the same server), and candidate's log
		// is at least as up-to-date as receiver's log, grant vote
		lastLog := LogEntry{}
		if len(rf.log) > 0 {
			lastLog = rf.log[len(rf.log)-1]
		}
		if res = (lastLog.Term <= args.LastLogTerm); !res {
			reason = "votedFor is not null or candidateId"
		}
		return res, reason
	}
	funcArray[4] = func(rf *Raft, args *RequestVoteArgs) (res bool, reason string) {
		lastLog := LogEntry{}
		if len(rf.log) > 0 {
			lastLog = rf.log[len(rf.log)-1]
		}
		if res = (lastLog.Term == args.LastLogTerm && len(rf.log) <= args.LastLogIndex); !res {
			reason = "votedFor is not null or candidateId"
		}
		return res, reason
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	for _, f := range funcArray {
		res, reason := f(rf, args)
		if reply.VoteGranted = reply.VoteGranted && res; !reply.VoteGranted {
			rf.println("vote reject", args.CandidateId, "because:", reason)
			break
		}
	}
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.println("vote agreed", args.CandidateId)
	}
	rf.mu.Unlock()
	
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(args RequestVoteArgs) bool {
	reqMu := sync.Mutex{}
	cond := sync.NewCond(&reqMu)

	recvCount := 0
	voteCount := 0
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(index int) {
				if atomic.LoadInt32(&rf.state) != CANDIDATE {
					cond.Broadcast()
					return
				}
				rf.println("request vote", index)
				reply := RequestVoteReply{}
				ok := rf.peers[index].Call("Raft.RequestVote", &args, &reply)
				reqMu.Lock()
				defer reqMu.Unlock()
				if ok {
					if reply.VoteGranted {
						voteCount++
					}
					rf.yield(reply.Term)
				}
				recvCount++
				cond.Broadcast()
			}(i)
		}
	}
	reqMu.Lock()
	for voteCount+1 <= (len(rf.peers)-1)/2 && recvCount < len(rf.peers)-1 && atomic.LoadInt32(&rf.state) == CANDIDATE {
		cond.Wait()
	}
	reqMu.Unlock()
	return voteCount+1 > (len(rf.peers)-1)/2
}

// AppendEntries RPC structures
type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int
	PrevLogIndex uint32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit uint32
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) sendHeartbeats(args AppendEntriesArgs) {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(index int) {
				reply := AppendEntriesReply{}
				rf.println("send heartbeat to", index)
				ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
				if atomic.LoadInt32(&rf.state) != LEADER {
					return
				}
				rf.println("send heartbeat finish", index)
				if ok {
					rf.println("recv response from", index)
					rf.yield(reply.Term)
					rf.decideNewElection(reply.Term)
				} else {
					rf.println("not recv response from", index)
				}
			}(i)
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	rf.yield(args.Term)
	rf.decideNewElection(args.Term)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) yield(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.println("recv", term, "yield")
		rf.currentTerm = term
		rf.votedFor = NONESERVER
		atomic.StoreInt32(&rf.state, FOLLOWER)
	}
}

func (rf *Raft) decideNewElection(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.println("decide new election", rf.currentTerm > term)
	rf.electDecider <- false
}

func (rf *Raft) newElection() {
	rf.mu.Lock()
	atomic.AddInt32(&rf.currentTerm, 1)
	rf.votedFor = NONESERVER
	atomic.StoreInt32(&rf.state, CANDIDATE)
	// get lastLogIndex and lastLogTerm
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	appendEntriesArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: 0,
	}
	rf.mu.Unlock()
	if electionResult := rf.sendRequestVote(requestVoteArgs); electionResult {
		rf.leaderAction(appendEntriesArgs)
	}

}

func (rf *Raft) leaderAction(args AppendEntriesArgs) {
	atomic.StoreInt32(&rf.state, LEADER)
	for !rf.killed() && atomic.LoadInt32(&rf.state) == LEADER {
		go rf.sendHeartbeats(args)
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 450 + (rand.Int63() % 150)
		shouldNewElection := false
		select {
		case shouldNewElection = <-rf.electDecider:
		case <-time.After(time.Duration(ms) * time.Millisecond):
			if atomic.LoadInt32(&rf.state) == LEADER {
				rf.println("leader timeout")
				atomic.StoreInt32(&rf.state, FOLLOWER)
				continue
			}
			shouldNewElection = true
		}
		if shouldNewElection {
			if atomic.LoadInt32(&rf.state) != LEADER {
				go rf.newElection()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// init persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = NONESERVER
	rf.log = make([]LogEntry, 0)

	// init volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// init volatile state on leaders
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// init other state
	rf.hasRecvHB = 0
	rf.hasRecvTerm = 0
	rf.state = FOLLOWER

	// init debug
	logFile, err := os.OpenFile("debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("file open error")
	}
	rf.rfLogger = log.New(logFile, "raft:", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	rf.mode = true
	rf.println("----------------start-------------------")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// initialize election decider
	rf.electDecider = make(chan bool)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
