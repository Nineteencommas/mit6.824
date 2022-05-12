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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	MASTER_TIMEOUT = 100
	MIN_TIMEOUT    = 500
	MAX_TIMEOUT    = 800
)

type ServerState int32

const (
	STATE_LEADER    ServerState = 0
	STATE_CANDIDATE ServerState = 1
	STATE_FOLLOWER  ServerState = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the servi·c·e (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	timeout     time.Duration
	myState     ServerState
	reset       bool
	resetMu     sync.Mutex
	// below is for 2B
	log           []Entry //  stores the command and term, in the test command is ignored, only term is stored
	commitedIndex int
	lastApplied   int
	//2B for leader
	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.myState == STATE_LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int //candidate requesting vote
	//lastLogIndex int32   //index of candidate's last log

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm for the candidate to update itself
	VoteGranted bool //true if candidate received vote
}

type Entry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	} else {
		if rf.myState == STATE_FOLLOWER {
			rf.resetMu.Lock()
			rf.reset = true
			rf.resetMu.Unlock()
		} else {
			rf.myState = STATE_FOLLOWER
		}
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	rf.mu.Unlock()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Success = true
		if rf.myState == STATE_FOLLOWER {
			rf.resetMu.Lock()
			rf.reset = true
			rf.resetMu.Unlock()
		} else {
			rf.myState = STATE_FOLLOWER
		}

		if len(rf.log) < args.PrevLogIndex {
			reply.Success = false
			return
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			rf.log = rf.log[args.PrevLogIndex : len(rf.log)-1]
			return
		}
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		if args.LeaderCommit > rf.commitedIndex {
			if args.LeaderCommit > len(rf.log) {
			}
			rf.commitedIndex = Min(len(rf.log)-1, args.LeaderCommit)
		}
	}
}

func Min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func (rf *Raft) replicate() {
	count := 0
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		result := false
		for !result {
			args := AppendEntriesArgs{rf.currentTerm, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-2].Term, rf.getEntriesToSend(peer), rf.commitedIndex}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(peer, &args, &reply)
			result = reply.Success
			if !result {
				rf.nextIndex[peer]--
			} else {
				count++
				rf.nextIndex[peer] = len(rf.log)
				rf.matchIndex[peer] = len(rf.log) - 1
			}
		}

		// TODO the count need to be changed
		if count > len(rf.peers)/2 {
			if rf.currentTerm == rf.log[len(rf.log)-1].Term {
				rf.commitedIndex = len(rf.log) - 1
			}
		}
	}
}

func (rf *Raft) getEntriesToSend(peer int) []Entry {
	nextIndex := rf.nextIndex[peer]
	return rf.log[nextIndex+1 : len(rf.log)-1]
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.myState != STATE_LEADER {
		isLeader = false

	} else {
		index = rf.commitedIndex + 1
		term = rf.currentTerm
		go rf.replicate()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(peers []*labrpc.ClientEnd) {
	rf.mu.Lock()
	tCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			fmt.Printf("%v is sending heartbeat to %v at term %v\n", rf.me, i, tCurrentTerm)
			args := AppendEntriesArgs{tCurrentTerm, rf.me}
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(i, &args, &reply)
			if reply.Term > tCurrentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.myState = STATE_FOLLOWER
				rf.votedFor = -1
				rf.mu.Unlock()
			}
		}(i)

	}
}

func (rf *Raft) candidateThing(peers []*labrpc.ClientEnd) {
	rf.mu.Lock()
	rf.currentTerm++
	fmt.Printf("%v is the candidate at term %v \n", rf.me, rf.currentTerm)
	tCurrentTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.mu.Unlock()
	//to visit everyone
	count := 1
	chVotes := make(chan bool)
	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			fmt.Println("One vote request is sent")
			args := RequestVoteArgs{Term: tCurrentTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			ok := rf.peers[i].Call("Raft.RequestVote", &args, &reply)
			if !ok {
				fmt.Printf("One bad connection from %v \n", i)
			}
			chVotes <- reply.VoteGranted
		}(i)
	}
	chVoteFinished := make(chan bool, 1)
	go func() {
		for j := 0; j < len(peers)-1; j++ {
			v := <-chVotes
			if v {
				count++
				fmt.Printf("One vote is received and the count is %v. \n", count)
			}
			if count >= 2 {
				fmt.Println("The vote received is larger than 2")
				chVoteFinished <- true
				rf.mu.Lock()
				if rf.myState == STATE_CANDIDATE {
					rf.myState = STATE_LEADER
					fmt.Printf("The received votes for %v is %v \n", rf.me, count)
					rf.currentTerm++
				}
				rf.mu.Unlock()
			}
		}

	}()
	select {
	case value := <-chVoteFinished:
		if !value {
			rf.mu.Lock()
			rf.votedFor = -1
			rf.mu.Unlock()
			time.Sleep(rf.timeout)
		}
		fmt.Println("Vote is finished")
	case <-time.After(rf.timeout):
		fmt.Println("Election timeout")
		rf.mu.Lock()
		rf.votedFor = -1
		rf.mu.Unlock()
		time.Sleep(rf.timeout)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.commitedIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.CommandIndex = rf.commitedIndex
			applyMsg.Command = rf.log[len(rf.log)-1].Command
			rf.applyCh <- applyMsg
		}
		if rf.myState == STATE_LEADER {
			fmt.Printf("%v is the leader at term %v \n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			rf.sendHeartBeat(rf.peers)
			time.Sleep(time.Duration(MASTER_TIMEOUT) * time.Millisecond)
		} else if rf.myState == STATE_CANDIDATE {
			rf.mu.Unlock()
			rf.candidateThing(rf.peers)
		} else {
			fmt.Printf("%v is the follower at term %v \n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			time.Sleep(rf.timeout)
			rf.resetMu.Lock()
			if !rf.reset {
				rf.mu.Lock()
				rf.myState = STATE_CANDIDATE
				rf.mu.Unlock()
			}
			rf.reset = false
			rf.resetMu.Unlock()
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.mu.Lock()
	// Your initialization code here (2A, 2B, 2C).

	// rf.chHeartReset = make (chan int,10)
	// rf.chVoteReset =  make(chan int,10)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.timeout = time.Millisecond * time.Duration(rand.Int63n(MAX_TIMEOUT-MIN_TIMEOUT)+MIN_TIMEOUT)
	rf.myState = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
