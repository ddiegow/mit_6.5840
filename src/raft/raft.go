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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	electionTimeout *time.Timer
	voteChan        chan bool
	leaderChan      chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	lastLogIndex := 0
	lastLogTerm := 0

	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= lastLogIndex && args.LastLogTerm >= lastLogTerm {
		reply.VoteGranted = true
		return
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeout.Reset(time.Duration(rf.getElectionTimeout()) * time.Millisecond)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	//if args.Term > rf.currentTerm {
	rf.state = FOLLOWER
	rf.currentTerm = args.Term
	rf.votedFor = -1
	//}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (2B).

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

func (rf *Raft) manageState() {
	for !rf.killed() {
		select {
		case _ = <-rf.electionTimeout.C:
			rf.mu.Lock()
			rf.electionTimeout.Reset(time.Duration(rf.getElectionTimeout()) * time.Millisecond) // reset the election timeout
			rf.currentTerm++
			rf.votedFor = rf.me  // vote for self
			rf.state = CANDIDATE // become a candidate
			receivedVotes := 1   // votes I've received, starts with one because voted for self
			lastLogIndex := 0
			lastLogTerm := 0
			if len(rf.log) > 0 {
				lastLogIndex = len(rf.log) - 1
				lastLogTerm = rf.log[lastLogIndex].Term
			}
			args := RequestVoteArgs{Term: rf.currentTerm, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
			rf.mu.Unlock()

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me { // don't send myself a request
					continue
				}
				go func(index int) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(index, &args, &reply)
					if !ok {
						rf.voteChan <- false
						return
					}
					if reply.VoteGranted {
						rf.voteChan <- true
					}
				}(i)
			}
			for {
				rf.mu.Lock()
				if rf.state == FOLLOWER {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				vote := <-rf.voteChan
				if vote {
					receivedVotes++
				}
				if receivedVotes > len(rf.peers)/2 { // we have the majority vote

					go func() {
						rf.leaderChan <- true
					}()
					break
				}
			}
		case _ = <-rf.leaderChan: // become the leader
			rf.mu.Lock()
			rf.state = LEADER
			rf.electionTimeout.Reset(time.Duration(rf.getElectionTimeout()) * time.Millisecond)
			rf.mu.Unlock()
			for {
				rf.electionTimeout.Reset(time.Duration(rf.getElectionTimeout()) * time.Millisecond) // reset our election timer
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				args := AppendEntriesArgs{Term: rf.currentTerm}
				checkReply := make(chan bool)
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(index int) {
						reply := AppendEntriesReply{}
						ok := rf.sendAppendEntries(index, &args, &reply)
						if ok {
							checkReply <- true
						}
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.state = FOLLOWER
							rf.currentTerm = reply.Term
							rf.votedFor = -1
						}
						rf.mu.Unlock()
						go func() { // check for server timeout
							select {
							case _ = <-checkReply: // if we got a reply, we will get something through this channel
								return
							case <-time.After(time.Duration(500) * time.Millisecond): // if we don't get a reply this timer will activate and reset server to follower
								rf.mu.Lock()
								rf.state = FOLLOWER
								rf.votedFor = -1
								rf.mu.Unlock()
							}
						}()
					}(i)
				}
				time.Sleep(time.Duration(10) * time.Millisecond)
			}
		}
	}
}
func (rf *Raft) getElectionTimeout() int64 {
	ms := 50 + (rand.Int63() % 300)
	return ms

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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.matchIndex = make([]int, 0)
	rf.nextIndex = make([]int, 0)
	rf.lastApplied = 0
	rf.electionTimeout = time.NewTimer(time.Duration(rf.getElectionTimeout()) * time.Millisecond)
	rf.voteChan = make(chan bool)
	rf.leaderChan = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.manageState()
	return rf
}
