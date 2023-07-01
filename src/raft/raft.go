package raft

// TODO: need to work on what happens to leader when no responses are received
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
	"fmt"
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
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

type LogEntry struct {
	Command string
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	state       int
	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases	monotonically)
	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	// Utility stuff
	voteTimer *time.Timer
	hbTicker  *time.Ticker
	verbose   bool
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

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// If we grant a vote, reset the timer!!
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.makeFollower(args.Term)
	}
	logIndex := 0
	logTerm := 0
	if len(rf.log) > 0 {
		logIndex = len(rf.log) - 1
		logTerm = rf.log[logIndex].Term
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= logIndex && args.LastLogTerm >= logTerm {
		reply.VoteGranted = true
		rf.restartTimer()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// If we grant a vote, reset the timer!!

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.verbose {
		//fmt.Printf("[%d] Received AppendEntries RPC rf.currentTerm: %d, args.Term: %d [%d]\n", rf.me, rf.currentTerm, args.Term, time.Now().Nanosecond())
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if rf.currentTerm <= args.Term {
		if rf.verbose {
			//fmt.Printf("[%d] Got heartbeat, becoming follower [%d]\n", rf.me, time.Now().Nanosecond())
		}
		rf.makeFollower(args.Term)
		rf.restartTimer()
	}
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

func (rf *Raft) startVote() {
	if rf.verbose {
		fmt.Printf("[%d] Starting vote [%d]\n", time.Now().UnixMilli(), rf.me)
	}
	totalVotes := 1    // we initially counted one vote
	receivedVotes := 1 // voted for self
	rf.mu.Lock()
	rf.state = CANDIDATE                                                      // become a candidate
	rf.currentTerm++                                                          // increment current term
	rf.votedFor = rf.me                                                       // vote for self
	rf.voteTimer.Reset(time.Duration(50+rand.Int63()%300) * time.Millisecond) // reset election timer
	// send rpc vote requests
	lastLogIndex := 0
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	rf.mu.Unlock()
	voteChan := make(chan bool)
	if rf.verbose {
		//fmt.Printf("[%d] Sending vote requests [%d]\n", rf.me, time.Now().Nanosecond())
	}
	for i := 0; i < len(rf.peers); i++ { // send the votes
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go func(index int, r *RequestVoteReply) {
			ok := rf.sendRequestVote(index, &args, r)
			if !ok { // if we didn't get a reply
				voteChan <- false // send back a false vote to increment the total count
				return
			}
			rf.mu.Lock()
			if r.Term > rf.currentTerm { // if the response term is higher than ours
				rf.makeFollower(r.Term)
				rf.mu.Unlock()
			}
			rf.mu.Unlock()
			voteChan <- r.VoteGranted // send vote to vote channel
		}(i, &reply)

	}
	for { // count the votes
		rf.mu.Lock()
		if rf.state != CANDIDATE { // if we're not a candidate anymore
			if rf.verbose {
				fmt.Printf("[%d] Not candidate anymore! Leaving voting routine [%d]\n", time.Now().UnixMilli(), rf.me)
			}
			rf.votedFor = -1 // reset the vote
			rf.mu.Unlock()
			return // finish voting process
		}
		rf.mu.Unlock()
		vote := <-voteChan // get votes
		if vote {          // if we got a vote
			if rf.verbose {
				//fmt.Printf("[%d] Got a vote! [%d]\n", rf.me, time.Now().Nanosecond())
			}
			receivedVotes++ // increment the count
		}
		totalVotes++
		if rf.verbose {
			//fmt.Printf("[%d] Total votes so far: %d [%d]\n", rf.me, totalVotes, time.Now().Nanosecond())
		}
		if totalVotes == len(rf.peers) { // if we got all the votes
			if rf.verbose {
				//fmt.Printf("[%d] Received all the votes [%d]\n", rf.me, time.Now().Nanosecond())
			}
			break
		}
	}
	// finished counting
	if rf.verbose {
		fmt.Printf("[%d] Got %d out of %d votes! [%d]\n", time.Now().UnixMilli(), receivedVotes, totalVotes, rf.me)
	}
	rf.mu.Lock()
	if rf.state != CANDIDATE { // same as before
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	if receivedVotes > len(rf.peers)/2 {
		rf.mu.Lock()
		rf.state = LEADER
		rf.hbTicker = time.NewTicker(time.Duration(10) * time.Millisecond)
		go rf.hbManager()
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) hbManager() {
	if rf.verbose {
		fmt.Printf("[%d] Heartbeat started [%d]\n", time.Now().UnixMilli(), rf.me)
	}
	for !rf.killed() {
		_ = <-rf.hbTicker.C
		rf.restartTimer()
		rf.mu.Lock()
		if rf.state != LEADER { // check if we're not the leader
			if rf.verbose {
				fmt.Printf("[%d] Stopping heartbeat [%d]\n", time.Now().UnixMilli(), rf.me)
			}
			rf.hbTicker.Stop()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		args := AppendEntriesArgs{Term: rf.currentTerm}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(index int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(index, &args, &reply)
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					if rf.verbose {
						//fmt.Printf("[%d] appendEntries reply had higher term than ours. Going back to being a follower [%d]\n", rf.me, time.Now().Nanosecond())
					}
					rf.makeFollower(reply.Term)
				}
				rf.mu.Unlock()
			}(i)
		}
		rf.mu.Lock()
		if rf.state != LEADER {
			if rf.verbose {
				//fmt.Printf("[%d] Not the leader, exiting heartbeat routine [%d]\n", rf.me, time.Now().Nanosecond())
			}
			rf.restartTimer()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) manageState() {
	for !rf.killed() {
		_ = <-rf.voteTimer.C
		if rf.verbose {
			//fmt.Printf("[%d] Time to call a vote! [%d]\n", time.Now().Nanosecond(),  rf.me)
		}
		rf.startVote()

	}

}

func (rf *Raft) makeFollower(term int) {
	// we assume that we already have a lock
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.currentTerm = term

}

func (rf *Raft) restartTimer() {
	n := 50 + (rand.Int63() % 300)
	rf.voteTimer.Reset(time.Duration(n) * time.Millisecond)
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.state = FOLLOWER
	rf.verbose = true // Do we want debug messages?
	rf.voteTimer = time.NewTimer(time.Duration(50+rand.Int63()%300) * time.Millisecond)
	go rf.manageState()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
