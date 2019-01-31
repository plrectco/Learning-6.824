/**
 * File   : raft.go
 * License: MIT
 * Author : Xinyue Ou <xinyue3ou@gmail.com>
 * Date   : 20.01.2019
 */
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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	heartbeatrate      int = 150
	randomBackoffBase  int = 100
	randomBackoffRange int = 500
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
	Log  string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int        // initially 0
	votedFor    int        // initially -1
	leader      int        // initially -1
	log         []LogEntry // First valid index is 1

	// Volatile state
	commitIndex   int         // index of the highest log entry that has been commited
	lastApplied   int         // index of the highest log entry that has been applied
	electionTimer *time.Timer // Timer for election timer out

	// Leader state
	nextIndex  []int // for each server, the index of the highest log entry known to be sent
	matchIndex []int // for each server, the index of the highest log entry knwon to be replicated

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.leader == rf.me
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rt *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if arg.Term >= rt.currentTerm {
		// Transform to a follower
		rt.electionTimer.Reset(time.Duration(rand.Intn(randomBackoffRange)+randomBackoffBase) * (time.Millisecond))
		rt.leader = arg.LeaderId
		rt.currentTerm = arg.Term
		rt.votedFor = -1
		lastLogIndex := len(rt.log) - 1
		if lastLogIndex >= arg.PrevLogIndex && rt.log[arg.PrevLogIndex].Term == arg.PrevLogTerm {
			for i, e := range arg.Entries {
				nextIndexToAdd := arg.PrevLogIndex + 1 + i
				if nextIndexToAdd <= lastLogIndex {
					rt.log[nextIndexToAdd] = e
				} else {
					rt.log = append(rt.log, e)
				}
			}
			if rt.commitIndex < arg.LeaderCommit {
				if arg.LeaderCommit > len(rt.log) {
					rt.commitIndex = len(rt.log) - 1
				} else {
					rt.commitIndex = arg.LeaderCommit
				}

			}
			reply.Term = arg.Term
			reply.Success = true
			return
		}
	}
	// AppendEntries Fails
	// The leader is either fall behind in term
	// or the log entry does not match with the current raft log
	reply.Term = rt.currentTerm
	reply.Success = false

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.currentTerm <= args.Term {
		DPrintf("%d:term %d, %d: term %d", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			DPrintf("voter: commit id %d", rf.commitIndex)
			DPrintf("candidate: commit id %d", args.LastLogIndex)
			if rf.commitIndex <= args.LastLogIndex && rf.log[rf.commitIndex].Term <= args.LastLogTerm {
				rf.votedFor = args.CandidateID
				reply.Term = args.Term
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) HeartBeat() {
	for rf.leader == rf.me {
		timer := time.NewTimer(time.Duration(heartbeatrate) * (time.Millisecond))
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1)

		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				peer := i

				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				copy(args.Entries, rf.log[rf.nextIndex[peer]:])
				args.LeaderCommit = rf.commitIndex

				var reply AppendEntriesReply
				go func() {
					ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
					if ok {
						if reply.Term == rf.currentTerm && !reply.Success {
							rf.nextIndex[peer]--
						}
						DPrintf("Append from %d to %d", rf.me, peer)
					}
					wg.Done()
				}()
			}
		}
		wg.Wait()
		<-timer.C
	}
}

func (rf *Raft) BoostrapLeader() {
	rf.mu.Lock()
	rf.leader = rf.me
	// Change leader state
	if rf.nextIndex == nil {
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = 1
		}
		copy(rf.matchIndex, rf.nextIndex)
	}
	rf.mu.Unlock()
	rf.HeartBeat()
}

func (rf *Raft) LeaderElection() {
	for {
		if rf.leader != rf.me {
			// Wait for time out
			<-rf.electionTimer.C
			// Become a candidate
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.currentTerm++
			DPrintf("%d is now a candidate, term #%d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			count := 1
			var wg sync.WaitGroup
			wg.Add(len(rf.peers) - 1)
			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					peer := i
					var reply RequestVoteReply
					// Send out requestvote
					go func() {
						ok := rf.sendRequestVote(peer, &args, &reply)
						DPrintf("Get a vote from %d, to elect %d, %t", peer, rf.me, ok)
						if ok {
							DPrintf("Vote is %t", reply.VoteGranted)
						}

						if ok && reply.VoteGranted {
							count++
						}
						wg.Done()
					}()
				}
			}

			wg.Wait()
			DPrintf("%d: term#%d, get votes:%d", rf.me, rf.currentTerm, count)
			if count > len(rf.peers)/2 {
				DPrintf("%d is elected to be leader", rf.me)
				rf.BoostrapLeader()
			}

			rf.mu.Lock()
			rf.votedFor = -1
			rf.mu.Unlock()

			rf.electionTimer.Reset(time.Duration(rand.Intn(randomBackoffRange)+randomBackoffBase) * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = append(rf.log, LogEntry{Term: 0})

	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(time.Duration(rand.Intn(randomBackoffRange)+randomBackoffBase) * time.Millisecond)

	// Leader Election
	go rf.LeaderElection()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
