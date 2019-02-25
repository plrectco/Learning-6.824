/**
 * File   : raft.go
 * License: MIT
 * Author : Xinyue Ou <xinyue3ou@gmail.com>
 * Date   : 20.01.2019 */
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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	heartbeatrate      int = 50
	timeout            int = 20
	randomBackoffBase  int = 200
	randomBackoffRange int = 500
)

// A go flavor definition of Enum
type State int

const (
	Follower State = iota
	Candidate
	Leader
	Dead
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
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state
	currentTerm int        // initially 0
	votedFor    int        // initially -1, whenever currentTerm change, reset to be -1
	log         []LogEntry // First valid index is 1

	// Volatile state
	commitIndex   int         // index of the highest log entry that has been commited
	lastApplied   int         // index of the highest log entry that has been applied
	electionTimer *time.Timer // Timer for election timer out
	state         State

	// Leader state
	nextIndex  []int // for each server, the index of the highest log entry known to be sent
	matchIndex []int // for each server, the index of the highest log entry knwon to be replicated

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Error when decoding state.")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		DPrintf("%d: recover with term %d", rf.me, rf.currentTerm)
	}
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
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// in reverse order
	defer rf.mu.Unlock()
	defer rf.persist()
	lastLogIndex := len(rf.log) - 1
	if arg.Term >= rf.currentTerm { // Transform to a follower
		// Convert to a follwer.
		rf.state = Follower
		// rf.clientCond.Broadcast()
		// DPrintf("Appending leader term %d, follower term %d", arg.Term, rf.currentTerm)
		rf.currentTerm = arg.Term

		rf.electionTimer.Reset(getElectionTimeOutValue())
		if lastLogIndex >= arg.PrevLogIndex && rf.log[arg.PrevLogIndex].Term == arg.PrevLogTerm {
			// Should discard anything beyond the leader's log, when the follower has a longer log than the leader.
			rf.log = rf.log[:arg.PrevLogIndex+1]
			for _, e := range arg.Entries {
				rf.log = append(rf.log, e)
			}
			if rf.commitIndex < arg.LeaderCommit {
				if arg.LeaderCommit > len(rf.log)-1 {
					rf.commitIndex = len(rf.log) - 1
				} else {
					rf.commitIndex = arg.LeaderCommit
				}

			}
			reply.Term = arg.Term
			reply.Success = true
			reply.NextIndex = 0
			return
		}
	}
	// AppendEntries Fails
	// The leader is either fall behind in term
	// or the log entry does not match with the current raft log
	reply.Term = rf.currentTerm
	reply.Success = false
	if lastLogIndex >= arg.PrevLogIndex {
		// There is a conflict.
		// Find the first index of the conflicting term.
		conflictTerm := rf.log[arg.PrevLogIndex].Term
		i := arg.PrevLogIndex
		for i >= 0 && rf.log[i].Term == conflictTerm {
			i--
		}
		// NextIndex will always be greater than 0.
		reply.NextIndex = i + 1
	} else {
		reply.NextIndex = lastLogIndex + 1
	}

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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Guard the entire function with the lock.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm <= args.Term {
		// Catch up in logic time.
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			// Convert to follower.
			rf.state = Follower
			// Haven't voted for this term.
			rf.votedFor = -1
		}

		// The request is from the same candidate, we check it again.
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			// Haven't voted yet.
			myLastLogIndex := len(rf.log) - 1

			DPrintf("Term %d, voter %d:term %d index %d, cand %d: term %d index %d", rf.currentTerm, rf.me, rf.log[myLastLogIndex].Term, myLastLogIndex, args.CandidateID, args.LastLogTerm, args.LastLogIndex)
			// Vote  yes if my log is more up to date
			// if the candidate has higher term, or if has equal term but longer index,
			// grant the vote
			if rf.log[myLastLogIndex].Term < args.LastLogTerm || (rf.log[myLastLogIndex].Term == args.LastLogTerm && myLastLogIndex <= args.LastLogIndex) {
				rf.electionTimer.Reset(getElectionTimeOutValue())
				rf.votedFor = args.CandidateID
				reply.Term = args.Term
				reply.VoteGranted = true
				return
			}
		}
	}
	// At this point rf.currentTerm >= args.Term.
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
// command will ever be_committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term, isLeader := rf.GetState()

	if !isLeader {
		return -1, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1

	// for rf.commitIndex < index {
	//     DPrintf("Waiting for index %d", index)
	//     rf.clientCond.Wait()
	//     term, isLeader = rf.GetState()
	//     if !isLeader {
	//         return -1, term, isLeader
	//     }
	// }
	// term, isLeader = rf.GetState()
	//
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
	rf.mu.Lock()
	rf.state = Dead
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) heartBeat() {
	for rf.state == Leader {
		highestTermSeen := rf.currentTerm
		count := 1
		total := len(rf.peers) - 1
		hasNotified := false
		followerChan := make(chan bool)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// DPrintf("Term %d: Append from %d to %d", rf.currentTerm, rf.me, peer)
				go func(peer int) {

					rf.mu.Lock()
					var args AppendEntriesArgs
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[peer] - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					entriesToAppend := rf.log[rf.nextIndex[peer]:]
					args.Entries = make([]LogEntry, len(entriesToAppend))
					// Save the temporary log state to avoid change in between the request and reply.
					nextIndex := len(rf.log)
					copy(args.Entries, entriesToAppend)
					args.LeaderCommit = rf.commitIndex
					rf.mu.Unlock()

					var reply AppendEntriesReply
					ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)

					rf.mu.Lock()
					total--
					if ok {
						// Log index not match.
						if !reply.Success {
							if reply.Term == rf.currentTerm {
								// Modifying share data.
								// Reply's nextindex is less than PrevLogIndex.
								rf.nextIndex[peer] = reply.NextIndex
							}

							if reply.Term > highestTermSeen {
								highestTermSeen = reply.Term
							}
						} else {
							rf.nextIndex[peer] = nextIndex
							rf.matchIndex[peer] = rf.nextIndex[peer] - 1
							count++
						}
						// DPrintf("Append from %d to %d, %t", rf.me, peer, reply.Success)
					}
					// Eagerly commit when more than half has replicated the logs
					if !hasNotified && (count > len(rf.peers)/2 || total == 0) {
						select {
						case followerChan <- true:
						case <-time.After(time.Duration(timeout) * time.Millisecond):
						}
						hasNotified = true
					}
					rf.mu.Unlock()
				}(i)

			}
		}
		select {
		case <-followerChan:
		case <-time.After(time.Duration(timeout) * time.Millisecond):
		}

		rf.mu.Lock()
		if highestTermSeen > rf.currentTerm {
			rf.currentTerm = highestTermSeen
			rf.state = Follower
			rf.votedFor = -1
		}

		if rf.state == Leader {
			rf.commitRoutine()
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(heartbeatrate) * time.Millisecond)
	}
}

func (rf *Raft) boostrapLeader() {
	rf.mu.Lock()
	rf.state = Leader
	// Change leader state
	if rf.nextIndex == nil {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) // last index + 1
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	rf.heartBeat()
}

// Start the leader election
func (rf *Raft) leaderElection() {
	for rf.state != Dead {

		if rf.electionTimer == nil {
			rf.electionTimer = time.NewTimer(getElectionTimeOutValue())
		} else {
			rf.electionTimer.Reset(getElectionTimeOutValue())
		}
		<-rf.electionTimer.C

		// Election time out.
		rf.mu.Lock()
		if rf.state == Follower {
			// Start a new election.
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.currentTerm++
			// DPrintf("%d is now a candidate, term #%d", rf.me, rf.currentTerm)
			var args RequestVoteArgs

			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
			// Unlock here to allow changing of state while waiting for votes.
			rf.persist()
			rf.mu.Unlock()

			count := 1
			total := len(rf.peers) - 1
			hasNotified := false
			voteChan := make(chan bool)

			highestTermSeen := rf.currentTerm
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					peer := i
					var reply RequestVoteReply
					// Send out requestvote
					go func() {

						ok := rf.sendRequestVote(peer, &args, &reply)
						rf.mu.Lock()
						total--

						if ok {
							// Use countLock to modify shared variables.

							if highestTermSeen < reply.Term {
								highestTermSeen = reply.Term
							}

							if reply.VoteGranted {
								count++
							}

							DPrintf("term %d Get a vote from %d, to elect %d, %t", rf.currentTerm, peer, rf.me, reply.VoteGranted)
						}
						if !hasNotified && (count > len(rf.peers)/2 || total == 0) {
							select {
							case voteChan <- true:
							case <-time.After(time.Duration(timeout) * time.Millisecond):
							}
							hasNotified = true
						}
						rf.mu.Unlock()
					}()
				}
			}
			select {
			case <-voteChan:
			case <-time.After(time.Duration(timeout) * time.Millisecond):
			}

			rf.mu.Lock()

			if highestTermSeen > rf.currentTerm {
				rf.state = Follower
			}

			// If I am still a candidate, i.e., no one converts me into a follower during the voting process.
			if rf.state == Candidate && count > len(rf.peers)/2 {
				DPrintf("term:%d, %d is elected to be leader", rf.currentTerm, rf.me)
				rf.mu.Unlock()
				rf.boostrapLeader()
			} else {
				rf.votedFor = -1
				rf.state = Follower
				rf.mu.Unlock()
			}

		}
	}
}

// Require holding the lock.
func (rf *Raft) commitRoutine() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	matchIndices := make([]int, len(rf.matchIndex))
	copy(matchIndices, rf.matchIndex)
	sort.Ints(matchIndices)

	// When odd, half will be the middle index
	// When even, half will be the index to the left of middle
	// For both cases, servers from half to the rightmost one will
	// at least commit matchIndices[half]
	half := len(matchIndices) / 2

	DPrintf("Matching Index %v", rf.matchIndex)

	// commitIndex should increase monotonically.
	// Only commit logs of currentTerm
	if rf.commitIndex < matchIndices[half] && rf.log[matchIndices[half]].Term == rf.currentTerm {
		rf.commitIndex = matchIndices[half]
		DPrintf("%d term%d Index commit to %d", rf.me, rf.currentTerm, rf.commitIndex)

		// rf.clientCond.Broadcast()
	}
}

func (rf *Raft) applyLogToServer() {
	for rf.state != Dead {
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				app := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
				rf.applyCh <- app
			}

		}
		time.Sleep(time.Duration(heartbeatrate*2) * time.Millisecond)
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
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		applyCh:       applyCh,
		state:         Follower,
		votedFor:      -1,
		electionTimer: nil,
		currentTerm:   0,
		commitIndex:   0,
		lastApplied:   0,
	}
	// rf.clientCond = sync.NewCond(&rf.mu)
	rf.log = append(rf.log, LogEntry{Term: 0})
	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("log length %d", len(rf.log))

	// Leader election.
	go rf.leaderElection()
	// Apply messages.
	go rf.applyLogToServer()
	return rf
}

// Return the election random time out.
func getElectionTimeOutValue() time.Duration {
	return time.Duration(rand.Intn(randomBackoffRange)+randomBackoffBase) * time.Millisecond
}
