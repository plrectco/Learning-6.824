/**
 * File   : raft.go
 * License: MIT
 * Author : Xinyue Ou <xinyue3ou@gmail.com> * Date   : 20.01.2019 */
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

const (
	heartbeatrate      int = 50
	timeout            int = 20
	randomBackoffBase  int = 1000
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
	CommandTerm  int
	Snapshot     []byte
}

type LogEntry struct {
	Term    int
	Index   int
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
	currentTerm       int        // initially 0
	votedFor          int        // initially -1, whenever currentTerm change, reset to be -1
	log               []LogEntry // First valid index is 1
	lastSnapshotIndex int
	lastSnapshotTerm  int

	// Volatile state
	commitIndex   int         // index of the highest log entry that has been commited
	lastApplied   int         // index of the highest log entry that has been applied
	electionTimer *time.Timer // Timer for election timer out
	state         State
	snapshot      []byte

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
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// Need to get lock
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
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		DPrintf("Error when decoding state.")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		DPrintf("%d: recover with term %d", rf.me, rf.currentTerm)
	}
}

type AppendEntriesArgs struct {
	Term         int
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
	DPrintf("append to %d", rf.me)

	// Translate global index to local index.
	localPrevIndex, found := rf.findLog(arg.PrevLogIndex)
	localPrevTerm := -1

	// Originally it is the imaginary -1 index in arg.Entries
	remoteNextIndex := -1

	if arg.Term >= rf.currentTerm { // Transform to a follower
		// Convert to a follwer.
		rf.state = Follower
		DPrintf("Appending leader term %d, follower %d term %d log length %d", arg.Term, rf.me, rf.currentTerm, len(rf.log))
		rf.currentTerm = arg.Term

		rf.electionTimer.Reset(getElectionTimeOutValue())

		if !found {
			// DPrintf("not found")
			// Three cases for not found
			// Denote argEntries range from As to Ae
			// Local log [0:S] has been snapped and [S+1:E] in log
			// case 1 Ae <= S, success, as if empty appendentry remotePrevIndex = -1, localPrevIndex = -1. This normally won't happen.
			// case 2 S<As<=E, remotePrevIndex can be obtained from following, localPrevIndex = -1
			// case 4 E < Ae, remotePrevIndex = -1, localPrevIndex = -1, append fail

			localPrevTerm = rf.lastSnapshotTerm
			// Remote's Previous log Index has been snapshot
			// If remotePrevIndex is not 0, it is impossible to fail.
			for i, e := range arg.Entries {
				// Look for the first matching index.
				if e.Index == rf.lastSnapshotIndex+1 && e.Index != 0 {
					remoteNextIndex = i
					DPrintf("%d remote index found %d", rf.me, remoteNextIndex)
					break
				}
			}
		} else {
			// One case for found
			// case 3 S < As <= E
			// DPrintf("found")
			localPrevTerm = rf.log[localPrevIndex].Term
		}

		// Case 2 and Case 3 here
		// Entries[..: remotePrevIndex+1] is in snapshot, including remotePrevIndex
		// Entries[remotePrevIndex+1: -1] are candidate to append
		DPrintf("local previndex %d, localPrevTerm %d, argument prev index term %d %d remote %d", localPrevIndex, localPrevTerm, arg.PrevLogIndex, arg.PrevLogTerm, remoteNextIndex)
		if (localPrevIndex != -1 && localPrevTerm == arg.PrevLogTerm) || (remoteNextIndex != -1) {
			// Should discard anything beyond the leader's log, when the follower has a longer log than the leader.
			rf.log = rf.log[:localPrevIndex+1]
			remoteStart := 0
			if remoteNextIndex != -1 {
				remoteStart = remoteNextIndex
			}
			for i := remoteStart; i < len(arg.Entries); i++ {
				DPrintf("Appending remote index %d global %d", remoteStart, arg.Entries[i].Index)
				rf.log = append(rf.log, arg.Entries[i])
			}
			if rf.commitIndex < arg.LeaderCommit && len(rf.log) > 0 {
				lastIndex := rf.log[len(rf.log)-1].Index
				if arg.LeaderCommit > lastIndex {
					rf.commitIndex = lastIndex
				} else {
					rf.commitIndex = arg.LeaderCommit
				}

			}
			DPrintf("success to append %d, commit index %d", rf.me, rf.commitIndex)
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
	if localPrevIndex != -1 {
		// There is a conflict.
		// Find the first index of the conflicting term.
		// case 3 with the term check fails.
		DPrintf("not match")
		conflictTerm := rf.log[localPrevIndex].Term
		i := localPrevIndex
		for i >= 0 && rf.log[i].Term == conflictTerm {
			i--
		}
		// At this point i >= -1
		// NextIndex will always be greater or equal to 0.
		reply.NextIndex = i + 1
	} else {
		// Lag in term or the current rf has a log far behind
		if len(rf.log) > 0 {
			reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
		} else {
			reply.NextIndex = rf.lastSnapshotIndex + 1
		}
		DPrintf("Append to %d fail, Peer term %d leader term %d arg index %d, next index %d", rf.me, rf.currentTerm, arg.Term, arg.PrevLogIndex, reply.NextIndex)
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
			var myLastLogIndex int
			var myLastLogTerm int
			if len(rf.log) > 0 {
				myLastLogIndex = rf.log[len(rf.log)-1].Index
				myLastLogTerm = rf.log[len(rf.log)-1].Term
			} else {
				myLastLogIndex = rf.lastSnapshotIndex
				myLastLogTerm = rf.lastSnapshotTerm
			}

			DPrintf("Term %d, voter %d:term %d index %d, cand %d: term %d index %d", rf.currentTerm, rf.me, myLastLogTerm, myLastLogIndex, args.CandidateID, args.LastLogTerm, args.LastLogIndex)
			// Vote  yes if my log is more up to date
			// if the candidate has higher term, or if has equal term but longer index,
			// grant the vote
			if myLastLogTerm < args.LastLogTerm || (myLastLogTerm == args.LastLogTerm && myLastLogIndex <= args.LastLogIndex) {
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

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d is installing snapshot index %d term %d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		// Convert to follower.
		rf.state = Follower
		// Haven't voted for this term.
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm

	if rf.commitIndex < rf.lastSnapshotIndex {
		rf.commitIndex = rf.lastSnapshotIndex
	}
	localLastIncludedIndex, found := rf.findLog(args.LastIncludedIndex)
	if !found {
		rf.log = []LogEntry{}
	} else {
		rf.log = rf.log[localLastIncludedIndex+1:]
	}
	DPrintf("%d successfully installed  snapshot index %d term %d size %d snapshot size %d", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data), len(rf.snapshot))

	rf.persistSnapshot()

}

// Should get lock before call this
// Find the log with specified index.
// Need this because log will be compact.
func (rf *Raft) findLog(index int) (int, bool) {
	for i, l := range rf.log {
		if l.Index == index {
			return i, true
		}
	}
	return -1, false
}

// Every server can snapshot its current state.
// Before call this function, make sure that the snapshot has been save to persistent
// storage.
func (rf *Raft) CompactLog(lastCommittedLogIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	localLastIncludedIndex, found := rf.findLog(lastCommittedLogIndex)
	if found {
		rf.lastSnapshotIndex = lastCommittedLogIndex
		rf.lastSnapshotTerm = rf.log[localLastIncludedIndex].Term
		rf.log = rf.log[localLastIncludedIndex:]
		rf.snapshot = snapshot
		DPrintf("Serve %d Snapshot has been taken size %d, now compacting log %d %d", rf.me, len(rf.snapshot), rf.lastSnapshotIndex, rf.lastSnapshotTerm)
		rf.persistSnapshot()
	}
	// If not found it has already been snapshot.
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
	var index int
	if len(rf.log) > 0 {
		index = rf.log[len(rf.log)-1].Index + 1
	} else {
		index = rf.lastSnapshotIndex + 1
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Index: index, Command: command})
	DPrintf("%d: Starting a command with index %d term %d", rf.me, index, term)
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

func (rf *Raft) sendAppendEntries(peer, localNextIndex int, meta *HeartBeatMeta) {
	rf.mu.Lock()

	var args AppendEntriesArgs
	args.Term = rf.currentTerm
	localPrevIndex := localNextIndex - 1
	var nextIndex int
	if localNextIndex == -1 {
		// Not found, it hasn't been received; as the snapshot case has been handled by sendInstallSnapshot.
		// Use the last logEntry as the previous log.
		if len(rf.log) == 0 {
			args.PrevLogIndex = rf.lastSnapshotIndex
			args.PrevLogTerm = rf.lastSnapshotTerm
		} else {
			args.PrevLogIndex = rf.log[len(rf.log)-1].Index
			args.PrevLogTerm = rf.log[len(rf.log)-1].Term
		}
		args.Entries = []LogEntry{}
	} else if localNextIndex == 0 {
		args.PrevLogIndex = rf.lastSnapshotIndex
		args.PrevLogTerm = rf.lastSnapshotTerm
		args.Entries = rf.log[0:]
	} else {
		DPrintf("%d localprevindex %d, loglenght %d", rf.me, localPrevIndex, len(rf.log))
		args.PrevLogIndex = rf.log[localPrevIndex].Index
		args.PrevLogTerm = rf.log[localPrevIndex].Term
		args.Entries = rf.log[localPrevIndex+1:]
	}
	DPrintf("Prepare to append ArgEntries %d, prev %d", len(args.Entries), args.PrevLogIndex)
	nextIndex = args.PrevLogIndex + len(args.Entries) + 1
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	meta.total--
	if ok {
		// Log index not match.
		if !reply.Success {
			DPrintf("Fail peer %d term %d nextIndex %d", peer, reply.Term, reply.NextIndex)
			if reply.Term == rf.currentTerm {
				// Modifying share data.
				// Reply's nextindex is less than PrevLogIndex.
				rf.nextIndex[peer] = reply.NextIndex
			}

			if reply.Term > meta.highestTermSeen {
				meta.highestTermSeen = reply.Term
			}
		} else {
			rf.nextIndex[peer] = nextIndex
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			meta.count++
		}
		// DPrintf("Append from %d to %d, %t", rf.me, peer, reply.Success)
	}
	// Eagerly commit when more than half has replicated the logs
	if !meta.hasNotified && (meta.count > len(rf.peers)/2 || meta.total == 0) {
		select {
		case meta.followerChan <- true:
		case <-time.After(time.Duration(timeout) * time.Millisecond):
		}
		meta.hasNotified = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(peer int, meta *HeartBeatMeta) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	meta.total--
	if ok {
		if reply.Term > meta.highestTermSeen {
			meta.highestTermSeen = reply.Term
		} else if reply.Term == rf.currentTerm {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
	}
	rf.mu.Unlock()
}

type HeartBeatMeta struct {
	highestTermSeen int
	count           int
	total           int
	hasNotified     bool
	followerChan    chan bool
}

func (rf *Raft) heartBeat() {
	for rf.state == Leader {
		DPrintf("%d heartbeat", rf.me)
		heartbeatMeta := HeartBeatMeta{
			highestTermSeen: rf.currentTerm,
			count:           1,
			total:           len(rf.peers) - 1,
			hasNotified:     false,
			followerChan:    make(chan bool),
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				// DPrintf("Term %d: Append from %d to %d", rf.currentTerm, rf.me, peer)
				go func(peer int) {
					rf.mu.Lock()

					localNextIndex, found := rf.findLog(rf.nextIndex[peer])

					rf.mu.Unlock()
					// Two possibility for not found:
					// 1. has been snapshot, which means the snapshot index should be greater than the nextindex
					// 2. has not been received. should just append empty.

					if !found && rf.lastSnapshotIndex >= rf.nextIndex[peer] {
						// Install snapshot
						// DPrintf("peer %d index %d not found", peer, rf.nextIndex[peer])
						rf.sendInstallSnapshot(peer, &heartbeatMeta)
					} else {
						// Heartbeat.
						// localnextindex can be -1 here.
						// which means nothing to send.
						// DPrintf("peer %d index %d found %t", peer, rf.nextIndex[peer], found)
						rf.sendAppendEntries(peer, localNextIndex, &heartbeatMeta)
					}

				}(i)

			}
		}
		select {
		case <-heartbeatMeta.followerChan:
		case <-time.After(time.Duration(timeout) * time.Millisecond):
		}

		rf.mu.Lock()
		if heartbeatMeta.highestTermSeen > rf.currentTerm {
			rf.currentTerm = heartbeatMeta.highestTermSeen
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
		if len(rf.log) > 0 {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // last index + 1
		} else {
			rf.nextIndex[i] = rf.lastSnapshotIndex + 1
		}
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
		if rf.state == Follower {
			rf.mu.Lock()
			DPrintf("%d starts a new election", rf.me)
			// Start a new election.
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.currentTerm++
			// DPrintf("%d is now a candidate, term #%d", rf.me, rf.currentTerm)
			var args RequestVoteArgs

			args.Term = rf.currentTerm
			args.CandidateID = rf.me
			if len(rf.log) > 0 {
				args.LastLogIndex = rf.log[len(rf.log)-1].Index
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			} else {
				args.LastLogTerm = rf.lastSnapshotTerm
				args.LastLogIndex = rf.lastSnapshotIndex
			}
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

// Return the election random time out.
func getElectionTimeOutValue() time.Duration {
	return time.Duration(rand.Intn(randomBackoffRange)+randomBackoffBase) * time.Millisecond
}

// Require holding the lock.
func (rf *Raft) commitRoutine() {
	if len(rf.log) > 0 {
		rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	} else {
		rf.matchIndex[rf.me] = rf.lastSnapshotIndex
	}
	matchIndices := make([]int, len(rf.matchIndex))
	copy(matchIndices, rf.matchIndex)
	sort.Ints(matchIndices)

	// When odd, half will be the middle index
	// When even, half will be the index to the left of middle
	// For both cases, servers from half to the rightmost one will
	// at least commit matchIndices[half]
	half := len(matchIndices) / 2

	DPrintf("%d Matching Index %v", rf.me, rf.matchIndex)

	// commitIndex should increase monotonically.
	// Only commit logs of currentTerm
	localMatchedIndex, found := rf.findLog(matchIndices[half])

	if rf.commitIndex < matchIndices[half] && found && rf.log[localMatchedIndex].Term == rf.currentTerm {
		rf.commitIndex = matchIndices[half]
		DPrintf("%d term%d Index commit to %d", rf.me, rf.currentTerm, rf.commitIndex)
		// rf.clientCond.Broadcast()
	}
}

func (rf *Raft) applyLogToServer() {
	for rf.state != Dead {
		if rf.commitIndex > rf.lastApplied {
			if rf.lastSnapshotIndex > 0 && rf.lastApplied < rf.lastSnapshotIndex {
				// We should apply the snapshot.
				rf.mu.Lock()
				snapshot := rf.persister.ReadSnapshot()
				copiedSnapshot := make([]byte, len(snapshot))
				copy(copiedSnapshot, snapshot)
				DPrintf("%d snapshot for index %d of size %d", rf.me, rf.lastSnapshotIndex, len(snapshot))
				app := ApplyMsg{
					CommandValid: false,
					CommandIndex: rf.lastSnapshotIndex,
					CommandTerm:  rf.lastSnapshotTerm,
					Snapshot:     copiedSnapshot,
				}
				rf.lastApplied = rf.lastSnapshotIndex
				DPrintf("%d Applied snapshot snapshot index %d", rf.me, rf.lastSnapshotIndex)
				rf.mu.Unlock()
				rf.applyCh <- app
			} else {
				var length int
				rf.mu.Lock()
				localLastApplied, found := rf.findLog(rf.lastApplied)

				if !found {
					// LocalCommit should always be found.
					localCommit, _ := rf.findLog(rf.commitIndex)
					length = localCommit + 1
				} else {
					length = rf.commitIndex - rf.lastApplied
				}
				copyLog := rf.log[localLastApplied+1 : localLastApplied+length+1]
				rf.mu.Unlock()

				DPrintf("%d: Commit index %d last Applied %d local %d", rf.me, rf.commitIndex, rf.lastApplied, localLastApplied)
				for i := 0; i < length; i++ {
					DPrintf("%d log length %d, rf.log %d", rf.me, length, len(copyLog))
					DPrintf("%d apply to index %d", rf.me, copyLog[i].Index)
					app := ApplyMsg{
						CommandValid: true,
						Command:      copyLog[i].Command,
						CommandIndex: copyLog[i].Index,
						CommandTerm:  copyLog[i].Term,
					}
					rf.applyCh <- app
					rf.lastApplied += 1
				}
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
		peers:             peers,
		persister:         persister,
		me:                me,
		applyCh:           applyCh,
		state:             Follower,
		votedFor:          -1,
		electionTimer:     nil,
		currentTerm:       0,
		commitIndex:       0,
		lastApplied:       0,
		lastSnapshotTerm:  0,
		lastSnapshotIndex: 0,
	}
	// rf.clientCond = sync.NewCond(&rf.mu)
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Leader election.
	go rf.leaderElection()
	// Apply messages.
	go rf.applyLogToServer()
	return rf
}
