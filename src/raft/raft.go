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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

var ElectionLog bool = false
var ElectionWinLog bool = false
var AppendEntriesLog bool = false
var AppendChangeLog bool = false
var ApplyMsgLog bool = false
var SnapshotLog bool = false

// Raft节点状态
type PeerState int

const (
	LeaderState    PeerState = 0
	CandidateState PeerState = 1
	FollowerState  PeerState = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
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

type Entry struct {
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
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       PeerState
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Volatile snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int
	//
	applyCh chan ApplyMsg

	electionTimer int32
	heartbeatsCh  chan bool
	applyStartCh  chan bool
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LeaderState)

	return term, isleader
}

func (rf *Raft) EncodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.EncodeRaftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Entry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("[ERROR] read persist fail.")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = rf.log[0].Command.(int)
		rf.lastIncludedTerm = rf.log[0].Term
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

func (rf *Raft) trimLog(index int) {
	snapshotIndex, snapshotTerm := index, rf.log[index-rf.lastIncludedIndex].Term
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	log := make([]Entry, lastLogIndex-index+1)
	copy(log[1:], rf.log[snapshotIndex-rf.lastIncludedIndex+1:])
	log[0] = Entry{snapshotTerm, snapshotIndex}
	rf.lastIncludedIndex, rf.lastIncludedTerm = snapshotIndex, snapshotTerm
	rf.log = log
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}

	rf.trimLog(index)

	if SnapshotLog {
		fmt.Printf("[SNAPSHOT] peer %v-%v lastIncludeIndex %v commitIndex %v log %v\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.commitIndex, rf.log)
	}

	rf.persister.SaveStateAndSnapshot(rf.EncodeRaftState(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if SnapshotLog {
		fmt.Printf("[INSTALL SNAPSHOT] %v <- %v | log %v\n", rf.me, args.LeaderId, rf.log)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.resetElectionTimeout()
	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex >= rf.lastIncludedIndex+len(rf.log)-1 {
		// 抛弃当前所有log，正常安装
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.log = []Entry{{rf.lastIncludedTerm, rf.lastIncludedIndex}}
	} else if args.LastIncludedIndex > rf.lastIncludedIndex {
		// 抛弃当前在LastIncludedIndex之前的Log，正常安装
		lastIncludedIndex := args.LastIncludedIndex
		lastIncludedTerm := rf.log[lastIncludedIndex-rf.lastIncludedIndex].Term

		tmpLog := make([]Entry, len(rf.log)+rf.lastIncludedIndex-lastIncludedIndex)

		copy(tmpLog[1:], rf.log[lastIncludedIndex-rf.lastIncludedIndex+1:])

		tmpLog[0] = Entry{lastIncludedTerm, lastIncludedIndex}
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.log = tmpLog
	} else {
		rf.persist()
		return
	}

	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.raftstate, args.Snapshot)

	if rf.lastIncludedIndex > rf.commitIndex {
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
		if SnapshotLog {
			fmt.Printf("[SNAPSHOT] peer %v-%v lastIncludeIndex %v commitIndex %v log %v\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.commitIndex, rf.log)
			// fmt.Printf("me %v\n newLength %v\n lastIncludeIndex %v\n lastInludeTerm %v\n oldLog %v\n newLog %v\n", rf.me, 1+lastIncludedIndex-rf.lastIncludedIndex, lastIncludedIndex, lastIncludedTerm, rf.log, tmpLog)
			// fmt.Printf("me %v\n snapshot %v\n", rf.me, snapshot)
		}
		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Snapshot,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 请求term小于当前term 立即返回
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 请求term大于当前term 当前node转为follower
	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的LastLogTerm大于等于当前的lastLogTerm一样新才允许投票
		if lastOriginIndex := rf.lastIncludedIndex + len(rf.log) - 1; args.LastLogTerm > rf.log[lastOriginIndex-rf.lastIncludedIndex].Term ||
			args.LastLogTerm == rf.log[lastOriginIndex-rf.lastIncludedIndex].Term && args.LastLogIndex >= lastOriginIndex {
			rf.resetElectionTimeout()

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId

			if ElectionLog {
				fmt.Printf("[ELECTION VOTE ] peer(%v term %v) vote for candidate(%v term %v)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	needApply := false
	defer func() {
		if needApply {
			rf.applyStartCh <- true
		}
	}()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
		if AppendEntriesLog {
			fmt.Printf("[APPEND REPLY  ] peer(%v term %v) | commit %v | applied %v | log %v | reply %v\n",
				rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log, *reply)
		}
		if SnapshotLog {
			fmt.Printf("[SNAPSHOT APPEND END   ] %v(%v) | log %v\n", rf.me, rf.currentTerm, rf.log)
		}
	}()
	if AppendEntriesLog {
		fmt.Printf("\n[APPEND REQUEST] peer(%v term %v) | commit %v | applied %v | log %v | sender(%v term %v) | commit %v | entries %v\n",
			rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log, args.LeaderId, args.Term, args.LeaderCommit, args.Entries)
	}

	if SnapshotLog {
		fmt.Printf("[SNAPSHOT APPEND START ] %v(%v) | log %v\n", rf.me, rf.currentTerm, rf.log)
	}

	reply.Success = true

	// fail
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// success
	rf.resetElectionTimeout()

	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	// 当前log长度小于PrevLogIndex 或 对应index的term不同，则返回false
	if lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1; lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		// 二分查找conflictTerm的第一个index
		l, r := -1, len(rf.log)
		for l+1 < r {
			mid := (l + r) >> 1
			if rf.log[mid].Term < reply.ConflictTerm {
				l = mid
			} else {
				r = mid
			}
		}
		reply.ConflictIndex = r + rf.lastIncludedIndex
		reply.ConflictTerm = rf.log[reply.ConflictIndex-rf.lastIncludedIndex].Term
		return
	}

	// 判断当前server是否已包含请求append的entry, 如果已包含则找到term匹配的最后一个index, 删除之后所有entry
	matchLogIndex := args.PrevLogIndex + 1
	for lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1; matchLogIndex <= lastLogIndex && matchLogIndex-args.PrevLogIndex-1 < len(args.Entries); matchLogIndex++ {
		if rf.log[matchLogIndex-rf.lastIncludedIndex].Term != args.Entries[matchLogIndex-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:matchLogIndex-rf.lastIncludedIndex]
			break
		}
	}

	rf.log = append(rf.log, args.Entries[matchLogIndex-args.PrevLogIndex-1:]...)
	rf.persist()

	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	newCommitIndex := min(lastNewIndex, args.LeaderCommit)
	if newCommitIndex > rf.commitIndex && rf.log[newCommitIndex-rf.lastIncludedIndex].Term == rf.currentTerm {
		rf.commitIndex = newCommitIndex
	}

	// [All servers] rule 1
	if rf.lastApplied < rf.commitIndex {
		needApply = true
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) doAppendEntries(peer, term, leaderId, prevLogIndex, prevLogTerm, leaderCommit int, entries []Entry) {
	// Leaders rule 3
	rf.mu.Lock()
	prevLogIndex = rf.nextIndex[peer] - 1
	prevLogTerm = rf.log[prevLogIndex-rf.lastIncludedIndex].Term
	entries = make([]Entry, len(rf.log)-(1+prevLogIndex-rf.lastIncludedIndex))
	copy(entries, rf.log[1+prevLogIndex-rf.lastIncludedIndex:])

	// if rf.lastIncludedIndex >= rf.nextIndex[peer] {
	// 	args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.persister.snapshot}
	// 	reply := &InstallSnapshotReply{}
	// 	rf.mu.Unlock()
	// 	go func(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 		ok := rf.sendInstallSnapshot(peer, args, reply)
	// 		if ok {
	// 			rf.mu.Lock()
	// 			defer rf.mu.Unlock()
	// 			if reply.Term > rf.currentTerm {
	// 				defer rf.persist()
	// 				rf.state = FollowerState
	// 				rf.votedFor = -1
	// 				rf.currentTerm = reply.Term
	// 			} else {
	// 				rf.matchIndex[peer] = rf.lastIncludedIndex
	// 				rf.nextIndex[peer] = rf.lastIncludedIndex + 1
	// 			}
	// 		}
	// 	}(peer, args, reply)
	// 	return
	// }

	// if prevLogIndex >= rf.nextIndex[peer] {
	// 	if SnapshotLog {
	// 		fmt.Printf("[SNAPSHOT APPEND CHANGE] %v(%v) | lastIncludeIndex %v prevLogIndex %v nextIndex %v commitIndex %v\n",
	// 			peer, rf.currentTerm, rf.lastIncludedIndex, prevLogIndex, rf.nextIndex[peer], rf.commitIndex)
	// 	}
	// 	new_entries := make([]Entry, prevLogIndex-rf.nextIndex[peer]+1)
	// 	copy(new_entries, rf.log[rf.nextIndex[peer]-rf.lastIncludedIndex:prevLogIndex-rf.lastIncludedIndex+1])
	// 	new_entries = append(new_entries, entries...)
	// 	if AppendChangeLog {
	// 		fmt.Printf("[APPEND CHANGE] leader(%v term %v) append to peer(%v) : nextIndex %v | prevLogIndex+1 %v | entries %v -> %v\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer], prevLogIndex+1, entries, new_entries)
	// 	}
	// 	entries = new_entries
	// 	prevLogIndex = rf.nextIndex[peer] - 1

	// 	prevLogTerm = rf.log[prevLogIndex-rf.lastIncludedIndex].Term
	// }

	if SnapshotLog {
		fmt.Printf("[SNAPSHOT APPEND MASTER] %v(%v) | log %v\n", rf.me, rf.currentTerm, rf.log)
	}
	rf.mu.Unlock()

	args := &AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(peer, args, reply)

	if ok {
		needApply := false
		defer func() {
			if needApply {
				rf.applyStartCh <- true
			}
		}()

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if args.Term != rf.currentTerm || rf.state != LeaderState {
			return
		}

		defer rf.persist()

		if reply.Term > rf.currentTerm {
			rf.state = FollowerState
			rf.votedFor = -1
			rf.currentTerm = reply.Term
		} else if reply.Success {
			rf.matchIndex[peer] = prevLogIndex + len(entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			matchIndex := rf.matchIndex[rf.me]

			for ; matchIndex > rf.commitIndex && rf.log[matchIndex-rf.lastIncludedIndex].Term == rf.currentTerm; matchIndex-- {
				cnt := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= matchIndex {
						cnt++
					}
				}
				if cnt > len(rf.peers)>>1 {
					break
				}
			}

			if matchIndex > rf.commitIndex && rf.log[matchIndex-rf.lastIncludedIndex].Term == rf.currentTerm {
				rf.commitIndex = matchIndex
			}

			if rf.commitIndex > rf.lastApplied {
				needApply = true
			}
		} else {
			// 二分查找conflictTerm的的一个index
			l, r := -1, len(rf.log)
			for l+1 < r {
				mid := (l + r) >> 1
				if rf.log[mid].Term <= reply.ConflictTerm {
					l = mid
				} else {
					r = mid
				}
			}

			// conflictTerm 存在
			if l > 0 && l < len(rf.log) && r > 0 && r < len(rf.log) && rf.log[l].Term == reply.ConflictTerm {
				rf.nextIndex[peer] = r + rf.lastIncludedIndex
			} else {
				rf.nextIndex[peer] = reply.ConflictIndex
			}
		}
	}
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index, term, isLeader = -1, -1, false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LeaderState {
		prevLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
		prevLogTerm := rf.log[prevLogIndex-rf.lastIncludedIndex].Term
		currentTerm := rf.currentTerm
		commitIndex := rf.commitIndex

		rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
		rf.persist()

		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++

		if AppendEntriesLog {
			fmt.Printf("\n[APPEND LEADER ] leader(%v term %v) | commit %v | applied %v | log %v\n",
				rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log)
		}

		// index, term, isLeader = rf.lastIncludedIndex+len(rf.log)-1, rf.currentTerm, true
		index, term, isLeader = rf.lastIncludedIndex+len(rf.log)-1, rf.currentTerm, true

		go func() {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.doAppendEntries(i, currentTerm, rf.me, prevLogIndex, prevLogTerm, commitIndex, []Entry{{currentTerm, command}})
				}
			}
		}()
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

func (rf *Raft) setElectionTimeout() {
	rf.electionTimer = 1
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer = 0
}

func (rf *Raft) isElectionTimeout() bool {
	z := rf.electionTimer
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// random sleep
		rand.Seed(time.Now().Unix() + int64(rf.me))
		electionTimeout := 450 + rand.Intn(150)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		// timeout judgement
		rf.mu.Lock()
		if rf.isElectionTimeout() {
			// reset time out state
			rf.resetElectionTimeout()
			// change to candidate
			rf.state = CandidateState
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()

			if ElectionLog {
				fmt.Printf("\n[ELECTION START] peer(%v term %v) timeout in %vms, election starting...\n", rf.me, rf.currentTerm, electionTimeout)
			}

			go rf.startElection(rf.currentTerm, rf.lastIncludedIndex+len(rf.log)-1, rf.log[len(rf.log)-1].Term)

		} else if rf.state != LeaderState {
			rf.setElectionTimeout()
		}
		rf.mu.Unlock()
	}
}

// 选举协程
func (rf *Raft) startElection(term, lastLogIndex, lastLogTerm int) {
	voteCount := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}
			reply := &RequestVoteReply{}

			if ok := rf.sendRequestVote(peer, args, reply); ok {
				winElection := false
				rf.mu.Lock()
				defer func() {
					if winElection {
						rf.heartbeatsCh <- true
					}
				}()
				defer rf.mu.Unlock()
				defer rf.persist()

				if args.Term != rf.currentTerm || rf.state != CandidateState {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.state = FollowerState
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					return
				}

				if reply.VoteGranted {
					voteCount++
					if rf.currentTerm > term || rf.state != CandidateState {
						return
					}
					if voteCount > len(rf.peers)>>1 {
						rf.state = LeaderState

						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
							rf.matchIndex[i] = 0
						}

						rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.lastIncludedIndex

						winElection = true

						if ElectionWinLog {
							fmt.Printf("[ELECTION WIN  ] peer(%v term %v) got %v/%v ticket, win the election\n", rf.me, rf.currentTerm, voteCount, len(rf.peers))
						}
					}
				}
			}
		}(i)
	}
}

// 心跳协程
func (rf *Raft) pacemaker() {
	for start := <-rf.heartbeatsCh; start; start = <-rf.heartbeatsCh {
		for !rf.killed() {
			rf.mu.Lock()
			if rf.state != LeaderState {
				rf.mu.Unlock()
				break
			}

			prevLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
			prevLogTerm := rf.log[prevLogIndex-rf.lastIncludedIndex].Term

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.doAppendEntries(i, rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex, []Entry{})
				}
			}
			rf.mu.Unlock()

			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

func (rf *Raft) applier() {
	for start := <-rf.applyStartCh; start; start = <-rf.applyStartCh {
		rf.mu.Lock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		currentTerm := rf.currentTerm
		var newApplyEntries []Entry
		if lastApplied < commitIndex {
			newApplyEntries = make([]Entry, commitIndex-lastApplied)
			copy(newApplyEntries, rf.log[lastApplied+1-rf.lastIncludedIndex:commitIndex+1-rf.lastIncludedIndex])
			if ApplyMsgLog {
				if rf.state == LeaderState {
					fmt.Printf("[APPLY] leader %v(%3v) log(%v)\n", rf.me, currentTerm, rf.log)
				} else {
					fmt.Printf("[APPLY] peeeer %v(%3v) log(%v)\n", rf.me, currentTerm, rf.log)
				}

			}
		}
		rf.mu.Unlock()

		lastApplied++
		for appliedIndex := lastApplied; appliedIndex <= commitIndex; appliedIndex++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: newApplyEntries[appliedIndex-lastApplied].Command, CommandIndex: appliedIndex}
		}

		rf.mu.Lock()
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
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
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FollowerState
	rf.votedFor = -1
	// log index start from 1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{0, 0})
	// 心跳开始信号阻塞队列，
	rf.setElectionTimeout()
	rf.heartbeatsCh = make(chan bool)
	rf.applyStartCh = make(chan bool)
	rf.applyCh = applyCh
	// 在election之后初始化
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.pacemaker()
	go rf.applier()

	return rf
}
