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
var AppendEntriesLog bool = false
var AppendChangeLog bool = false
var ApplyMsgLog bool = false
var ApplyCheckLog bool = false
var ApplyMsgLiteLog bool = false
var TimeoutLog bool = false
var StartLog bool = true

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

// Raft节点状态
type PeerState int

const (
	LeaderState    PeerState = 0
	CandidateState PeerState = 1
	FollowerState  PeerState = 2
)

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

	// Volatile state on leaders (选举后重置)
	nextIndex  []int
	matchIndex []int

	//
	applyCh chan ApplyMsg

	electionTimer int32
	heartbeatsCh  chan bool
	applyStartCh  chan bool
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 请求term小于当前term 立即返回
	if args.Term < rf.currentTerm {
		return
	}
	// 请求term大于当前term 当前node转为follower
	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的LastLogTerm大于等于当前的lastLogTerm一样新才允许投票
		if lastLogIndex := len(rf.log) - 1; args.LastLogTerm > rf.log[lastLogIndex].Term ||
			args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex {
			rf.resetTimeout()

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	defer func() {
		if AppendEntriesLog {
			fmt.Printf("[APPEND REPLY  ] peer(%v term %v) | commit %v | applied %v | log %v | reply %v\n",
				rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log, *reply)
		}
	}()

	if AppendEntriesLog {
		fmt.Printf("\n[APPEND REQUEST] peer(%v term %v) | commit %v | applied %v | log %v | sender(%v term %v) | commit %v | entries %v\n",
			rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log, args.LeaderId, args.Term, args.LeaderCommit, args.Entries)
	}

	reply.Term = rf.currentTerm

	// 请求term小于当前term 立即返回
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 重置超时计时器
	rf.resetTimeout()
	reply.Success = true
	// 请求term大于当前term 当前node转为follower
	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	// 当前log长度小于PrevLogIndex或对应index的term不同，则返回false
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// 二分查找conflictTerm的的一个index
		l, r := -1, len(rf.log)
		for l+1 < r {
			mid := (l + r) >> 1
			if rf.log[mid].Term < reply.ConflictTerm {
				l = mid
			} else {
				r = mid
			}
		}
		reply.ConflictIndex = r
		return
	}

	// 判断当前server是否已包含请求append的entry, 如果已包含则找到term匹配的最后一个index, 删除之后所有entry
	matchLogIndex := args.PrevLogIndex + 1
	for ; matchLogIndex <= lastLogIndex && matchLogIndex-args.PrevLogIndex-1 < len(args.Entries); matchLogIndex++ {
		if rf.log[matchLogIndex].Term != args.Entries[matchLogIndex-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:matchLogIndex]
			break
		}
	}

	rf.log = append(rf.log, args.Entries[matchLogIndex-args.PrevLogIndex-1:]...)

	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
	}

	// [All servers] rule 1
	if rf.lastApplied < rf.commitIndex {
		rf.applyStartCh <- true
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

func (rf *Raft) doRequestVote(voteCount *int32, server int, term, candidateId, lastLogIndex, lastLogTerm int) {
	args := &RequestVoteArgs{term, candidateId, lastLogIndex, lastLogTerm}
	reply := &RequestVoteReply{}

	if ok := rf.sendRequestVote(server, args, reply); ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		if reply.Term > rf.currentTerm {
			rf.state = FollowerState
			rf.votedFor = -1
			rf.currentTerm = reply.Term
		} else if reply.VoteGranted {
			atomic.AddInt32(voteCount, 1)

			if rf.currentTerm > term || rf.state != CandidateState {
				return
			} else if majorCount := int32(1 + len(rf.peers)>>1); atomic.LoadInt32(voteCount) >= majorCount {
				rf.state = LeaderState

				// 选举成功后初始化
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}

				rf.matchIndex[rf.me] = len(rf.log) - 1

				rf.heartbeatsCh <- true
				rf.applyStartCh <- true

				if ElectionLog {
					fmt.Printf("[ELECTION WIN  ] peer(%v term %v) got %v/%v ticket, win the election\n", rf.me, rf.currentTerm, atomic.LoadInt32(voteCount), len(rf.peers))
				}
			}
		}
	}
}

func (rf *Raft) doAppendEntries(server, term, leaderId, prevLogIndex, prevLogTerm, leaderCommit int, entries []Entry) {
	// Leaders rule 3
	rf.mu.Lock()
	if prevLogIndex >= rf.nextIndex[server] {
		new_entries := make([]Entry, prevLogIndex+1-rf.nextIndex[server])
		copy(new_entries, rf.log[rf.nextIndex[server]:prevLogIndex+1])
		// new_entries := rf.log[rf.nextIndex[server] : prevLogIndex+1]
		new_entries = append(new_entries, entries...)
		if AppendChangeLog {
			fmt.Printf("[APPEND CHANGE] leader(%v term %v) append to peer(%v) : nextIndex %v | prevLogIndex+1 %v | entries %v -> %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server], prevLogIndex+1, entries, new_entries)
		}
		entries = new_entries
		prevLogIndex = rf.nextIndex[server] - 1
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	rf.mu.Unlock()

	args := &AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		if reply.Term > rf.currentTerm {
			rf.state = FollowerState
			rf.votedFor = -1
			rf.currentTerm = reply.Term
		} else if reply.Success {
			rf.nextIndex[server] = prevLogIndex + len(entries) + 1
			rf.matchIndex[server] = prevLogIndex + len(entries)

			majorCount := 1 + len(rf.peers)>>1
			tmpMatchIndex := rf.matchIndex[rf.me]

			for ; tmpMatchIndex > rf.commitIndex; tmpMatchIndex-- {
				tmpCount := 0

				for i := 0; i < len(rf.matchIndex); i++ {
					if rf.matchIndex[i] >= tmpMatchIndex {
						tmpCount++
					}
				}

				if tmpCount >= majorCount && rf.log[tmpMatchIndex].Term == rf.currentTerm {
					if rf.commitIndex = tmpMatchIndex; rf.commitIndex > rf.lastApplied {
						rf.applyStartCh <- true
					}
					break
				} else if rf.log[tmpMatchIndex].Term < rf.currentTerm {
					break
				}
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
			if r > 0 && r < len(rf.log) {
				rf.nextIndex[server] = r
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
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
		prevLogIndex := len(rf.log) - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
		rf.persist()

		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++

		if AppendEntriesLog {
			fmt.Printf("\n[APPEND LEADER ] leader(%v term %v) | commit %v | applied %v | log %v\n",
				rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log)
		}

		index, term, isLeader = len(rf.log)-1, rf.currentTerm, true

		go rf.broadcast(rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex, []Entry{{rf.currentTerm, command}})
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

func (rf *Raft) setTimeout() {
	atomic.StoreInt32(&rf.electionTimer, 1)
}

func (rf *Raft) resetTimeout() {
	atomic.StoreInt32(&rf.electionTimer, 0)
}

func (rf *Raft) isTimeout() bool {
	z := atomic.LoadInt32(&rf.electionTimer)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 选举超时
		rand.Seed(time.Now().Unix() + int64(rf.me))
		electionTimeout := 300 + rand.Intn(150)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		if TimeoutLog {
			fmt.Printf("[TIMEOUT ELECTION] peer(%v term %v) will timeout in %vms\n", rf.me, rf.currentTerm, electionTimeout)
		}

		// 选举超时判断
		rf.mu.Lock()
		if rf.isTimeout() {
			rf.resetTimeout()

			rf.state = CandidateState
			rf.votedFor = rf.me
			rf.currentTerm++
			rf.persist()

			if ElectionLog {
				fmt.Printf("\n[ELECTION START] peer(%v term %v) timeout in %vms, election starting...\n", rf.me, rf.currentTerm, electionTimeout)
			}

			go rf.voter(rf.currentTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		} else if rf.state != LeaderState {
			rf.setTimeout()
		}
		rf.mu.Unlock()
	}
}

// 在当前任期执行选举
func (rf *Raft) voter(term, lastLogIndex, lastLogTerm int) {
	var voteCount int32 = 1 // 协程退出时失效？

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.doRequestVote(&voteCount, i, term, rf.me, lastLogIndex, lastLogTerm)
		}
	}
}

func (rf *Raft) broadcast(currentTerm int, leaderId int, prevLogIndex int, prevLogTerm int, leaderCommit int, entries []Entry) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.doAppendEntries(i, currentTerm, leaderId, prevLogIndex, prevLogTerm, leaderCommit, entries)
		}
	}
}

// 心跳同步例程，只有当成为leader时启动一次
func (rf *Raft) pacemaker() {
	for start := <-rf.heartbeatsCh; start; start = <-rf.heartbeatsCh {
		for !rf.killed() {
			rf.mu.Lock()

			if rf.state != LeaderState {
				rf.mu.Unlock()
				break
			}

			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			go rf.broadcast(rf.currentTerm, rf.me, lastLogIndex, lastLogTerm, rf.commitIndex, []Entry{})

			rf.mu.Unlock()

			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

func (rf *Raft) applier() {
	for start := <-rf.applyStartCh; start; start = <-rf.applyStartCh {
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if ApplyMsgLog {
				fmt.Printf("[APPLY] leader(%v term %v) apply entries(%v)\n", rf.me, rf.currentTerm, rf.log[rf.lastApplied])
			}
			if ApplyMsgLiteLog {
				fmt.Printf("[APPLY] leader(%v term %v) apply entries(%v)\n", rf.me, rf.currentTerm, rf.log[rf.lastApplied].Term)
			}
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
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
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FollowerState
	rf.votedFor = -1
	// log index start from 1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{})
	// 心跳开始信号阻塞队列，
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
