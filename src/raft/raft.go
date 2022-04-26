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
	"sort"
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
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LeaderState)
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
	// Example:
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
	// args.Term < rf.currentTerm 直接返回
	if args.Term < rf.currentTerm {
		return
	}
	// args.Term > rf.currentTerm 转为follower
	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
		// rf.persist()
	}

	// 只有当前peer没有投票或已投票给candidate时才可能返回true
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的log至少和当前log一样新, 允许投票
		if lastLogIndex := len(rf.log) - 1; args.LastLogTerm > rf.log[lastLogIndex].Term ||
			args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex {
			if ElectionLog {
				fmt.Printf("[ELECTION VOTE ] peer(%v term %v) vote for candidate(%v term %v)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
			}
			rf.resetTimeout()

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			// rf.persist()
		}
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int // so follower can redirect clients ？ 如何实现

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
		// fmt.Printf("\n[APPEND REQUEST] peer(%v term %v) | sender(%v term %v) | prevLogIndex %v | prevLogTerm %v | leaderCommit %v | entries %v\n",
		// 	rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
		fmt.Printf("\n[APPEND REQUEST] peer(%v term %v) | commit %v | applied %v | log %v | sender(%v term %v) | commit %v | entries %v\n",
			rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log, args.LeaderId, args.Term, args.LeaderCommit, args.Entries)
	}

	reply.Term = rf.currentTerm
	// args.Term < rf.currentTerm 直接返回-不应该是term的，而是leader的
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 重置超时计时器
	rf.resetTimeout()
	reply.Success = true
	// args.Term > rf.currentTerm 转为follower
	if args.Term > rf.currentTerm {
		rf.state = FollowerState
		rf.votedFor = -1
		rf.currentTerm = args.Term
		// rf.persist()
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
			// if ApplyMsgLog {
			// 	fmt.Printf("%v(任期%v) 删除不匹配log %v %v matchLogIndex%v\n", rf.me, rf.currentTerm, rf.log, rf.log[:matchLogIndex], matchLogIndex)
			// }
			rf.log = rf.log[:matchLogIndex]
			// rf.persist()
			break
		}
	}

	// 将所有不存在于当前peer的log中的entry加入
	rf.log = append(rf.log, args.Entries[matchLogIndex-args.PrevLogIndex-1:]...)
	// rf.persist()
	// if AppendEntriesLog {
	// 	fmt.Printf("[APPEND NEW LOG] peer(%v term %v) : new entries %v | matchLogIndex %v | prevLogIndex %v\n",
	// 		rf.me, rf.currentTerm, len(args.Entries[matchLogIndex-args.PrevLogIndex-1:]), matchLogIndex, args.PrevLogIndex)
	// }
	// 使commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// 添加到fl.log中的最后一个index
		// oldCommitIndex := rf.commitIndex
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			// if ApplyMsgLog {
			// 	fmt.Printf("%v(任期%v) 更新 commitIndex %v -> LeaderCommit %v, \n", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
			// }
			rf.commitIndex = args.LeaderCommit
		} else {
			// if ApplyMsgLog {
			// 	fmt.Printf("%v(任期%v) 更新 commitIndex %v -> lastNewIndex %v, \n", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)
			// }
			rf.commitIndex = lastNewIndex
		}
		// if AppendEntriesLog {
		// 	fmt.Printf("[APPEND COMMIT UPDATE] peer(%v term %v) commitIndex %v -> %v\n", rf.me, rf.currentTerm, oldCommitIndex, rf.commitIndex)
		// }
	}

	// [All servers] rule 1 是否应该起一个goroutine单独执行本操作?
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		if ApplyMsgLog {
			fmt.Printf("[APPLY] peer(%v term %v) apply entries(%v)\n", rf.me, rf.currentTerm, rf.log[rf.lastApplied])
		}
		if ApplyMsgLiteLog {
			fmt.Printf("[APPLY] peer(%v term %v) apply entries(%v)\n", rf.me, rf.currentTerm, rf.log[rf.lastApplied].Term)
		}
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
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
			// rf.persist()
		} else if reply.VoteGranted {
			// 使用原子操作实现自增
			atomic.AddInt32(voteCount, 1)
		}
	}
}

func (rf *Raft) doAppendEntries(replyCount *int32, successCount *int32, server, term, leaderId, prevLogIndex, prevLogTerm, leaderCommit int, entries []Entry) {
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
	atomic.AddInt32(replyCount, 1)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		defer rf.persist()

		if reply.Term > rf.currentTerm {
			rf.state = FollowerState
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			// rf.persist()
		} else if reply.Success {
			// 更新server的index
			atomic.AddInt32(successCount, 1)
			rf.nextIndex[server] = prevLogIndex + len(entries) + 1
			rf.matchIndex[server] = prevLogIndex + len(entries)
		} else {
			// 如果失败，则尝试向前同步。indefinite retry通过heartbeats实现
			// if rf.nextIndex[server] > 1 {
			// 	rf.nextIndex[server]--
			// }

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

			foundTerm := false
			if r > 0 && r < len(rf.log) {
				foundTerm = true
			}

			if foundTerm {
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	replyCount := int32(1)
	successCount := int32(1)
	// majorCount := int32(1 + len(rf.peers)>>1)
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state == LeaderState {
		isLeader = true
		prevLogIndex := len(rf.log) - 1

		rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
		// rf.persist()

		if AppendEntriesLog {
			fmt.Printf("[APPEND LEADER ] leader(%v term %v) | commit %v | applied %v | log %v\n",
				rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log)
		}

		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		index = len(rf.log) - 1
		term = rf.currentTerm
		// 并行发送
		go rf.broadcast(&replyCount, &successCount, prevLogIndex, []Entry{{rf.currentTerm, command}})
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

// 选举超时检测例程
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 选举超时 timer
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
			electionTerm := rf.currentTerm

			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term

			if ElectionLog {
				fmt.Printf("\n[ELECTION START] peer(%v term %v) timeout in %vms, election starting...\n", rf.me, electionTerm, electionTimeout)
			}

			go rf.raiseVote(electionTerm, lastLogIndex, lastLogTerm)
		} else if rf.state != LeaderState {
			// leader永不超时
			rf.setTimeout()
		}

		rf.persist()
		rf.mu.Unlock()
	}
}

// 在当前任期执行选举
func (rf *Raft) raiseVote(term, lastLogIndex, lastLogTerm int) {
	// 向除了自身之外的server发送投票请求
	voteCount := int32(1)
	majority := int32(1 + len(rf.peers)>>1)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.doRequestVote(&voteCount, i, term, rf.me, lastLogIndex, lastLogTerm)
		}
	}

	// For 无间隔循环
	for endElection := false; !endElection; {
		rf.mu.Lock()
		if rf.currentTerm > term {
			endElection = true
		} else if voteCount >= majority {
			// candidate 转为 leader
			endElection = true
			rf.state = LeaderState
			if ElectionLog {
				fmt.Printf("[ELECTION WIN  ] peer(%v term %v) win the election\n", rf.me, rf.currentTerm)
			}
			// 选举成功后初始化
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}

			rf.matchIndex[rf.me] = len(rf.log) - 1

			rf.heartbeatsCh <- true
			rf.applyStartCh <- true
		}
		rf.mu.Unlock()
	}

	if ElectionLog {
		fmt.Printf("[ELECTION COUNT] peer(%v term %v) got %v/%v ticket\n\n", rf.me, rf.currentTerm, atomic.LoadInt32(&voteCount), len(rf.peers))
	}
}

func (rf *Raft) broadcast(replyCount *int32, successCount *int32, prevLogIndex int, entries []Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.doAppendEntries(replyCount, successCount, i, rf.currentTerm, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, rf.commitIndex, entries)
		}
	}
}

// 心跳同步例程，只有当成为leader时启动一次
func (rf *Raft) pacemaker() {
	for start := <-rf.heartbeatsCh; start; start = <-rf.heartbeatsCh {
		for endHeartbeats := false; !rf.killed(); {
			time.Sleep(time.Duration(100) * time.Millisecond)

			beatsCount := int32(1)
			replyCount := int32(1)
			rf.mu.Lock()
			if endHeartbeats = (rf.state != LeaderState); endHeartbeats {
				rf.mu.Unlock()
				break
			}

			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go rf.doAppendEntries(&replyCount, &beatsCount, i, rf.currentTerm, rf.me, lastLogIndex, lastLogTerm, rf.commitIndex, []Entry{})
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applier() {
	for start := <-rf.applyStartCh; start; start = <-rf.applyStartCh {
		for endHeartbeats := false; !rf.killed(); {
			time.Sleep(time.Duration(50) * time.Millisecond)

			rf.mu.Lock()
			if endHeartbeats = (rf.state != LeaderState); endHeartbeats {
				rf.mu.Unlock()
				break
			}

			// 判断matchIndex最新？
			// 为了不影响heartbeats下次发送？
			// 找到一个index，使得有至少一半matchIndex大于等于index，同时使index最大
			// 排序+二分查找
			majorCount := 1 + len(rf.peers)>>1
			tmpMatchIndex := make([]int, 0)
			tmpMatchIndex = append(tmpMatchIndex, rf.matchIndex...)
			sort.Ints(tmpMatchIndex)
			maxMatchIndex := tmpMatchIndex[len(rf.matchIndex)-majorCount]
			//
			if ApplyCheckLog {
				fmt.Printf("[APPLY CHECK] leader(%v term %v) | nextIndex %v | matchIndex %v | maxMatch %v | commitIndex %v\n",
					rf.me, rf.currentTerm, rf.nextIndex, rf.matchIndex, maxMatchIndex, rf.commitIndex)
			}
			//
			if rf.commitIndex < maxMatchIndex {
				rf.commitIndex = maxMatchIndex
			}
			// if ApplyMsgLog {
			// 	fmt.Printf("[APPLY CHECK] leader(%v term %v) : lastApplied %v | commitIndex %v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			// }
			//
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
			// 执行持久化
			rf.mu.Unlock()
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
