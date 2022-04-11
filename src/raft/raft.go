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

var RequestVoteLog bool = true
var AppendEntriesLog bool = false
var LeaderChangeLog bool = true
var TimeoutLog bool = true

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
type ServerState int

const (
	Leader    ServerState = 0
	Candidate ServerState = 1
	Follower  ServerState = 2
)

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

	isLeader      bool
	leaderTimeout int32
	// electionCh    chan bool
	heartbeatsCh chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int = rf.currentTerm
	var isleader bool = rf.isLeader
	// Your code here (2A).

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
	CandidateTerm int
	CandidateId   int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoterTerm   int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoterTerm = rf.currentTerm

	if args.CandidateTerm < rf.currentTerm {
		if RequestVoteLog {
			fmt.Printf("%v(任期%v)拒绝为 %v(任期%v)投票: 任期过期\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
		}
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if RequestVoteLog {
			fmt.Printf("%v(任期%v)同意为 %v(任期%v)投票\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		return
	}

	if RequestVoteLog {
		fmt.Printf("%v(任期%v)拒绝为 %v(任期%v)投票: 已投票\n", rf.me, rf.currentTerm, args.CandidateId, args.CandidateTerm)
	}
	reply.VoteGranted = false
}

type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId   int
}

type AppendEntriesReply struct {
	FollowerTerm int
	Success      bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.FollowerTerm = rf.currentTerm

	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}

	// Rule 2

	// Rule 3

	// Rule 4

	// Rule 5

	rf.unsetTimeout()
	rf.votedFor = -1
	rf.isLeader = false

	reply.Success = true
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
	atomic.StoreInt32(&rf.leaderTimeout, 1)
}

func (rf *Raft) unsetTimeout() {
	atomic.StoreInt32(&rf.leaderTimeout, 0)
}

func (rf *Raft) isTimeout() bool {
	z := atomic.LoadInt32(&rf.leaderTimeout)
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

		// 随机选举超时时间
		rand.Seed(time.Now().Unix())
		electionTimeout := 450 + rand.Intn(150)
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

		rf.mu.Lock()
		if rf.isTimeout() {
			if TimeoutLog {
				fmt.Printf("%v 超时\n", rf.me)
			}
			// 重置选举超时
			rf.unsetTimeout()
			go rf.doRequestVote()
		} else {
			// leader永不超时，除非reply的term比当前大
			if !rf.isLeader {
				rf.setTimeout()
			}
		}
		rf.mu.Unlock()
	}
}

// 在当前任期执行选举
func (rf *Raft) doRequestVote() {

	// 修改当前状态，做好选举准备
	rf.mu.Lock()
	rf.isLeader = false
	rf.votedFor = rf.me

	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.mu.Unlock()

	if LeaderChangeLog {
		fmt.Printf("%v(任期%v)开始拉票\n", rf.me, electionTerm)
	}

	// 向除了自身之外的server发送投票请求
	// voteArgs := make([]*RequestVoteArgs, len(rf.peers))
	// voteReply := make([]*RequestVoteReply, len(rf.peers))

	voteCount := 1
	majority := 1 + len(rf.peers)>>1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// voteArgs[i] = &RequestVoteArgs{CandidateTerm: electionTerm, CandidateId: rf.me}
			// voteReply[i] = &RequestVoteReply{VoteGranted: false}
			// go rf.sendRequestVote(i, voteArgs[i], voteReply[i])
			go func(id int) {
				args := &RequestVoteArgs{CandidateTerm: electionTerm, CandidateId: rf.me}
				reply := &RequestVoteReply{VoteGranted: false}
				if ok := rf.sendRequestVote(id, args, reply); ok {
					rf.mu.Lock()
					if reply.VoterTerm > rf.currentTerm {
						// candidate 转入 follower
						rf.currentTerm = reply.VoterTerm
					} else {
						if reply.VoteGranted {
							voteCount++
						}
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}

	for endElection := false; !endElection; {
		rf.mu.Lock()
		if rf.currentTerm > electionTerm {
			// 当前选举无效
			// candidate 转为 follower
			// 要么是选举超时
			// 要么是其他server term比当前大
			// 或者是其他server当选
			rf.votedFor = -1
			endElection = true
		} else {
			// 当前选举有效
			// candidate 转为 leader
			if voteCount >= majority {
				rf.votedFor = -1
				rf.isLeader = true
				rf.heartbeatsCh <- true
				endElection = true
			}
		}
		rf.mu.Unlock()
	}

	if LeaderChangeLog {
		fmt.Printf("在任期 %v 内 %v 获得 %v/%v 票\n", electionTerm, rf.me, voteCount, len(rf.peers))
	}

	// // 统计投票请求结果，使用channel？
	// voteCount := 1

	// // 使用rf.currentTerm代替rf.isTimeout?
	// for voteCount < majority && !rf.isTimeout() {
	// 	voteCount = 1
	// 	for i := 0; i < len(rf.peers); i++ {
	// 		if i != rf.me && voteReply[i].VoteGranted {
	// 			voteCount++
	// 		}
	// 	}
	// }
	// 判断退出情况

	// 判断是否当选，这里判断超时应该使用Term判断吧
	// if !rf.isTimeout() && voteCount >= majority {

	// }
}

// 心跳同步例程，只有当成为leader时启动一次
// 需要和选举例程一样拆分
func (rf *Raft) doHeartBeats() {
	if start := <-rf.heartbeatsCh; start {
		if LeaderChangeLog {
			fmt.Printf("%v 当选为总统\n", rf.me)
		}

		for !rf.killed() {
			// 心跳间隔
			time.Sleep(time.Duration(100) * time.Millisecond)

			heartbeatsCount := 0
			majority := 1 + len(rf.peers)>>1

			rf.mu.Lock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(id int) {
						args := &AppendEntriesArgs{LeaderTerm: rf.currentTerm, LeaderId: rf.me}
						reply := &AppendEntriesReply{Success: false}
						if ok := rf.sendAppendEntries(id, args, reply); ok {
							rf.mu.Lock()
							if reply.FollowerTerm > rf.currentTerm {
								// leader 转为 follower

							}
							rf.mu.Unlock()
						}
					}(i)
				}
			}
			rf.mu.Unlock()

			// heartbeatsArgs := make([]*AppendEntriesArgs, len(rf.peers))
			// heartbeatsReply := make([]*AppendEntriesReply, len(rf.peers))
			// // 获取当前任期
			// rf.mu.Lock()
			// leaderTerm := rf.currentTerm
			// rf.mu.Unlock()
			// // 多线程发送心跳同步
			// for i := 0; i < len(rf.peers); i++ {
			// 	if i == rf.me {
			// 		// 这里不应该不给自己发送心跳，leader如何确认自己未超时？
			// 		continue
			// 	}
			// 	heartbeatsArgs[i] = &AppendEntriesArgs{LeaderTerm: leaderTerm, LeaderId: rf.me}
			// 	heartbeatsReply[i] = &AppendEntriesReply{Success: false}
			// 	if AppendEntriesLog {
			// 		fmt.Printf("%v 发送心跳给 %v\n", rf.me, i)
			// 	}
			// 	go rf.sendAppendEntries(i, heartbeatsArgs[i], heartbeatsReply[i])
			// }

			// // 统计当前心跳回复
			// replyCount := 1
			// majority := 1 + len(rf.peers)>>1
			// for replyCount < majority && !rf.isTimeout() {
			// 	replyCount = 0
			// 	for i := 0; i < len(rf.peers); i++ {
			// 		if i == rf.me {
			// 			continue
			// 		}
			// 		if heartbeatsReply[i].Success {
			// 			replyCount++
			// 		}
			// 	}
			// }
			// // 判断退出情况
			// // if LeaderChangeLog {
			// // 	fmt.Printf("在任期 %v 内 %v 获得 %v/%v 票\n", electionTerm, rf.me, voteCount, len(rf.peers))
			// // }

			// if replyCount < majority {
			// 	rf.isLeader = false
			// 	rf.setTimeout()
			// 	rf.votedFor = -1
			// 	rf.heartbeatsCh <- true
			// }
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
	rf.isLeader = false
	rf.heartbeatsCh = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if TimeoutLog {
		fmt.Printf("%v连接 其他已知节点数%v\n", me, len(peers))
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.doHeartBeats()

	return rf
}
