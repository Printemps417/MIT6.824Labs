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
	"6.824/labrpc"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	state         RaftState
	appendEntryCh chan *Entry

	electionTime    time.Time
	SnapShotTime    time.Time
	SnapShotTimeOut time.Duration
	heartBeat       time.Duration
	rpcTimeout      time.Duration
	//每次调用electionTime都会重新计算
	//electionTimeout time.Duration

	//所有服务器的持久性状态
	currentTerm int
	votedFor    int
	logs        Log

	//所有服务器的易变状态
	commitIndex int
	lastApplied int
	//leader的易变状态
	nextIndex  []int
	matchIndex []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	lastSnapshotIndex int // 快照中的 index
	lastSnapshotTerm  int
	InstallList       []bool
	// 通知应用层的channel
	notifyApplyCh chan struct{}
	stopCh        chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// leader添加日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.logs.lastLog().Index + 1
	term := rf.currentTerm

	log := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.logs.append(log)
	rf.persist()
	DPrintf("[%v] :TERM:%v START:%v", rf.me, term, log)
	rf.appendEntriesToPeers(false)

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.state == Leader {
			//领导者到时发送心跳
			rf.appendEntriesToPeers(true)
		}
		if time.Now().After(rf.electionTime) {
			//追随者到时开始竞选
			rf.leaderElection()
		}
		//if rf.state == Leader && time.Now().After(rf.SnapShotTime) {
		//	DPrintf("【Node %v】 *****************Leader定期发送快照***************", rf.me)
		//	for peer, _ := range rf.peers {
		//		go rf.sendInstallSnapshotToPeer(peer)
		//	}
		//	//重置快照时间
		//	rf.resetSnapShotTimer()
		//}
		DPrintf("【【Node %v】】's state is {role %v,term %v,commitIndex %v,lastApplied %v,lastSnapshot: %v,\nlogs: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastSnapshotIndex, rf.logs.String())
		//DPrintf("【Node %v】's state is {role %v,term %v,commitIndex %v,lastApplied %v,lastSnapshot: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastSnapshotIndex)
		rf.mu.Unlock()
	}
}
func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	DPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.lastApplied < rf.lastSnapshotIndex {
			DPrintf("【%v】: Restarting using snapshot…………", rf.me)
			DPrintf("【【Node %v】】's state is {role %v,term %v,commitIndex %v,lastApplied %v,lastSnapshot: %v,\nlogs: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.lastSnapshotIndex, rf.logs.String())
			rf.lastApplied = rf.lastSnapshotIndex
			////等待快照安装完成
			//rf.InstallLocalSnapshot()
			////rf.CondInstallSnapshot(rf.lastSnapshotIndex, rf.lastSnapshotTerm, rf.persister.ReadSnapshot())
			continue
		}
		if rf.commitIndex > rf.lastApplied && rf.logs.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logsat(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			//DPrintf("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}
func (rf *Raft) commits() string {
	nums := []string{}
	for i := rf.lastSnapshotIndex + 1; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.logsat(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = 50 * time.Millisecond
	rf.SnapShotTime = time.Now()
	rf.SnapShotTimeOut = 10 * time.Second
	rf.rpcTimeout = 100 * time.Millisecond

	//初始化时间
	rf.resetElectionTimer()
	rf.resetSnapShotTimer()

	rf.logs = makeEmptyLog()
	rf.logs.append(Entry{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.InstallList = make([]bool, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.notifyApplyCh = make(chan struct{}, 100)
	rf.stopCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

// 返回当前状态机的最后一条日志的任期和索引
// 索引是一直会增大的，但是我们的日志队列却不可能无限增大，在队列中下标0存储快照
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	//return rf.logs.lastLog().Term, rf.lastSnapshotIndex + rf.logs.len() - 1
	return rf.logs.lastLog().Term, rf.logs.lastLog().Index
}

// 获取当前存储位置的索引
func (rf *Raft) getStoreIndexByLogIndex(logIndex int) int {
	storeIndex := logIndex - rf.lastSnapshotIndex
	if storeIndex < 0 {
		return -1
	}
	return storeIndex
}
func (rf *Raft) logsat(logIndex int) *Entry {
	if rf.getStoreIndexByLogIndex(logIndex) >= 0 {
		return rf.logs.at(rf.getStoreIndexByLogIndex(logIndex))
	} else {
		return nil
	}
}
