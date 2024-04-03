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

	heartBeat    time.Duration
	electionTime time.Time
	rpcTimeout   time.Duration

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
	notifyApplyCh     chan struct{}
	stopCh            chan struct{}
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
		if rf.commitIndex > rf.lastApplied && rf.logs.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
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
	for i := 0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.logs.at(i).Command))
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
	rf.resetElectionTimer()
	rf.rpcTimeout = 100 * time.Millisecond

	rf.logs = makeEmptyLog()
	rf.logs.append(Entry{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

// Lad2D
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// 其实CondInstallSnapshot中的逻辑可以直接在InstallSnapshot中来完成，让CondInstallSnapshot成为一个空函数，这样可以减少锁的获取和释放
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2D).
	//installLen := lastIncludedIndex - rf.lastSnapshotIndex
	//if installLen >= len(rf.logs)-1 {
	//	rf.logs = make([]LogEntry, 1)
	//	rf.logs[0].Term = lastIncludedTerm
	//} else {
	//	rf.logs = rf.logs[installLen:]
	//}
	_, lastIndex := rf.getLastLogTermAndIndex()
	if lastIncludedIndex > lastIndex {
		rf.logs = makeEmptyLog()
	} else {
		installLen := lastIncludedIndex - rf.lastSnapshotIndex
		rf.logs.Entries = rf.logs.Entries[installLen:]
		rf.logs.Entries[0].Command = nil
	}
	//0处是空日志，代表了快照日志的标记
	rf.logs.Entries[0].Term = lastIncludedTerm

	//其实接下来可以读入快照的数据进行同步，这里可以不写

	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	//保存快照和状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 生成一次快照，实现很简单，删除掉对应已经被压缩的 raft log 即可
// index是当前要压缩到的index，snapshot是已经帮我们压缩好的数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.lastSnapshotIndex
	if snapshotIndex >= index {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	oldLastSnapshotIndex := rf.lastSnapshotIndex
	rf.lastSnapshotTerm = rf.logs.Entries[rf.getStoreIndexByLogIndex(index)].Term
	rf.lastSnapshotIndex = index
	//删掉index前的所有日志
	rf.logs.Entries = rf.logs.Entries[index-oldLastSnapshotIndex:]
	//0位置就是快照命令
	rf.logs.Entries[0].Term = rf.lastSnapshotTerm
	rf.logs.Entries[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	DPrintf("{Node %v}'s state is {role %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, snapshotIndex)
}

// 返回当前状态机的最后一条日志的任期和索引
// 索引是一直会增大的，但是我们的日志队列却不可能无限增大，在队列中下标0存储快照
func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	return rf.logs.at(rf.logs.len() - 1).Term, rf.lastSnapshotIndex + rf.logs.len() - 1
}

// 获取当前存储位置的索引
func (rf *Raft) getStoreIndexByLogIndex(logIndex int) int {
	storeIndex := logIndex - rf.lastSnapshotIndex
	if storeIndex < 0 {
		return -1
	}
	return storeIndex
}
