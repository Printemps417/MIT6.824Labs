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
	"6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"strings"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
	heartBeat     time.Duration
	electionTime  time.Time

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	DPrintf("[%v] :STATE:%v", rf.me, rf.logs.String())
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// readPersist方法用于恢复之前持久化的状态。
// 如果数据为空或长度小于1（即没有任何状态），则不进行任何操作。
// 否则，将使用labgob的解码器从字节数据中解码出currentTerm（当前任期）、voteFor（投票给谁）和logs（日志）。
// 如果在解码过程中出现错误，将记录致命错误并停止程序。
// 如果解码成功，将恢复Raft实例的currentTerm、votedFor和logs状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs Log

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	rf.appendEntries(false)

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
