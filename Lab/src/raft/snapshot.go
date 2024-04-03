package raft

import (
	"log"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
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
func (rf *Raft) Snapshot(logIndex int, snapshotData []byte) {
	rf.mu.Lock()
	DPrintf("savePs get logindex:%d", logIndex)
	defer rf.mu.Unlock()

	if logIndex <= rf.lastSnapshotIndex {
		return
	}

	if logIndex > rf.commitIndex {
		panic("logindex > rf.commitdex")
	}

	// TODO
	DPrintf("before savePS, logindex:%d, lastspindex:%d, logslen:%d, logs:%+v", logIndex, rf.lastSnapshotIndex, rf.logs.len(), rf.logs.String())
	// logindex 一定在 raft.logEntries 中存在
	lastLog := rf.logs.at(logIndex)
	rf.logs.Entries = rf.logs.Entries[rf.getRealIdxByLogIndex(logIndex):]
	rf.lastSnapshotIndex = logIndex
	rf.lastSnapshotTerm = lastLog.Term
	persistData := rf.getPersistData()
	rf.persister.SaveStateAndSnapshot(persistData, snapshotData)
}

func (rf *Raft) sendInstallSnapshot(peerIdx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	timer := time.NewTimer(rf.rpcTimeout)
	defer timer.Stop()

	for {
		timer.Stop()
		timer.Reset(rf.rpcTimeout)
		okCh := make(chan bool, 1)
		reply := InstallSnapshotReply{}
		go func() {
			o := rf.peers[peerIdx].Call("Raft.InstallSnapshot", &args, &reply)
			if !o {
				time.Sleep(time.Millisecond * 10)
			}
			okCh <- o
		}()

		ok := false
		select {
		case <-rf.stopCh:
			return
		case <-timer.C:
			continue
		case ok = <-okCh:
			if !ok {
				continue
			}
		}

		// ok == true
		DPrintf("send_install_snapshot")
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != args.Term || rf.state != Leader {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}
		// success
		if args.LastIncludedIndex > rf.matchIndex[peerIdx] {
			rf.matchIndex[peerIdx] = args.LastIncludedIndex
		}
		if args.LastIncludedIndex+1 > rf.nextIndex[peerIdx] {
			rf.nextIndex[peerIdx] = args.LastIncludedIndex + 1
		}
		return

	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	DPrintf("install_snapshot")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.state != Follower {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.resetElectionTimer()
		defer rf.persist()
	}

	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}
	// success
	start := args.LastIncludedIndex - rf.lastSnapshotIndex
	if start < 0 {
		// 不可能
		log.Fatal("install sn")
	} else if start >= rf.logs.len() {
		rf.logs = makeEmptyLog()
		rf.logs.Entries[0].Term = args.LastIncludedTerm
		rf.logs.Entries[0].Index = args.LastIncludedTerm
	} else {
		rf.logs.Entries = rf.logs.Entries[start:]
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
}
func (rf *Raft) getRealIdxByLogIndex(logIndex int) int {
	idx := logIndex - rf.lastSnapshotIndex
	if idx < 0 {
		return -1
	} else {
		return idx
	}
}
