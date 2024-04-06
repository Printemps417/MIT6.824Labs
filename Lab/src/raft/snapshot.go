package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//Offset            int
	Data []byte
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

// // 向指定节点发送快照
//
//	func (rf *Raft) sendInstallSnapshotToPeer(server int) {
//		rf.mu.Lock()
//		args := InstallSnapshotArgs{
//			Term:              rf.currentTerm,
//			LeaderId:          rf.me,
//			LastIncludedIndex: rf.lastSnapshotIndex,
//			LastIncludedTerm:  rf.lastSnapshotTerm,
//			Data:              rf.persister.ReadSnapshot(),
//		}
//		rf.mu.Unlock()
//		rf.InstallList[server] = true
//		timer := time.NewTimer(rf.rpcTimeout)
//
//		DPrintf("【%v】 Snapshot installing", server)
//		defer func() {
//			timer.Stop()
//			rf.InstallList[server] = false
//			DPrintf("【%v】Snapshot install finish", server)
//		}()
//
//		DPrintf("%v role: %v, send snapshot  to peer,%v,args = %+v", rf.me, rf.state, server, args)
//		//cnt := 0
//		//reply := &InstallSnapshotReply{}
//		//for !rf.killed() {
//		//	cnt++
//		//	if cnt > 10 {
//		//		//超时
//		//		DPrintf("【%v】TIME OUT!!Snapshot install failed", server)
//		//		return
//		//	}
//		//	DPrintf("%v role: %v, send snapshot  to peer,%v,args = %+v", rf.me, rf.state, server, args)
//		//	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
//		//	if !ok {
//		//		time.Sleep(time.Millisecond * 200)
//		//	} else {
//		//		break
//		//	}
//		//}
//		//
//		for {
//			timer.Stop()
//			timer.Reset(rf.rpcTimeout)
//
//			ch := make(chan bool, 1)
//			reply := &InstallSnapshotReply{}
//			go func() {
//				ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
//				if !ok {
//					time.Sleep(time.Millisecond * 10)
//				}
//				ch <- ok
//			}()
//
//			select {
//			case <-rf.stopCh:
//				return
//			case <-timer.C:
//				DPrintf("%v role: %v, send snapshot to peer %v TIME OUT!!!", rf.me, rf.state, server)
//				continue
//			case ok := <-ch:
//				if !ok {
//					continue
//				}
//			}
//
//			rf.mu.Lock()
//			defer rf.mu.Unlock()
//			if rf.state != Leader || args.Term != rf.currentTerm {
//				return
//			}
//			if reply.Term > rf.currentTerm {
//				rf.state = Follower
//				rf.currentTerm = reply.Term
//				rf.resetElectionTimer()
//				rf.persist()
//				return
//			}
//			//发送快照成功，更新matchindex和nextindex
//			if args.LastIncludedIndex > rf.matchIndex[server] {
//				DPrintf("Update 【%v】 matchIndex,from %v to %v", server, rf.matchIndex[server], args.LastIncludedIndex)
//				rf.matchIndex[server] = args.LastIncludedIndex
//			}
//			if args.LastIncludedIndex+1 > rf.nextIndex[server] {
//				DPrintf("Update 【%v】 nextIndex,from %v to %v", server, rf.nextIndex[server], args.LastIncludedIndex+1)
//				rf.nextIndex[server] = args.LastIncludedIndex + 1
//			}
//			//DPrintf("【%v】Snapshot installed successfully!next:【%v】match:【%v】", server, rf.nextIndex[server], rf.matchIndex[server])
//			return
//		}
//	}
//
// 向指定节点发送快照
func (rf *Raft) sendInstallSnapshotToPeer(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	rf.InstallList[server] = true
	timer := time.NewTimer(rf.rpcTimeout)

	DPrintf("【%v】 Snapshot installing", server)
	defer func() {
		timer.Stop()
		rf.InstallList[server] = false
		DPrintf("【%v】Snapshot install finish", server)
	}()
	cnt := 0
	reply := &InstallSnapshotReply{}
	for !rf.killed() {
		cnt++
		if cnt > 5 {
			//超时
			DPrintf("【%v】TIME OUT!!Snapshot install failed", server)
			return
		}
		rf.mu.Lock()
		DPrintf("%v role: %v, send snapshot  to peer,%v,args = %+v", rf.me, rf.state, server, args)
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, reply)
		rf.mu.Unlock()
		if !ok {
			time.Sleep(time.Millisecond * 200)
		} else {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.resetElectionTimer()
		rf.persist()
		return
	}
	//发送快照成功，更新matchindex和nextindex
	if args.LastIncludedIndex > rf.matchIndex[server] {
		DPrintf("Update 【%v】 matchIndex,from %v to %v", server, rf.matchIndex[server], args.LastIncludedIndex)
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex+1 > rf.nextIndex[server] {
		DPrintf("Update 【%v】 nextIndex,from %v to %v", server, rf.nextIndex[server], args.LastIncludedIndex+1)
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
	//DPrintf("【%v】Snapshot installed successfully!next:【%v】match:【%v】", server, rf.nextIndex[server], rf.matchIndex[server])
	return
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}

	if args.Term > rf.currentTerm || rf.state != Follower {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.resetElectionTimer()
		rf.persist()
	}
	DPrintf("【INSIDE】【%v】Snapshot install begining", rf.me)
	//如果自身快照包含的最后一个日志>=leader快照包含的最后一个日志，就没必要接受了
	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		DPrintf("【INSIDE】【%v】Snapshot install failed! rf.lastSnapshotIndex >= args.LastIncludedIndex", rf.me)
		return
	}
	/********以下内容和CondInstallSnapshot的操作是相同的，因为不知道为什么在lab4B中只要调用CondInstallSnapshot函数就会陷入阻塞，因此将操作逻辑复制到这里一份，lab4中就没有调用CondInstallSnapshot函数了***********/

	lastIncludedIndex := args.LastIncludedIndex
	lastIncludedTerm := args.LastIncludedTerm
	snapshot := args.Data
	DPrintf("【INSIDE】【%v】Snapshot install begin,last:lastapplied:%v commit:%v", rf.me, rf.lastApplied, rf.commitIndex)
	_, lastIndex := rf.getLastLogTermAndIndex()
	if lastIncludedIndex > lastIndex {
		rf.logs = makeInitLog(1)
	} else {
		installLen := lastIncludedIndex - rf.lastSnapshotIndex
		rf.logs.Entries = rf.logs.Entries[installLen:]
		rf.logs.Entries[0].Command = "SnapShot"
	}
	//0处是空日志，代表了快照日志的标记。添加日志时，0处的日志不会被覆盖，index与term和快照时相同
	rf.logs.Entries[0].Term = lastIncludedTerm
	rf.logs.Entries[0].Index = lastIncludedIndex
	//接下来可以读入快照的数据进行同步，这里可以不写
	rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	//保存新接受的快照和状态
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	DPrintf("【INSIDE】【%v】Snapshot install finish,last:lastapplied:%v commit:%v", rf.me, rf.lastApplied, rf.commitIndex)
	//复制快照结束
	/***********************************/
	DPrintf("【INSIDE】【%v】Snapshot install finish", rf.me)
	//接收发来的快照，并提交一个命令处理
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

// CondInstallSnapshot Lad2D
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// 其实CondInstallSnapshot中的逻辑可以直接在InstallSnapshot中来完成，让CondInstallSnapshot成为一个空函数，这样可以减少锁的获取和释放
func (rf *Raft) CondInstallSnapshot(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) bool {
	// Your code here (2D).
	//installLen := lastIncludedIndex - rf.lastSnapshotIndex
	//if installLen >= len(rf.logs)-1 {
	//	rf.logs = make([]LogEntry, 1)
	//	rf.logs[0].Term = lastIncludedTerm
	//} else {
	//	rf.logs = rf.logs[installLen:]
	//}

	//DPrintf("【INSIDECond】【%v】Snapshot install begin,last:lastapplied:%v commit:%v", rf.me, rf.lastApplied, rf.commitIndex)
	//_, lastIndex := rf.getLastLogTermAndIndex()
	//if lastIncludedIndex > lastIndex {
	//	rf.logs = makeInitLog(1)
	//} else {
	//	installLen := lastIncludedIndex - rf.lastSnapshotIndex
	//	rf.logs.Entries = rf.logs.Entries[installLen:]
	//	rf.logs.Entries[0].Command = "SnapShot"
	//}
	////0处是空日志，代表了快照日志的标记
	//rf.logs.Entries[0].Term = lastIncludedTerm
	//
	////其实接下来可以读入快照的数据进行同步，这里可以不写
	//
	//rf.lastSnapshotIndex, rf.lastSnapshotTerm = lastIncludedIndex, lastIncludedTerm
	//rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	////保存新接受的快照和状态
	//rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	//DPrintf("【INSIDECond】【%v】Snapshot install finish,last:lastapplied:%v commit:%v", rf.me, rf.lastApplied, rf.commitIndex)
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
	rf.logs.Entries[0].Index = index
	rf.logs.Entries[0].Term = rf.lastSnapshotTerm
	rf.logs.Entries[0].Command = "SnapShot"
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
	DPrintf("*****************************{Node %v}'s SNAPSHOT!!*****************************\n"+
		"State is {role %v,term %v,commitIndex %v,lastApplied %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller\n"+
		"********************************************************************************************************************", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, index, snapshotIndex)
}
