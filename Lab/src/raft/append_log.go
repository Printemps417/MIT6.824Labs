package raft

//日志复制与提交相关

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry //日志条目，作为心跳时为空
	LeaderCommit int     //领导者的commitIndex,表示领导者已经提交的日志条目的索引
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool //冲突标志
	XTerm    int  //冲突的日志条目的任期
	XIndex   int  //冲突的日志条目的索引
	XLen     int
}

func (rf *Raft) appendEntriesToPeers(heartbeat bool) {
	//在领导者服务器上被调用，用于向所有的追随者发送日志条目or心跳
	// 获取最后一条日志
	lastLog := rf.logs.lastLog()
	// 遍历所有的服务器
	for peer, _ := range rf.peers {
		// 如果服务器是自己
		if peer == rf.me {
			// 发送心跳后重置选举定时器
			rf.resetElectionTimer()
			continue
		}
		if rf.InstallList[peer] {
			//如果当前peer正在安装快照，不发送日志
			rf.resetElectionTimer()
			continue
		}
		//if rf.nextIndex[peer] <= rf.lastSnapshotIndex {
		//	// 如果下一个索引小于或等于快照的索引，发送快照
		//	go rf.sendInstallSnapshotToPeer(peer)
		//	continue
		//}
		// 如果最后一条日志的索引大于或等于下一个索引，或者是心跳
		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			// 获取下一个索引
			nextIndex := rf.nextIndex[peer]
			// 如果下一个索引小于或等于0，那么设置下一个索引为1
			if nextIndex <= 0 {
				nextIndex = 1
			}
			// 如果最后一条日志的索引+1小于下一个索引，那么设置下一个索引为最后一条日志的索引
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			// 获取前一条日志
			prevLog := rf.logsat(nextIndex - 1)
			if prevLog == nil {
				continue
			}
			// 初始化附加日志条目请求的参数
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,                           // 当前任期
				LeaderId:     rf.me,                                    // 领导者ID
				PrevLogIndex: prevLog.Index,                            // 前一条日志的索引
				PrevLogTerm:  prevLog.Term,                             // 前一条日志的任期
				Entries:      make([]Entry, lastLog.Index-nextIndex+1), // 日志条目
				LeaderCommit: rf.commitIndex,                           // 领导者的commitIndex
			}
			// 深拷贝复制日志条目，不然会和Snapshot()发生数据上的冲突
			copy(args.Entries, rf.logs.slice(rf.getStoreIndexByLogIndex(nextIndex)))
			// 启动一个新的goroutine来处理领导者发送日志条目的逻辑
			go rf.leaderSendEntries(peer, &args)
		}
	}
}
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	// 向指定的服务器serverId发送附加日志条目请求
	var reply AppendEntriesReply
	//RPC调用，使远程服务器接收领导者发送的日志条目
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// 回复的任期大于当前任期
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		// 请求的任期等于当前任期
		// rules for leader 3.1
		if reply.Success {
			//添加成功
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			//发生冲突
			DPrintf("[%v]: Conflict from %v %#v", rf.me, serverId, reply)
			if rf.lastSnapshotIndex > reply.XIndex {
				if !rf.InstallList[serverId] {
					//防止重复发送快照
					DPrintf("【Node %v conflict】CASE1发送快照", serverId)
					go rf.sendInstallSnapshotToPeer(serverId)
				}
			} else {
				if reply.XTerm == args.PrevLogTerm {
					//任期相同: If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
					if rf.nextIndex[serverId] > 1 {
						DPrintf("【Node %v conflict】CASE2递减", serverId)
						//落后不递减，直接设置为冲突的日志条目的索引
						//rf.nextIndex[serverId]--
						rf.nextIndex[serverId] = reply.XIndex
					}
				} else {
					// 任期不同，查找冲突的日志条目的任期的最后一个日志条目
					lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm) // 查找冲突的日志条目的任期的最后一个日志条目
					DPrintf("【Node %v conflict】CASE3日志冲突", serverId)
					DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
					if lastLogInXTerm > 0 {
						// 如果找到，设置nextIndex为冲突的日志条目的任期的最后一个日志条目的索引
						rf.nextIndex[serverId] = lastLogInXTerm
					} else {
						// 如果没有找到，设置nextIndex为冲突的日志条目的索引
						rf.nextIndex[serverId] = reply.XIndex
					}
				}
			}
			DPrintf("[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else {
			//2D并发错误
			DPrintf("[%v]:2D Conflict from %v %#v", rf.me, serverId, reply)
			match := reply.XIndex
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
		}
		rf.leaderCommitRule() // 执行领导者提交
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()         // 加锁，防止并发操作
	defer rf.mu.Unlock() // 函数结束时解锁
	DPrintf("【RECEIVE】[%d]: (term %d) follower have receive from [%v]!! prevIndex %v, prevTerm %v,AppendEntries %v", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	// server rules
	//  If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	reply.Success = false       // 默认回复的成功标志为false
	reply.Term = rf.currentTerm // 设置回复的任期为当前任期
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term) // 如果请求的任期大于当前任期，更新当前任期
		return
	}

	// 附加日志 rpc 1
	if args.Term < rf.currentTerm {
		return // 如果请求的任期小于当前任期，直接返回
	}
	rf.resetElectionTimer() // 重置选举定时器

	// 候选人规则 3  If AppendEntries RPC received from new leader: convert to follower
	if rf.state == Candidate {
		rf.state = Follower // 如果当前状态是候选人，转变为追随者
	}

	//Lab2D错误：如果PrevLogIndex大于快照的最后一个日志索引，会导致冲突，但是这种情况下不应该冲突，应该直接返回成功
	if args.PrevLogIndex < rf.lastSnapshotIndex {
		//未成功但不冲突的情况，更新nextIndex
		reply.Conflict = false
		//最后安装位置的索引没有冲突
		reply.XIndex = rf.lastSnapshotIndex + 1
		reply.XTerm = rf.lastSnapshotTerm
		DPrintf("[%v]: HAVE SNAPSHOT! Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		DPrintf("【Node %v】's state is {role %v,term %v,commitIndex %v,lastApplied %v,\nlogs: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs.String())
		return
	}
	//日志追赶
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.logs.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true // 设置冲突标志为true
		//reply.XTerm = -1
		//reply.XIndex = -1          // 设置冲突的日志条目的索引为-1
		reply.XTerm = rf.logs.lastLog().Term
		//需要的下一个日志
		reply.XIndex = rf.logs.lastLog().Index + 1
		reply.XLen = rf.logs.len() // 设置冲突的日志条目的长度为日志的长度
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		DPrintf("【Node %v】's state is {role %v,term %v,commitIndex %v,lastApplied %v,\nlogs: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs.String())
		return
	}

	//日志冲突
	if rf.logsat(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true                      // 设置冲突标志为true
		xTerm := rf.logsat(args.PrevLogIndex).Term // 获取冲突的日志条目的任期
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.logsat(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex // 设置冲突的日志条目的索引
				break
			}
		}
		reply.XTerm = xTerm        // 设置冲突的日志条目的任期
		reply.XLen = rf.logs.len() // 设置冲突的日志条目的长度为日志的长度
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		DPrintf("【Node %v】's state is {role %v,term %v,commitIndex %v,lastApplied %v,\nlogs: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs.String())
		return
	}

	//没冲突，reply.Conflict=false，正常追加日志
	for idx, entry := range args.Entries {
		// 附加日志 rpc 3  If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		if entry.Index <= rf.logs.lastLog().Index && rf.logsat(entry.Index).Term != entry.Term {
			rf.logs.truncate(rf.getStoreIndexByLogIndex(entry.Index)) // 截断不同的日志
			rf.persist()                                              // 持久化日志
		}
		// 附加日志 rpc 4 Append any new entries not already in the log
		if entry.Index > rf.logs.lastLog().Index {
			rf.logs.append(args.Entries[idx:]...) // 追加日志条目
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist() // 持久化日志
			break
		}
	}

	// 附加日志 rpc 5 If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.lastLog().Index) // 更新提交的索引
		rf.apply()                                                       // 应用日志条目
	}
	//DPrintf("*****************************Append Success!*****************************\n")
	//DPrintf("【Node %v】's state is {role %v,term %v,commitIndex %v,lastApplied %v,logs: %v} ", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logs.String())
	//DPrintf("*************************************************************************\n")
	reply.Success = true // 设置回复的成功标志为true
}
func (rf *Raft) leaderCommitRule() {
	// 领导者规则4：
	//If there exists an N such that N > commitIndex,
	//a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	//set commitIndex = N (§5.3, §5.4).
	if rf.state != Leader {
		// 如果当前状态不是领导者，直接返回
		return
	}

	for n := rf.commitIndex + 1; n <= rf.logs.lastLog().Index; n++ {
		// 从commitIndex+1开始，遍历到最后一条日志的索引
		if rf.logsat(n).Term != rf.currentTerm {
			// 如果日志的任期不等于当前任期，跳过当前循环
			continue
		}
		counter := 1 // 初始化计数器
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				// 服务器不是自己且matchIndex大于或等于n，计数器加1
				counter++
			}
			if counter > len(rf.peers)/2 {
				// 如果计数器大于服务器数量的一半
				rf.commitIndex = n
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				// 领导者提交的索引
				rf.apply()
				// 应用日志条目
				break
			}
		}
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	//查找给定任期（term）的最后一个日志条目的索引
	for i := rf.logs.lastLog().Index; i > rf.lastSnapshotIndex; i-- {
		term := rf.logsat(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}
