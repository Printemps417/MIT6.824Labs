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

func (rf *Raft) appendEntries(heartbeat bool) {
	//添加日志or心跳
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
			prevLog := rf.logs.at(nextIndex - 1)
			// 初始化附加日志条目请求的参数
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,                           // 当前任期
				LeaderId:     rf.me,                                    // 领导者ID
				PrevLogIndex: prevLog.Index,                            // 前一条日志的索引
				PrevLogTerm:  prevLog.Term,                             // 前一条日志的任期
				Entries:      make([]Entry, lastLog.Index-nextIndex+1), // 日志条目
				LeaderCommit: rf.commitIndex,                           // 领导者的commitIndex
			}
			// 复制日志条目
			copy(args.Entries, rf.logs.slice(nextIndex))
			// 启动一个新的goroutine来处理领导者发送日志条目的逻辑
			go rf.leaderSendEntries(peer, &args)
		}
	}
}
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		// rules for leader 3.1
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v", rf.me, serverId, reply)
			if reply.XTerm == -1 {
				rf.nextIndex[serverId] = reply.XLen
			} else {
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[serverId] = lastLogInXTerm
				} else {
					rf.nextIndex[serverId] = reply.XIndex
				}
			}

			DPrintf("[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			rf.nextIndex[serverId]--
		}
		rf.leaderCommitRule()
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.logs.lastLog().Index; i > 0; i-- {
		term := rf.logs.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (rf *Raft) leaderCommitRule() {
	// leader rule 4
	if rf.state != Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.logs.lastLog().Index; n++ {
		if rf.logs.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	// rules for servers
	// all servers 2
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	// append entries rpc 1
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	// candidate rule 3
	if rf.state == Candidate {
		rf.state = Follower
	}
	// append entries rpc 2
	if rf.logs.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.logs.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.logs.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.logs.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.logs.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.logs.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	for idx, entry := range args.Entries {
		// append entries rpc 3
		if entry.Index <= rf.logs.lastLog().Index && rf.logs.at(entry.Index).Term != entry.Term {
			rf.logs.truncate(entry.Index)
			rf.persist()
		}
		// append entries rpc 4
		if entry.Index > rf.logs.lastLog().Index {
			rf.logs.append(args.Entries[idx:]...)
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}

	// append entries rpc 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.logs.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
