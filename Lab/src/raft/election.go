package raft

import (
	"math/rand"
	"sync"
	"time"
)

// 请求投票相关
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 请求投票的参数，包括当前的term，候选人的id，候选人的最后一条日志的index和term
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 请求投票的返回值，包括当前的term和是否投票
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		//如果RPC请求或响应包含任期 T > currentTerm：设置currentTerm = T，转换为follower（§5.1）
		rf.setNewTerm(args.Term)
	}

	// request vote rpc receiver 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 获取当前服务器的最后一条日志
	myLastLog := rf.logs.lastLog()
	// uptodate判断是否进行更新
	// 判断请求投票的服务器的任期更大or最后一条日志是否比当前服务器的最后一条日志更新
	upToDate := args.LastLogTerm > myLastLog.Term ||
		(args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
	// 如果当前服务器还没有投票或者已经投给了请求投票的服务器，并且请求投票的服务器的最后一条日志比当前服务器的最后一条日志更新
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		// 则投票给请求投票的服务器
		reply.VoteGranted = true
		// 记录已经投给了请求投票的服务器
		rf.votedFor = args.CandidateId
		// 持久化当前服务器的状态
		rf.persist()
		// 重置选举定时器
		rf.resetElectionTimer()
		// 打印调试信息。当前server索引，任期与投票
		DPrintf("[%v]: term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		// 否则不投票给请求投票的服务器
		reply.VoteGranted = false
	}
	// 返回当前服务器的任期
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// candidateRequestVote方法用于处理候选人请求投票的逻辑。调用的server向serverId的服务器请求投票
// serverId是目标服务器的索引，args是请求投票的参数，becomeLeader是一个sync.Once对象，用于确保某个操作只执行一次，voteCounter是投票计数器。
func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, becomeLeader *sync.Once, voteCounter *int) {
	// 打印调试信息，显示发送投票请求
	DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)
	// 初始化投票回复
	reply := RequestVoteReply{}
	// 发送投票请求，如果失败则返回
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	// 加锁以保护共享资源
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果回复的任期大于请求的任期，更新任期并返回
	if reply.Term > args.Term {
		DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}
	// 如果回复的任期小于请求的任期，打印调试信息并返回
	if reply.Term < args.Term {
		DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}
	// 如果没有获得投票，打印调试信息并返回
	if !reply.VoteGranted {
		DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	// 打印调试信息，显示获得投票
	DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)

	// 投票计数器加一
	*voteCounter++

	// 如果获得的投票数超过一半，并且当前任期等于请求的任期，且当前状态为候选人
	if *voteCounter > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == Candidate {
		// 打印调试信息，显示获得多数投票
		DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)
		// 执行一次成为领导者的操作。sync.Once对象的Do方法确保只执行一次
		becomeLeader.Do(func() {
			// 打印调试信息，显示当前任期结束
			DPrintf("[%d]: 当前term %d 结束\n", rf.me, rf.currentTerm)
			// 设置当前状态为领导者
			rf.state = Leader
			// 获取最后一条日志的索引
			lastLogIndex := rf.logs.lastLog().Index
			// 更新nextIndex和matchIndex
			for i, _ := range rf.peers {
				//当新的领导者产生时，初始化所有的nextIndex为最后一条日志的索引加一，初始化所有的matchIndex为0
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			// 打印调试信息，显示领导者的nextIndex
			DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)
			// 发送心跳。2B
			rf.appendEntriesToPeers(true)
		})
	}
}

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

// 选举相关
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	//每次调用都将竞选时间推后150+随机数150毫秒
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	// 如果传入的term大于当前的term，或者当前的term为0，那么就将当前的状态设置为Follower
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = Follower
		rf.currentTerm = term
		rf.votedFor = -1
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		//持久化当前状态
		rf.persist()
	}
}

// leaderElection方法用于处理领导者选举的逻辑。
func (rf *Raft) leaderElection() {
	// 增加当前任期
	rf.currentTerm++
	// 将当前状态设置为候选人
	rf.state = Candidate
	// 投票给自己
	rf.votedFor = rf.me
	// 持久化当前状态
	rf.persist()
	// 重置选举定时器
	rf.resetElectionTimer()
	// 获取当前任期
	term := rf.currentTerm
	// 初始化投票计数器
	voteCounter := 1
	// 获取最后一条日志
	lastLog := rf.logs.lastLog()
	// 打印调试信息，显示开始领导者选举
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)
	// 初始化请求投票的参数
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	// 初始化一个sync.Once对象，用于确保某个操作只执行一次
	var becomeLeader sync.Once
	// 遍历所有的服务器
	for serverId, _ := range rf.peers {
		// 如果服务器不是自己
		if serverId != rf.me {
			// 启动一个新的goroutine来处理候选人请求投票的逻辑
			go rf.candidateRequestVote(serverId, &args, &becomeLeader, &voteCounter)
		}
	}
}
