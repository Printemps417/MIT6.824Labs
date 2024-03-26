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
