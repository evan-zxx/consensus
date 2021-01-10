package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/evan-zxx/consensus/labgob"
	"github.com/evan-zxx/consensus/labrpc"
)

/*
两个定时器:
1. eletionTimer: 每个节点自身持有, 一旦超时, 即将自己角色变为candidate, term++, 发起新一轮的选举
2. heartbeatTimers: leader节点持有, 一旦超时, 将发送心跳到各个follower(日志同步也是通过该定时器, 一旦有客户端发来的请求, 强制立刻超时该定时器, 触发日志同步)

logEntry三个状态:
1. insert: 客户端发起请求(kv: put key value), 此时直接将请求的log insert到logEntry的最后, 同时将heartbeatTimers置0, 立即触发日志复制流程.
2. append()->commit: 日志复制流程中如果成功复制到半数服务, 则leader会在updateCommitIndex()更新自己的commitIndex,
					 同时根据commitIndex会进行apply操作, 因为已经被commit的日志就是永久日志, leader如果还没apply,则直接进行apply
			         leader apply之后会立即触发heartbeatTimers, 通知followers们也进行日志apply(follower通过rcp的LeaderCommit参数获知leader已经提交的日志, 进而更新自己的提交日志, 进而进行apply)
3. apply: 前提是已经commit(过半数实例复制成功), apply可以适当delay, 因为已经commit的日志是永远不会再改变的, apply只是迟早的事情

*/

//import "os"
//节点状态
const Follower, Leader, Candidate int = 1, 2, 3

//心跳周期 600ms
const HeartbeatDuration = time.Duration(time.Millisecond * 600)

//竞选周期 1200ms
const CandidateDuration = HeartbeatDuration * 2

var raftOnce sync.Once

//状态机apply(最终真正apply log, 先由leader commit/append/update到集群中, 相应过半后再apply)
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//日志
type LogEntry struct {
	Term  int // 插入日志时的leader term
	Index int // 插入日志时的index
	Log   interface{}
}

//日志快照
type LogSnapshot struct {
	Term  int
	Index int
	Datas []byte
}

//投票请求
type RequestVoteArgs struct {
	Me           int // 自身服务编号(请求选票的候选人编号)
	ElectionTerm int // 候选人的任期
	LogIndex     int // 候选人最后日志索引
	LogTerm      int // 候选人最后日志的任期号
}

//投票rpc返回
type RequestVoteReply struct {
	IsAgree     bool // 候选人是否赢得了选票
	CurrentTerm int  // 当前任期 用于候选人去更新自己的任期
}

//日志复制请求(leader发送到各个follower)
type AppendEntries struct {
	Me           int         // 自身服务器编号
	Term         int         // 领导人的任期
	PrevLogIndex int         // 新的日志条目(需要提交的)紧随之前的索引值
	PrevLogTerm  int         // PrevLogIndex条目的任期值(以此来保证日志连续)
	Entries      []LogEntry  // 需要存储当前的日志条目(空表示心跳 一次发送多个为提高效率)
	LeaderCommit int         // 领导人已经提交的日志索引值
	Snapshot     LogSnapshot // 快照
	// 该rcp中不需要传递leader的apply log状态, 因为follower会根据LeaderCommit字段更新自己的commit log, 进而触发自己的apply(提交的日志还未apply, 则apply)
}

//回复日志更新请求
type AppendEntriesResp struct {
	Term        int  // 跟随者当时的任期值 用于领导人去更新自己(也有可能当时的跟随者已经成为了leader)
	Successed   bool // 跟随者复制日志成功(包含了匹配上PrevLogIndex & PrevLogTerm时为真, 保证日志连续)
	LastApplied int  // 跟随者最后应用的log
}

type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // rpc节点
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // 自己服务编号
	logs            []LogEntry          // 日志存储:每一个条目包含一个用户状态机执行的指令，和收到时leader自身的任期(leader只要收到用户状态机命令就会插入此log, 后续commit&apply依靠其他参数约束)
	logSnapshot     LogSnapshot         // 日志快照
	commitIndex     int                 // 当前日志提交处: 已知的最大的已经提交的日志条目索引(appendLog rpc)
	lastApplied     int                 // 当前状态机执行处: 最后应用到状态机的日志条目索引(真正apply到状态机)
	status          int                 // 节点状态
	currentTerm     int                 // 当前任期:服务器最后一次知道的任期号
	heartbeatTimers []*time.Timer       // 心跳定时器(同时会由leader向各个follower进行日志复制)
	eletionTimer    *time.Timer         // 竞选超时定时器(每个follower自身都会维护, 一旦超时就自身term++发起选举流程)
	randtime        *rand.Rand          // 随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex      []int         // 记录每个follower的同步日志状态: 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一），leader有用
	matchIndex     []int         // 记录每个follower日志最大索引 0递增: 对于每一个服务器，已经复制给他的日志的最高索引值，leader有用
	applyCh        chan ApplyMsg // 状态机apply
	isKilled       bool          // 节点退出
	lastLogs       AppendEntries // 最后更新日志
	EnableDebugLog bool          // 打印调试日志开关
	LastGetLock    string
}

//打印调试日志
func (rf *Raft) println(args ...interface{}) {
	if rf.EnableDebugLog {
		log.Println(args...)
	}
}

func (rf *Raft) lock(info string) {
	rf.mu.Lock()
	rf.LastGetLock = info
}

func (rf *Raft) unlock(info string) {
	rf.LastGetLock = ""
	rf.mu.Unlock()
}

//投票RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rstChan := make(chan bool)
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(HeartbeatDuration):
		//rpc调用超时
		//log.Print("[rpc] Raft.RequestVote timeout...")
	}
	return ok
}

//同步日志RPC
func (rf *Raft) sendAppendEnteries(server int, req *AppendEntries, resp *AppendEntriesResp) bool {
	rstChan := make(chan bool)
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(time.Millisecond * 400):
	}
	return ok
}

//获取状态和任期
func (rf *Raft) GetState() (int, bool) {
	rf.lock("Raft.GetState")
	defer rf.unlock("Raft.GetState")
	isleader := rf.status == Leader
	return rf.currentTerm, isleader
}

//设置任期
/*
目前有4中情况会更新term:
1. follower/candidate处理投票请求时, 发现投票发起者term比自己大, 更新为自己的term
2. 投票发起者收到投票后, 发现集群中存在更大term的节点, 将自己转变为follower, 并更新为自己的term
3. follower收到leader的日志下发/心跳后, 更新自己的term为Leader的term
4. leader收到follower的日志请求回复后, 发现该follower的term比自己大, 则更新为自己的term, 并设置自己为follower
*/
func (rf *Raft) setTerm(term int) {
	rf.lock("Raft.setTerm")
	defer rf.unlock("Raft.setTerm")
	rf.currentTerm = term
}

//增加任期
//只会在发起投票的时候自增任期
func (rf *Raft) addTerm(term int) {
	rf.lock("Raft.addTerm")
	defer rf.unlock("Raft.addTerm")
	rf.currentTerm += term
}

//设置当前节点状态
func (rf *Raft) setStatus(status int) {
	rf.lock("Raft.setStatus")
	defer rf.unlock("Raft.setStatus")
	//设置节点状态，变换为fallow时候重置选举定时器（避免竞争）
	if (rf.status != Follower) && (status == Follower) {
		rf.resetCandidateTimer()
	}

	//节点变为leader，则初始化follower日志状态
	if rf.status != Leader && status == Leader {
		index := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = index + 1 + rf.logSnapshot.Index
			rf.matchIndex[i] = 0
		}
	}
	rf.status = status
}

//获取状态
func (rf *Raft) getStatus() int {
	rf.lock("Raft.getStatus")
	defer rf.unlock("Raft.getStatus")
	return rf.status
}

//获取提交日志索引
func (rf *Raft) getCommitIndex() int {
	rf.lock("Raft.getCommitedCnt")
	defer rf.unlock("Raft.getCommitedCnt")
	return rf.commitIndex
}

//设置提交日志索引
func (rf *Raft) setCommitIndex(index int) {
	rf.lock("Raft.setCommitIndex")
	defer rf.unlock("Raft.setCommitIndex")
	rf.commitIndex = index
}

//获取日志索引及任期
func (rf *Raft) getLogTermAndIndex() (int, int) {
	rf.lock("Raft.getLogTermAndIndex")
	defer rf.unlock("Raft.getLogTermAndIndex")
	index := 0
	term := 0
	size := len(rf.logs)
	if size > 0 {
		index = rf.logs[size-1].Index
		term = rf.logs[size-1].Term
	} else {
		index = rf.logSnapshot.Index
		term = rf.logSnapshot.Term
	}
	return term, index
}

//获取索引处及任期
func (rf *Raft) getLogTermOfIndex(index int) int {
	rf.lock("Raft.getLogTermOfIndex")
	defer rf.unlock("Raft.getLogTermOfIndex")
	index -= (1 + rf.logSnapshot.Index)
	if index < 0 {
		return rf.logSnapshot.Term
	}
	return rf.logs[index].Term
}

//获取快照
func (rf *Raft) getSnapshot(index int, snapshot *LogSnapshot) int {
	if index <= rf.logSnapshot.Index { //如果fallow日志小于快照，则获取快照
		*snapshot = rf.logSnapshot
		index = 0 //更新快照时，从0开始复制日志
	} else {
		index -= rf.logSnapshot.Index
	}
	return index
}

//获取该节点更新日志
func (rf *Raft) getEntriesInfo(index int, snapshot *LogSnapshot, entries *[]LogEntry) (preterm int, preindex int) {
	start := rf.getSnapshot(index, snapshot) - 1
	if start < 0 {
		preindex = 0
		preterm = 0
	} else if start == 0 {
		if rf.logSnapshot.Index == 0 {
			preindex = 0
			preterm = 0
		} else {
			preindex = rf.logSnapshot.Index
			preterm = rf.logSnapshot.Term
		}
	} else {
		preindex = rf.logs[start-1].Index
		preterm = rf.logs[start-1].Term
	}
	if start < 0 {
		start = 0
	}
	for i := start; i < len(rf.logs); i++ {
		*entries = append(*entries, rf.logs[i])
	}
	return
}

//获取该节点更新日志及信息
func (rf *Raft) getAppendEntries(peer int) AppendEntries {
	rf.lock("Raft.getAppendEntries")
	defer rf.unlock("Raft.getAppendEntries")

	//[]LogEntry 为空: 标示心跳包
	rst := AppendEntries{
		Me:           rf.me,
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex,
		Snapshot:     LogSnapshot{Index: 0},
	}
	//当前leader自己的日志next状态
	next := rf.nextIndex[peer]
	//获取当前需要提交log的prelog信息(term+index)
	rst.PrevLogTerm, rst.PrevLogIndex = rf.getEntriesInfo(next, &rst.Snapshot, &rst.Entries)
	return rst
}

//减少fallow next日志索引
func (rf *Raft) incNext(peer int) {
	rf.lock("Raft.incNext")
	defer rf.unlock("Raft.incNext")
	if rf.nextIndex[peer] > 1 {
		rf.nextIndex[peer]--
	}
}

//设置fallow next日志索引
func (rf *Raft) setNext(peer int, next int) {
	rf.lock("Raft.setNext")
	defer rf.unlock("Raft.setNext")
	rf.nextIndex[peer] = next
}

//设置follower next和match日志索引
//此时标示该follower已经成功提交该索引的日志
func (rf *Raft) setNextAndMatch(peer int, index int) {
	rf.lock("Raft.setNextAndMatch")
	defer rf.unlock("Raft.setNextAndMatch")
	rf.nextIndex[peer] = index + 1
	rf.matchIndex[peer] = index
}

//更新插入同步日志
func (rf *Raft) updateLog(start int, logEntrys []LogEntry, snapshot *LogSnapshot) {
	rf.lock("Raft.updateLog")
	defer rf.unlock("Raft.updateLog")
	if snapshot.Index > 0 { //更新快照
		rf.logSnapshot = *snapshot
		start = rf.logSnapshot.Index
		rf.println("update snapshot :", rf.me, rf.logSnapshot.Index, "len logs", len(logEntrys))
	}
	index := start - rf.logSnapshot.Index
	for i := 0; i < len(logEntrys); i++ {
		if index+i < 0 {
			//网络不可靠，follower节点成功apply并保存快照后，Leader未收到反馈，重复发送日志，
			//可能会导致index <0情况。
			continue
		}
		if index+i < len(rf.logs) {
			rf.logs[index+i] = logEntrys[i]
		} else {
			rf.logs = append(rf.logs, logEntrys[i])
		}
	}
	size := index + len(logEntrys)
	if size < 0 { //网络不可靠+各节点独立备份快照可能出现
		size = 0
	}
	//重置log大小
	rf.logs = rf.logs[:size]
}

//插入日志 just for leader
func (rf *Raft) insertLog(command interface{}) int {
	rf.lock("Raft.insertLog")
	defer rf.unlock("Raft.insertLog")
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: 1,
		Log:   command,
	}
	//获取log索引
	if len(rf.logs) > 0 {
		entry.Index = rf.logs[len(rf.logs)-1].Index + 1
	} else {
		entry.Index = rf.logSnapshot.Index + 1
	}
	//插入log
	rf.logs = append(rf.logs, entry)
	return entry.Index
}

//更新当前已被提交日志索引commit log index
func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexs []int
	rf.matchIndex[rf.me] = 0
	if len(rf.logs) > 0 {
		rf.matchIndex[rf.me] = rf.logs[len(rf.logs)-1].Index
	} else {
		rf.matchIndex[rf.me] = rf.logSnapshot.Index
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		indexs = append(indexs, rf.matchIndex[i])
	}
	sort.Ints(indexs)
	// TODO:这里判断当前日志是否提交是否过半(leader的视角统计follower日志提交状态是异步的, 存在matchIndex数组里)
	// index为当前follower中已收到日志最高索引排序后的中间位置
	// commit为中间位置的值
	index := len(indexs) / 2
	commit := indexs[index]
	// 如果是新提交的日志 leader就提交
	// 因为如果排序后的中位数上的日志>当前已经提交的日志最大值, 说明是新的且已经过半提交的日志
	if commit > rf.commitIndex {
		rf.println(rf.me, "leader update commit index", rf.currentTerm, "-", commit)
		// 这里只是先更新commitIndex & 后续流程中再真正的提交(修改lastApplied)
		rst = true
		rf.commitIndex = commit
	}
	return rst
}

//apply 状态机(尝试)
func (rf *Raft) apply() {
	rf.lock("Raft.apply")
	defer rf.unlock("Raft.apply")
	// 如果是leader
	if rf.status == Leader {
		// 这里不care返回值 因为在设置返回这成功的同时, 也更新了commitLog
		rf.updateCommitIndex()
	}

	lastapplied := rf.lastApplied
	// 如果当前自身applied小于快照的index, 则直接应用状态机, 先追赶快照的index
	if rf.lastApplied < rf.logSnapshot.Index {
		msg := ApplyMsg{
			CommandValid: false,
			Command:      rf.logSnapshot,
			CommandIndex: 0,
		}
		rf.applyCh <- msg
		rf.lastApplied = rf.logSnapshot.Index
		rf.println(rf.me, "apply snapshot :", rf.logSnapshot.Index, "with logs:", len(rf.logs))
	}

	// 自身当前最后接收到的日志
	last := 0
	if len(rf.logs) > 0 {
		last = rf.logs[len(rf.logs)-1].Index
	}
	// 如果: 最后的应用日志 < 当前提交日志 && 最后的提交日志未新的日志 --> 则 进行真正的应用状态机rf.lastApplied++
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied++ {
		//TODO: 这里为什么没有更新自己的rf.lastApplied??      更新了, 在rf.lastApplied++(for循环的条件里)
		index := rf.lastApplied
		// 应用状态机消息到外部系统(kvserver)
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index-rf.logSnapshot.Index].Log,
			CommandIndex: rf.logs[index-rf.logSnapshot.Index].Index,
		}
		rf.applyCh <- msg
	}
	// 如果上面提交了新的log apply  则打印日志..
	if rf.lastApplied > lastapplied {
		appliedIndex := rf.lastApplied - 1 - rf.logSnapshot.Index
		endIndex, endTerm := 0, 0
		if appliedIndex < 0 {
			endIndex = rf.logSnapshot.Index
			endTerm = rf.logSnapshot.Term
		} else {
			endTerm = rf.logs[rf.lastApplied-1-rf.logSnapshot.Index].Term
			endIndex = rf.logs[rf.lastApplied-1-rf.logSnapshot.Index].Index
		}
		rf.println(rf.me, "apply log", rf.lastApplied-1, endTerm, "-", endIndex, "/", last)
	}
}

//设置最后一次提交（用于乱序判定）
func (rf *Raft) setLastLog(req *AppendEntries) {
	rf.lock("Raft.setLastLog")
	defer rf.unlock("Raft.setLastLog")
	rf.lastLogs = *req
}

//判定乱序
func (rf *Raft) isOldRequest(req *AppendEntries) bool {
	rf.lock("Raft.isOldRequest")
	defer rf.unlock("Raft.isOldRequest")
	if req.Term == rf.lastLogs.Term && req.Me == rf.lastLogs.Me {
		lastIndex := rf.lastLogs.PrevLogIndex + rf.lastLogs.Snapshot.Index + len(rf.lastLogs.Entries)
		reqLastIndex := req.PrevLogIndex + req.Snapshot.Index + len(req.Entries)
		return lastIndex > reqLastIndex
	}
	return false
}

//重置竞选周期定时
func (rf *Raft) resetCandidateTimer() {
	// 随机超时
	randCnt := rf.randtime.Intn(250)
	duration := time.Duration(randCnt)*time.Millisecond + CandidateDuration
	rf.eletionTimer.Reset(duration)
}

func (rf *Raft) persist() {
	rf.lock("Raft.persist")
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastLogs)
	encoder.Encode(rf.logSnapshot.Index)
	encoder.Encode(rf.logSnapshot.Term)
	data := writer.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.unlock("Raft.persist")
	rf.persister.SaveStateAndSnapshot(data, rf.logSnapshot.Datas)
	//持久化数据后，apply 通知触发快照
	msg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: 0,
	}
	rf.applyCh <- msg
}

func (rf *Raft) readPersist(data []byte) {
	rf.lock("Raft.readPersist")
	if data == nil || len(data) < 1 {
		rf.unlock("Raft.readPersist")
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var commitIndex, currentTerm int
	var logs []LogEntry
	var lastlogs AppendEntries
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastlogs) != nil ||
		decoder.Decode(&rf.logSnapshot.Index) != nil ||
		decoder.Decode(&rf.logSnapshot.Term) != nil {
		rf.println("Error in unmarshal raft state")
	} else {

		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = 0
		rf.logs = logs
		rf.lastLogs = lastlogs
	}
	rf.unlock("Raft.readPersist")
	rf.logSnapshot.Datas = rf.persister.ReadSnapshot()
}

// 接收&处理 候选者发来的投票请求
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	reply.IsAgree = true
	// 将自身当前的term放入回包中.
	reply.CurrentTerm, _ = rf.GetState()
	//竞选任期小于等于自身任期，则反对票(发出选举之前一定会先自增term的)
	if reply.CurrentTerm >= req.ElectionTerm {
		rf.println(rf.me, "refuse", req.Me, "because of term")
		reply.IsAgree = false
		return
	}
	// 竞选任期大于自身任期，则更新自身任期，并转为follower
	// 无论是否最终同意选票,都会将自己term更新到更大
	rf.setStatus(Follower)
	rf.setTerm(req.ElectionTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	//判定竞选者日志是否新于自己
	if logterm > req.LogTerm {
		// 自己的日志大于竞选者的日志
		rf.println(rf.me, "refuse", req.Me, "because of logs's term")
		reply.IsAgree = false

		// 日志任期相同 继续比较index
	} else if logterm == req.LogTerm {
		// 如果自己的日志idx<=竞选着的idx, 任然同意该选票
		reply.IsAgree = logindex <= req.LogIndex
		if !reply.IsAgree {
			rf.println(rf.me, "refuse", req.Me, "because of logs's index")
		}
	}

	// logterm < req.LogTerm 自身最后的log term 小于候选者 也投统一票
	if reply.IsAgree {
		rf.println(rf.me, "agree", req.Me)
		//赞同票后重置选举定时，避免竞争
		rf.resetCandidateTimer()
	}
}

// 候选人自身发起选举.
func (rf *Raft) Vote() {
	//投票先增大自身任期, 且每个任期只能发出一次投票.
	rf.addTerm(1)
	rf.println("start vote :", rf.me, "term :", rf.currentTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	currentTerm, _ := rf.GetState()
	req := RequestVoteArgs{
		Me:           rf.me,
		ElectionTerm: currentTerm,
		LogTerm:      logterm,
		LogIndex:     logindex,
	}
	var wait sync.WaitGroup
	peercnt := len(rf.peers)
	wait.Add(peercnt)
	agreeVote := 0

	// 从投票的回复中得到的 集群中当前最大任期
	curMaxterm := currentTerm
	for i := 0; i < peercnt; i++ {
		//并行调用投票rpc，避免单点阻塞
		go func(index int) {
			defer wait.Done()
			resp := RequestVoteReply{false, -1}

			// 如果是自己, 则把票投给自己.
			if index == rf.me {
				agreeVote++
				return
			}
			// 向其他节点发起投票请求.
			rst := rf.sendRequestVote(index, &req, &resp)
			if !rst {
				return
			}
			// 如果对面节点同意将票投给自己 自己活得的投票数++
			if resp.IsAgree {
				agreeVote++
				return
			}
			// 更新目前集群中最大term
			if resp.CurrentTerm > curMaxterm {
				curMaxterm = resp.CurrentTerm
			}

		}(i)
	}
	wait.Wait()
	//如果集群中存在任期更大，则更新任期并转为follower
	//TODO: 这时如果网络分区, 单个节点收不到leader的心跳, 开始将自己变为候选者+term发起选举
	//这会导致该节点term高于集群中所有的节点, 这里为什么不校验投票的内容aggree(是否过半同意自己)? 直接就根据term放弃选举?
	//有一点, 这里只是放弃自己的选举流程, 但同时将term提升为当前最大term, 在下一次/后续选举中, 因为之前的被分区的节点log肯定不是最新, 所以不会当选为leader.
	//至于谁会当选为leader呢? 最终经过后续的选举轮次, 集群的term会逐步都提升为最大的term, 之后再通过比较log新旧来选出leader.
	if curMaxterm > currentTerm {
		rf.setTerm(curMaxterm)
		rf.setStatus(Follower)
	} else if agreeVote*2 > peercnt { //获得多数赞同则变成leader
		rf.println(rf.me, "become leader term:", currentTerm)
		rf.setStatus(Leader)
		rf.replicateLogNow()
	}
}

//选举定时器loop
func (rf *Raft) ElectionLoop() {
	//选举超时定时器
	rf.resetCandidateTimer()
	defer rf.eletionTimer.Stop()

	for !rf.isKilled {
		<-rf.eletionTimer.C
		if rf.isKilled {
			break
		}
		if rf.getStatus() == Candidate {
			//如果状态为竞选者，则直接发启选举
			rf.resetCandidateTimer()
			rf.Vote()
		} else if rf.getStatus() == Follower {
			//如果状态为fallow，则转变为candidata并发动选举
			rf.setStatus(Candidate)
			rf.resetCandidateTimer()
			rf.Vote()
		}
	}
	rf.println(rf.me, "Exit ElectionLoop")
}

// 接收&处理 日志复制请求
func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *AppendEntriesResp) {
	currentTerm, _ := rf.GetState()
	resp.Term = currentTerm
	resp.Successed = true
	if req.Term < currentTerm {
		//leader任期小于自身任期，则拒绝同步log
		resp.Successed = false
		return
	}
	//乱序日志，不处理
	if rf.isOldRequest(req) {
		return
	}
	//否则更新自身任期，切换自生为follow(一般情况本来就是follower)，重置选举定时器(因为受到了新的心跳)
	rf.resetCandidateTimer()
	rf.setTerm(req.Term)
	rf.setStatus(Follower)
	//获取自身当前最新的日志index
	_, logindex := rf.getLogTermAndIndex()
	//判定与leader发送过来的prelog是否一致
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > logindex {
			//没有该日志，则拒绝更新(自身日志不连续)
			rf.println(rf.me, "can't find preindex", req.PrevLogTerm)
			resp.Successed = false
			resp.LastApplied = rf.lastApplied
			return
		}
		if rf.getLogTermOfIndex(req.PrevLogIndex) != req.PrevLogTerm {
			//该索引与自身日志不同，则拒绝更新(自身日志连续, 但该索引日志term与leader不同)
			rf.println(rf.me, "term error", req.PrevLogTerm)
			resp.Successed = false
			resp.LastApplied = rf.lastApplied
			return
		}
	}
	//更新日志/心跳
	rf.setLastLog(req)
	if len(req.Entries) > 0 || req.Snapshot.Index > 0 {
		if len(req.Entries) > 0 {
			rf.println(rf.me, "update log from ", req.Me, ":", req.Entries[0].Term, "-", req.Entries[0].Index, "to", req.Entries[len(req.Entries)-1].Term, "-", req.Entries[len(req.Entries)-1].Index)
		}
		rf.updateLog(req.PrevLogIndex, req.Entries, &req.Snapshot)
	}
	// 更新follower的commitIndex为leader的.
	rf.setCommitIndex(req.LeaderCommit)
	// 尝试应用状态机.
	// TODO: 注意raft中没有明确的apply rpc
	// 而是leader先commit自己本地的日志, 然后立即触发心跳, 通过复制日志(心跳)来在下一次的交互中使得follower进行apply(如果条件为真)
	// 即本次apply() 如果跟随者自己commit log有更新(从leader那里获知的), 则也会进行自己的apply
	rf.apply()
	rf.persist()

	return
}

//复制日志给follower
func (rf *Raft) replicateLogTo(peer int) bool {
	replicateRst := false
	// 如果当前peer是自己 则直接返回
	if peer == rf.me {
		return false
	}
	isLoop := true
	for isLoop {
		isLoop = false
		// 检查当前自己是不是leader
		currentTerm, isLeader := rf.GetState()
		if !isLeader || rf.isKilled {
			break
		}
		// 构造出appendLog req/rsp
		req := rf.getAppendEntries(peer)
		resp := AppendEntriesResp{Term: 0}
		// 同步调用rpc, 进行日志复制
		rst := rf.sendAppendEnteries(peer, &req, &resp)
		// rpc返回
		currentTerm, isLeader = rf.GetState()
		if rst && isLeader {
			//如果某个节点任期大于自己，则更新任期，变成follower
			if resp.Term > currentTerm {
				rf.println(rf.me, "become follow ", peer, "term :", resp.Term)
				rf.setTerm(resp.Term)
				rf.setStatus(Follower)
			} else if !resp.Successed { //如果更新失败则更新follower日志next索引, 设置为该follower该同步到的地方.
				//rf.incNext(peer)
				rf.setNext(peer, resp.LastApplied+1)
				//继续下一次同步log
				isLoop = true
			} else { //更新成功
				//如果不是心跳, 而是真的log同步
				if len(req.Entries) > 0 {
					//更新leader的视角下各个follower已经提交的日志索引状态(matchIndex)
					rf.setNextAndMatch(peer, req.Entries[len(req.Entries)-1].Index)
					replicateRst = true
				} else if req.Snapshot.Index > 0 {
					rf.setNextAndMatch(peer, req.Snapshot.Index)
					replicateRst = true
				}
			}
		} else {
			isLoop = true
		}
	}
	return replicateRst
}

//立即复制日志
func (rf *Raft) replicateLogNow() {
	rf.lock("Raft.replicateLogNow")
	defer rf.unlock("Raft.replicateLogNow")
	for i := 0; i < len(rf.peers); i++ {
		// 重置心跳超时即可
		rf.heartbeatTimers[i].Reset(0)
	}
}

//心跳周期&复制日志loop
//(leader收到客户端的请求, 会将请求insert到logEntry中, 并立立即replicateLogNow()重置定时器, 触发复制日志)
func (rf *Raft) ReplicateLogLoop(peer int) {
	defer func() {
		rf.heartbeatTimers[peer].Stop()
	}()
	for !rf.isKilled {
		//定时器到时触发
		<-rf.heartbeatTimers[peer].C
		if rf.isKilled {
			break
		}
		rf.lock("Raft.ReplicateLogLoop")
		rf.heartbeatTimers[peer].Reset(HeartbeatDuration)
		rf.unlock("Raft.ReplicateLogLoop")
		_, isLeader := rf.GetState()
		// 如果是leader才会起实际作用 触发定时心跳ping集群中的follower/发送日志(接收到客户端的请求insert进的log)
		if isLeader {
			success := rf.replicateLogTo(peer)
			//如果复制日志成功
			//TODO: 不是需要复制给过半节点?
			//replicateLogTo()中已经更新machIndex
			//这里不是真正的apply, 而是尝试性apply, 通过更新commitLog, 判断过半才真正的apply
			if success {
				rf.apply()
				// apply之后立即触发下一次心跳, 因为有可能leader真正执行了apply, 需要立即通知follower们也进行apply
				rf.replicateLogNow()
				rf.persist()
			}
		}
	}
	rf.println(rf.me, "-", peer, "Exit ReplicateLogLoop")
}

//接收到客户状态机(kvserver)命令(只是leader将操作日志插入自己本地的LogEntry中)
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = 0
	term, isLeader = rf.GetState()
	if isLeader {
		//设置term并插入log
		index = rf.insertLog(command)
		rf.println("leader", rf.me, ":", "start append log", term, "-", index)
		// 立刻进行日志复制(leader同步日志到各个follower)
		rf.replicateLogNow()
	}
	return
}

func (rf *Raft) Kill() {
	rf.isKilled = true
	rf.eletionTimer.Reset(0)
	rf.replicateLogNow()
}

//保存快照
func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.lock("Raft.SaveSnapshot")
	if index > rf.logSnapshot.Index {
		//保存快照
		start := rf.logSnapshot.Index
		rf.logSnapshot.Index = index
		rf.logSnapshot.Datas = snapshot
		rf.logSnapshot.Term = rf.logs[index-start-1].Term
		//删除快照日志
		if len(rf.logs) > 0 {
			rf.logs = rf.logs[(index - start):]
		}
		rf.println("save snapshot :", rf.me, index, ",len logs:", len(rf.logs))
		rf.unlock("Raft.SaveSnapshot")
		rf.persist()
	} else {
		rf.unlock("Raft.SaveSnapshot")
	}

}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.isKilled = false
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.eletionTimer = time.NewTimer(CandidateDuration)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.setStatus(Follower)
	rf.EnableDebugLog = false
	rf.lastLogs = AppendEntries{
		Me:   -1,
		Term: -1,
	}
	rf.logSnapshot = LogSnapshot{
		Index: 0,
		Term:  0,
	}
	//日志同步协程
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		// 所有节点都创建该功能协程 但只有自身成为master后才真正起实际作用
		go rf.ReplicateLogLoop(i)
	}
	rf.readPersist(persister.ReadRaftState())
	rf.apply()
	raftOnce.Do(func() {
		//filename :=  "log"+time.Now().Format("2006-01-02 15_04_05") +".txt"
		//file, _ := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		//log.SetOutput(file)
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	})
	//Leader选举协程
	go rf.ElectionLoop()

	return rf
}
