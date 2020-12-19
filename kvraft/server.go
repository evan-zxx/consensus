package raftkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"ximalaya.com/MIT-6.824-2018/src/labgob"
	"ximalaya.com/MIT-6.824-2018/src/labrpc"
	"ximalaya.com/MIT-6.824-2018/src/raft"
)

type Op struct {
	Ch  chan (interface{})
	Req interface{}
}

var kvOnce sync.Once

type KVServer struct {
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	applyCh        chan raft.ApplyMsg
	maxraftstate   int // snapshot if log grows this big
	kvs            map[string]string
	msgIDs         map[int64]int64 //缓存收到各个客户端最大消息id 实现幂等性(消息去重)
	killChan       chan (bool)
	persister      *raft.Persister
	logApplyIndex  int //日志索引 用于创建最新快照
	EnableDebugLog bool
}

func (kv *KVServer) println(args ...interface{}) {
	if kv.EnableDebugLog {
		log.Println(args...)
	}
}

//raft操作
func (kv *KVServer) opt(client int64, msgId int64, req interface{}) (bool, interface{}) {
	//去重
	if msgId > 0 && kv.isRepeated(client, msgId, false) {
		return true, nil
	}
	op := Op{
		Req: req,                      //请求数据
		Ch:  make(chan (interface{})), //日志提交chan
	}
	//写入Raft
	_, _, isLeader := kv.rf.Start(op)
	//判定是否是leader, 如果不是leader, 则立刻返回
	if !isLeader {
		return false, nil
	}
	if msgId != -1 {
		kv.println("leader kvserver:", kv.me, "put id", msgId)
	} else {
		kv.println("leader kvserver:", kv.me, "get")
	}

	//自己是kvserver主节点, 对应raft也就是leader
	//如果将指令提交到了raft leader, 则等待raft状态机apply后回调kvserver处理请求
	//注意: 有可能自己作为主节点提交log到raft后, 自身状态变为了非主
	select {
	case resp := <-op.Ch:
		return true, resp
	case <-time.After(time.Millisecond * 1000): //超时
		kv.println(kv.me, "timeout for client:", client)
	}
	return false, nil
}

//读请求
func (kv *KVServer) Get(req *GetArgs, reply *GetReply) {
	ok, value := kv.opt(-1, -1, *req)
	reply.WrongLeader = !ok
	if ok {
		reply.Value = value.(string)
	}
}

//写请求
func (kv *KVServer) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	ok, _ := kv.opt(req.Me, req.MsgId, *req)
	reply.WrongLeader = !ok
}

//写操作
func (kv *KVServer) putAppend(req *PutAppendArgs) {
	kv.println(req.Me, "-", kv.me, "on", req.Op, "id:", req.MsgId, "[", req.Key, ":", req.Value, "]")
	if req.Op == "Put" {
		kv.kvs[req.Key] = req.Value
	} else if req.Op == "Append" {
		value, ok := kv.kvs[req.Key]
		if !ok {
			value = ""
		}
		value += req.Value
		kv.kvs[req.Key] = value
	}
}

//读操作
func (kv *KVServer) get(args *GetArgs) (value string) {
	value, ok := kv.kvs[args.Key]
	if !ok {
		value = ""
	}
	kv.println(kv.me, "on get", "[", args.Key, ":", value, "]")
	return
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.killChan <- true
}

//判定重复请求
func (kv *KVServer) isRepeated(client int64, msgId int64, update bool) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := false
	index, ok := kv.msgIDs[client]
	if ok {
		//判断msgIDs中记录的消息是否大于 新传入的消息
		rst = index >= msgId
	}
	if update && !rst {
		kv.msgIDs[client] = msgId
	}
	return rst
}

//判定是否写入快照
func (kv *KVServer) ifSaveSnapshot() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)
		encoder.Encode(kv.msgIDs)
		encoder.Encode(kv.kvs)
		data := writer.Bytes()
		kv.rf.SaveSnapshot(kv.logApplyIndex, data)
		return
	}
}

//更新快照
func (kv *KVServer) updateSnapshot(index int, data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	kv.logApplyIndex = index
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	if decoder.Decode(&kv.msgIDs) != nil ||
		decoder.Decode(&kv.kvs) != nil {
		kv.println("Error in unmarshal raft state")
	}
}

//raft状态机apply后的回调
//注意: 每个server对应的raft实例的状态机apply后都会执行此回调
//客户端在此回调中执行对应的get/put请求
func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid { //非状态机apply消息
		//更新快照消息
		if command, ok := applyMsg.Command.(raft.LogSnapshot); ok {
			kv.updateSnapshot(command.Index, command.Datas)
		}
		kv.ifSaveSnapshot()
		return
	}
	//更新日志索引，用于创建最新快照
	kv.logApplyIndex = applyMsg.CommandIndex
	opt := applyMsg.Command.(Op)
	var resp interface{}

	//Put && append操作
	if command, ok := opt.Req.(PutAppendArgs); ok {
		//去重复(幂等性在server端实现?)
		//如果发现是重复消息, 则不进行处理
		if !kv.isRepeated(command.Me, command.MsgId, true) {
			//非重复消息, 进行处理
			kv.putAppend(&command)
		}
		resp = true
	} else { //Get操作
		command := opt.Req.(GetArgs)
		resp = kv.get(&command)
	}
	select {
	case opt.Ch <- resp:
	default:
	}
	kv.ifSaveSnapshot()
}

//轮询
func (kv *KVServer) mainLoop() {
	for {
		select {
		case <-kv.killChan:
			return
			//收到了raft状态机apply的消息
			//注意: 这里被唤醒的kvserver是随机的
		case msg := <-kv.applyCh:
			if cap(kv.applyCh)-len(kv.applyCh) < 5 {
				log.Println("warn : maybe dead lock...")
			}
			kv.onApply(msg)
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.kvs = make(map[string]string)
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.persister = persister
	kv.EnableDebugLog = true
	kv.logApplyIndex = 0
	kvOnce.Do(func() {
		labgob.Register(Op{})
		labgob.Register(PutAppendArgs{})
		labgob.Register(GetArgs{})
	})
	go kv.mainLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.EnableDebugLog = false
	return kv
}
