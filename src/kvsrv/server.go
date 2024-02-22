package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.store[args.Key]
	kv.mu.Unlock()
	if !ok {
		v = ""
	}
	reply.Value = v
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	kv.store[args.Key] = args.Value
	kv.mu.Unlock()
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.store[args.Key]
	if !ok {
		v = ""
	}
	kv.store[args.Key] = v+args.Value
	kv.mu.Unlock()

	reply.Value = v
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	store := make(map[string]string, 10)
	kv.store = store

	return kv
}
