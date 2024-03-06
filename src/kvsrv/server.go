package kvsrv

import (
	// "fmt"
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
	db       map[string]string
	dbch     chan *DBOper
	clientch chan *ClientReq
	clients  map[int64]Client
}

const (
	GET = iota
	PUT
	APPEND
)

// type DBOper int

type DBOper struct {
	op    int
	key   string
	value *string
	ch    chan *string
}

type ClientReq struct {
	id    int64
	rpcid int64
	op    int
	key   string
	value *string
	ch    chan struct{}
}

type Client struct {
	cache    string
	resultch chan *string
	rpcid    int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	req := ClientReq{
		id:    args.ClientID,
		rpcid: args.RPCID,
		op:    GET,
		key:   args.Key,
		ch:    make(chan struct{}),
	}
	kv.mu.Lock()
	kv.clientch <- &req
	kv.mu.Unlock()
	<-req.ch

	result := <-kv.clients[args.ClientID].resultch
	reply.Value = *result
	// Your code here.
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	req := ClientReq{
		id:    args.ClientID,
		rpcid: args.RPCID,
		op:    PUT,
		key:   args.Key,
		value: &args.Value,
		ch:    make(chan struct{}),
	}
	kv.mu.Lock()
	kv.clientch <- &req
	kv.mu.Unlock()
	<-req.ch

	result := <-kv.clients[args.ClientID].resultch
	reply.Value = *result

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	req := ClientReq{
		id:    args.ClientID,
		rpcid: args.RPCID,
		op:    APPEND,
		key:   args.Key,
		value: &args.Value,
		ch:    make(chan struct{}),
	}
	kv.mu.Lock()
	kv.clientch <- &req
	kv.mu.Unlock()
	<-req.ch

	result := <-kv.clients[args.ClientID].resultch
	reply.Value = *result
	DPrintf("reply.value is %s\n", reply.Value)

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	db := make(map[string]string, 10)
	kv.db = db
	dbchan := make(chan *DBOper)
	kv.dbch = dbchan

	kv.clientch = make(chan *ClientReq, 10)
	kv.clients = make(map[int64]Client, 10)

	go func() {
		// this routine handle db operation
		for {
			dbop := <-kv.dbch
			DPrintf("after dbch, %v", dbop)
			DPrintf("dboper is %d\n", dbop.op)
			switch dbop.op {
			case GET:
				v, ok := kv.db[dbop.key]
				if !ok {
					v = ""
				}
				dbop.ch <- &v
			case PUT:
				kv.db[dbop.key] = *dbop.value
				DPrintf("put dbop.ch is %v\n", dbop.ch)
				dbop.ch <- dbop.value
			case APPEND:
				v, ok := kv.db[dbop.key]
				if !ok {
					v = ""
				}
				kv.db[dbop.key] = v + *dbop.value
				dbop.ch <- &v
			}
		}
	}()

	go func() {
		for {
			client := <-kv.clientch
			DPrintf("after clientch received")
			c, ok := kv.clients[client.id]
			if !ok {
				c = Client{
					rpcid:    client.rpcid,
					resultch: make(chan *string),
				}
				kv.clients[client.id] = c
			}
			if client.rpcid != kv.clients[client.id].rpcid || !ok {
				d := DBOper{
					op:    client.op,
					key:   client.key,
					value: client.value,
					ch:    make(chan *string),
				}
				kv.dbch <- &d
				c.cache = *<-d.ch
			}

			client.ch <- struct{}{}
			// fmt.Println("c.cache is", c.cache, "c.resultch is ", c.resultch)
			c.resultch <- &c.cache
		}
	}()

	return kv
}
