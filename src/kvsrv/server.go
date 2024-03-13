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
	dbch     chan DBOper
	// clientch chan ClientReq
	clients  map[int64]*Client
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
	value string
	ch    chan string
}

type ClientReq struct {
	id    int64
	rpcid int64
	// op    int
	// key   string
	// value *string
	ch    chan bool
}

type Client struct {
	cache    string
	// cachech  chan string
	// dbwtch   chan *string
	rpcid    int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	c, ok := kv.clients[args.ClientID]
	if !ok {
		c = &Client{
			rpcid: args.RPCID,
		}
		kv.clients[args.ClientID] = c
	}
	defer kv.mu.Unlock()
	if !ok || c.rpcid != args.RPCID {
		DPrintf("here we are")
		c.rpcid = args.RPCID
		d := DBOper{
			op: GET,
			key: args.Key,
			ch: make(chan string),
		}
		kv.dbch <- d
		result := <- d.ch
		c.cache = result
	}
	reply.Value = c.cache
	DPrintf("+++get reply.value is %s, args.RPCID is %d args.key %s \n", reply.Value, args.RPCID, args.Key)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	c, ok := kv.clients[args.ClientID]
	if !ok {
		c = &Client{
			rpcid: args.RPCID,
		}
		kv.clients[args.ClientID] = c
	}
	defer kv.mu.Unlock()
	if !ok || c.rpcid != args.RPCID {
		c.rpcid = args.RPCID
		d := DBOper{
			op: PUT,
			key: args.Key,
			value: args.Value,
			ch: make(chan string),
		}
		kv.dbch <- d
		result := <- d.ch
		c.cache = result
	}
	reply.Value = c.cache
	DPrintf("++++PUT reply.value is %s, args.RPCID is %d\n", reply.Value, args.RPCID)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	c, ok := kv.clients[args.ClientID]
	if !ok {
		c = &Client{
			rpcid: args.RPCID,
		}
		kv.clients[args.ClientID] = c
	}
	defer kv.mu.Unlock()
	if !ok || c.rpcid != args.RPCID {
		c.rpcid = args.RPCID
		d := DBOper{
			op: APPEND,
			key: args.Key,
			value: args.Value,
			ch: make(chan string),
		}
		kv.dbch <- d
		result := <- d.ch
		c.cache = result

		DPrintf("++++c.cache is %s, args rpcid is %d, args.Key(%s) \n", c.cache, args.RPCID, args.Key)
	}

	reply.Value = c.cache

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	db := make(map[string]string, 10)
	kv.db = db
	dbchan := make(chan DBOper)
	kv.dbch = dbchan

	// kv.clientch = make(chan ClientReq)
	kv.clients = make(map[int64]*Client, 10)

	go func() {
		// this routine handle db operation
		for {
			dbop := <-kv.dbch
			// DPrintf("after dbch, %v", dbop)
			// DPrintf("dboper is %d\n", dbop.op)
			switch dbop.op {
			case GET:
				v, ok := kv.db[dbop.key]
				if !ok {
					v = ""
				}
				dbop.ch <- v
			case PUT:
				kv.db[dbop.key] = dbop.value
				// DPrintf("put dbop.ch is %v\n", dbop.ch)
				dbop.ch <- ""
			case APPEND:
				v, ok := kv.db[dbop.key]
				if !ok {
					v = ""
				}
				kv.db[dbop.key] = v + dbop.value
				dbop.ch <- v
				DPrintf("====db append, old value is %v, new value is %v\n", v, dbop.value)
			}
		}
	}()

	// go func() {
	// 	for {
	// 		client := <-kv.clientch
	// 		DPrintf("after clientch received")
	// 		c, ok := kv.clients[client.id]
	// 		if !ok {
	// 			c = &Client{
	// 				rpcid:    client.rpcid,
	// 				cachech: make(chan string),
	// 			}
	// 			kv.mu.Lock()
	// 			kv.clients[client.id] = c
	// 			kv.mu.Unlock()
	// 			client.ch <- true
	// 		} else if client.rpcid != c.rpcid {
	// 			kv.clients[client.id].rpcid = client.rpcid
	// 			client.ch <- true
	// 		} else {
	// 			client.ch <- false
	// 		}
	// 		// fmt.Println("client.rpcid is ", client.rpcid, "old rpcid is ", c.rpcid,
	// 		// "ok is ", ok, "c.cache is ", c.cache)
	// 		// fmt.Println("+++cond is ", client.rpcid != c.rpcid || !ok, "c.cache is ", c.cache )
	// 		// if client.rpcid == c.rpcid {
	// 		// 	DPrintf("!!!! here we go")
	// 		// }
	// 		// if client.rpcid != c.rpcid || !ok {
	// 		// 	c.rpcid = client.rpcid
	// 		// 	d := DBOper{
	// 		// 		op:    client.op,
	// 		// 		key:   client.key,
	// 		// 		value: client.value,
	// 		// 		ch:    make(chan string),
	// 		// 	}
	// 		// 	kv.dbrdch <- d
	// 		// 	c.cache = <-d.ch
	// 		// }

	// 		// client.ch <- struct{}{}
	// 		// c.cachech <- c.cache
	// 		// fmt.Println("---c.cache is", c.cache)
	// 	}
	// }()

	return kv
}
