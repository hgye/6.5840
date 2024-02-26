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
	db map[string]string
	dbchan chan clientOper
	cache map[int64]string
}

const (
	GET = iota
	PUT
	APPEND
)

type DBOper int

type clientOper struct {
	dboper DBOper
	getargs *GetArgs
	putappendargs *PutAppendArgs
	ch chan string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("+++, whatever")
	kv.mu.Lock()
	v, ok := kv.cache[args.ID]
	kv.mu.Unlock()
	if !ok {
		DPrintf("before  kv.dbchan")
		ch := make(chan string)
		kv.dbchan <- clientOper{
			dboper: GET,
			getargs: args,
			ch: ch,
		}
		DPrintf("before get args.ch")
		v = <- ch
		DPrintf("after get args.ch")
	}
	reply.Value = v
	// fmt.Println("reply.Value is", v)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.cache[args.ID]
	kv.mu.Unlock()
	if !ok {
		ch := make(chan string)
		kv.dbchan <- clientOper{
			dboper: PUT,
			putappendargs: args,
			ch: ch,
		}

		v = <- ch
	}
	reply.Value = v
	// kv.db[args.Key] = args.Value
	// kv.mu.Unlock()
	// reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.cache[args.ID]
	kv.mu.Unlock()
	if !ok {
		ch := make(chan string)
		kv.dbchan <- clientOper{
			dboper: APPEND,
			putappendargs: args,
			ch: ch,
		}
		v = <- ch
	}
	reply.Value = v
	// fmt.Printf("reply.value is %s\n", reply.Value)

	// v, ok := kv.db[args.Key]
	// if !ok {
	// 	v = ""
	// }
	// kv.db[args.Key] = v+args.Value
	// kv.mu.Unlock()

	// reply.Value = v
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	db := make(map[string]string, 10)
	kv.db = db

	cache := make(map[int64]string, 10)
	kv.cache = cache

	dbchan := make(chan clientOper)
	kv.dbchan = dbchan



	go func() {
		for {
			DPrintf("before dbchan")
			clientoper := <-kv.dbchan
			DPrintf("after dbch, %v", clientoper)
			DPrintf("dboper is %d\n", clientoper.dboper)
			switch clientoper.dboper {
			case GET:
				v, ok := kv.db[clientoper.getargs.Key]
				if !ok {v=""}
				kv.mu.Lock()
				kv.cache[clientoper.getargs.ID] = v
				kv.mu.Unlock()
				clientoper.ch <- v
			case PUT:
				kv.db[clientoper.putappendargs.Key] = clientoper.putappendargs.Value
				kv.mu.Lock()
				kv.cache[clientoper.putappendargs.ID] = clientoper.putappendargs.Value
				kv.mu.Unlock()
				clientoper.ch <- clientoper.putappendargs.Value
			case APPEND:
				v, ok := kv.db[clientoper.putappendargs.Key]
				if !ok {v=""}
				kv.db[clientoper.putappendargs.Key] = v+clientoper.putappendargs.Value
				kv.mu.Lock()
				kv.cache[clientoper.putappendargs.ID] = v
				kv.mu.Unlock()
				clientoper.ch <- v

			}

		}
	}()

	return kv
}
