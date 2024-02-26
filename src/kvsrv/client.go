package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	// writeChan chan string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	// ch := make(chan string)
	// ck.writeChan = ch
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{
		Key: key,
		ID: nrand(),
		// ch: ck.writeChan,
	}

	reply := GetReply{}

	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		// fmt.Printf("--- args.ID is %d, key is %s\n", args.ID , args.Key)
		if !ok {
			fmt.Errorf("rpc call KVServer.Get failed")
		} else {
			break
		}
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key: key,
		Value: value,
		ID: nrand(),
		// ch: ck.writeChan,
	}
	reply := PutAppendReply{}

	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		// fmt.Printf("++++ op is %s, args.ID is %d, key is %s, value is %s\n", op, args.ID, args.Key, args.Value, )
		if !ok {
			fmt.Errorf("rpc call KVServer.%s failed", op)
		} else {
			break
		}
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
