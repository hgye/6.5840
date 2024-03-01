package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RPCID    int64
	ClientID int64
	// ch    chan string
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RPCID  int64
	ClientID int64
	// ch  chan string
}

type GetReply struct {
	Value string
}
