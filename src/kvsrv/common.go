package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key       string
	RequestId int64
	ClientId  int64
}

type GetReply struct {
	Value string
}

type Request struct {
	RequestId int64
	LastValue string
}
