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
	mu       sync.Mutex
	cache    map[string]string
	lastSeen map[int64]Request
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.get(args.Key)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	lastVal, seen := kv.withDedup(args.ClientId, args.RequestId, func() string {
		key, val := args.Key, args.Value

		prev := kv.get(key)
		reply.Value = prev

		kv.set(key, prev+val)

		return prev
	})

	if seen {
		reply.Value = lastVal
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.withDedup(args.ClientId, args.RequestId, func() string {
		key, val := args.Key, args.Value
		kv.set(key, val)
		return "" // don't store a lastValue
	})
}

func (kv *KVServer) markSeen(clientId int64, reqId int64, val string) {
	kv.lastSeen[clientId] = Request{
		RequestId: reqId,
		LastValue: val,
	}
}

func (kv *KVServer) haveSeen(clientId int64, reqId int64) (string, bool) {
	got, ok := kv.lastSeen[clientId]
	if !ok || got.RequestId != reqId {
		return "", false
	}

	return got.LastValue, ok
}

func (kv *KVServer) get(key string) string {
	val, ok := kv.cache[key]
	if ok {
		return val
	}

	return ""
}

func (kv *KVServer) set(key string, val string) {
	kv.cache[key] = val
}

// handles the lock on the clerk, as well as eliminating runnign duplicated requests for a client
func (kv *KVServer) withDedup(clientId int64, requestId int64, f func() string) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastVal, seen := kv.haveSeen(clientId, requestId)
	if seen {
		return lastVal, seen
	}
	val := f()
	kv.markSeen(clientId, requestId, val)
	return "", false
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.cache = make(map[string]string)
	kv.lastSeen = make(map[int64]Request)

	return kv
}
