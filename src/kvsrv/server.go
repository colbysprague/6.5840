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
	mu    sync.Mutex
	cache map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	kv.mu.Unlock()

	reply.Value = kv.get(args.Key)
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

// puts the value
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, val := args.Key, args.Value
	kv.set(key, val)
}

// puts and returns
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, val := args.Key, args.Value

	prev := kv.get(key)
	reply.Value = prev

	kv.set(key, prev+val)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.cache = make(map[string]string)

	return kv
}
