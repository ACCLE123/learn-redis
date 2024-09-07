package main

import (
	"fmt"
	"strconv"
	"sync"
)

var Handler = map[string]func([]Value) Value{
	"PING":   ping,
	"SET":    set,
	"GET":    get,
	"HSET":   hset,
	"HGET":   hget,
	"SAVE":   save,
	"DEL":    del,
	"ZCARD":  zcard,
	"ZADD":   zadd,
	"ZRANGE": zrange,
}

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

type ZSET struct {
	treap    *Treap
	elements map[string]*TreapNode
	mu       sync.RWMutex
}

var ZSETsMu sync.RWMutex

var ZSETs = map[string]*ZSET{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "set wrong number of arguments"}
	}
	key := args[0].bulk
	value := args[1].bulk

	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()
	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "get wrong number of arguments"}
	}
	key := args[0].bulk

	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "bulk", bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "set wrong number of arguments"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()
	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "get wrong number of arguments"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}
	return Value{typ: "bulk", bulk: value}
}

func save(args []Value) Value {
	if len(args) != 0 {
		return Value{typ: "error", str: "save wrong number of arguments"}
	}

	rdb, err := NewRdb("database.rdb")
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}
	err = rdb.Save()
	if err != nil {
		return Value{typ: "error", str: err.Error()}
	}

	return Value{typ: "string", str: "OK"}
}

func del(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "del wrong number of arguments"}
	}
	key := args[0].bulk

	HSETsMu.Lock()
	delete(HSETs, key)
	HSETsMu.Unlock()

	SETsMu.Lock()
	delete(SETs, key)
	SETsMu.Unlock()

	return Value{typ: "string", str: "ok"}
}

func zadd(args []Value) Value {
	n := len(args)
	if n < 2 || n%2 == 0 {
		return Value{typ: "error", str: "zadd wrong number of arguments"}
	}
	key := args[0].bulk

	ZSETsMu.Lock()
	zset, exists := ZSETs[key]
	if !exists {
		zset = &ZSET{treap: NewTreap(), elements: make(map[string]*TreapNode)}
		ZSETs[key] = zset
	}
	ZSETsMu.Unlock()

	zset.mu.Lock()
	for i := 1; i < n; i += 2 {
		score := args[i].num
		value := args[i+1].bulk
		if node, exists := zset.elements[value]; exists {
			zset.treap.Erase(node.key, value)
		}
		node, ok := zset.treap.Insert(score, value)
		if ok {
			zset.elements[value] = node
		}
	}
	zset.mu.Unlock()

	return Value{typ: "integer", num: (n - 1) / 2}
}

func zrange(args []Value) Value {
	n := len(args)
	if n != 3 {
		return Value{typ: "error", str: "zrange wrong number of arguments"}
	}
	key := args[0].bulk
	begin, _ := strconv.ParseInt(args[1].bulk, 10, 64)
	end, _ := strconv.ParseInt(args[2].bulk, 10, 64)

	ZSETsMu.RLock()
	zset, exists := ZSETs[key]
	if !exists {
		ZSETsMu.RUnlock()
		return Value{typ: "error", str: "zrange wrong"}
	}
	ZSETsMu.RUnlock()

	zset.mu.RLock()

	size := int64(zset.treap.size)
	begin = (begin%size + size) % size
	end = (end%size + size) % size
	if begin > end {
		zset.mu.RLock()
		return Value{typ: "error", str: "zrange wrong"}
	}
	res := Value{typ: "array", array: make([]Value, 0)}
	for i := begin + 1; i <= end+1; i++ {
		res.array = append(res.array, Value{typ: "bulk", bulk: fmt.Sprintf("%s", zset.treap.GetNodeByRank(int(i)).value)})
	}

	zset.mu.RUnlock()

	return res
}

func zrem(args []Value) Value {
	return Value{}
}

func zcard(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "zard wrong number of arguments"}
	}
	key := args[0].bulk
	zset := ZSETs[key]

	return Value{typ: "integer", num: zset.treap.size}
}
