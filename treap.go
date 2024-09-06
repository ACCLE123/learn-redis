package main

import (
	"math"
	"math/rand"
)

type Treap struct {
	key, val, cnt, size int
	value               string
	l, r                *Treap
}

func (t *Treap) pushUp() {
	t.size = t.cnt
	if t.l != nil {
		t.size += t.l.size
	}
	if t.r != nil {
		t.size += t.r.size
	}
}

func getNode(key int, value string) *Treap {
	node := &Treap{key: key, val: rand.Int(), cnt: 1, size: 1, value: value}
	return node
}

func zag(p **Treap) {
	if p == nil || *p == nil || (*p).r == nil {
		return
	}
	q := (*p).r
	(*p).r = q.l
	q.l = *p
	*p = q
	if (*p).r != nil {
		(*p).r.pushUp()
	}
	(*p).pushUp()
}

func zig(p **Treap) {
	if p == nil || *p == nil || (*p).l == nil {
		return
	}
	q := (*p).l
	(*p).l = q.r
	q.r = *p
	*p = q
	if (*p).l != nil {
		(*p).l.pushUp()
	}
	(*p).pushUp()
}

func NewTreap() *Treap {
	root := getNode(-math.MaxInt, "")
	node := getNode(math.MaxInt, "")
	root.r = node
	if root.val < node.val {
		zag(&root)
	}
	return root
}

func Insert(u **Treap, key int, value string) (node *Treap) {
	if *u == nil {
		*u = getNode(key, value)
		return *u
	}
	if (*u).key == key {
		(*u).cnt++
	} else if (*u).key < key {
		Insert(&(*u).r, key, value)
		if (*u).r.val > (*u).val {
			zag(u)
		}
	} else {
		Insert(&(*u).l, key, value)
		if (*u).l.val > (*u).val {
			zig(u)
		}
	}
	(*u).pushUp()
	return *u
}

func Delete(u **Treap, key int) {
	if *u == nil {
		return
	}
	if (*u).key == key {
		if (*u).cnt > 1 {
			(*u).cnt--
		} else if (*u).l != nil || (*u).r != nil {
			if (*u).r == nil || (*u).l != nil && (*u).l.val > (*u).r.val {
				zig(u)
				Delete(&(*u).r, key)
			} else {
				zag(u)
				Delete(&(*u).l, key)
			}
		} else {
			*u = nil
		}
	} else if (*u).key < key {
		Delete(&(*u).r, key)
	} else {
		Delete(&(*u).l, key)
	}

	if *u != nil {
		(*u).pushUp()
	}
}

func GetPrev(u **Treap, key int) int {
	if *u == nil {
		return -math.MaxInt
	}
	if (*u).key >= key {
		return GetPrev(&(*u).l, key)
	} else {
		return max((*u).key, GetPrev(&(*u).r, key))
	}
}

func GetNext(u **Treap, key int) int {
	if *u == nil {
		return math.MaxInt
	}
	if (*u).key <= key {
		return GetNext(&(*u).r, key)
	} else {
		return min((*u).key, GetNext(&(*u).l, key))
	}
}

// TODO bug
func GetRankByKey(u **Treap, key int) int {
	return getRankByKey(u, key) - 1
}

func getRankByKey(u **Treap, key int) int {
	if *u == nil {
		return -math.MaxInt
	}
	lsize := 0
	if (*u).l != nil {
		lsize = (*u).l.size
	}
	if (*u).key == key {
		return lsize + 1
	} else if (*u).key > key {
		return getRankByKey(&(*u).l, key)
	} else {
		return lsize + (*u).cnt + getRankByKey(&(*u).r, key)
	}
}

func GetKeyByRank(u **Treap, rank int) int {
	return getKeyByRank(u, rank+1)
}

func getKeyByRank(u **Treap, rank int) int {
	if *u == nil {
		return math.MaxInt
	}
	lsize := 0
	if (*u).l != nil {
		lsize = (*u).l.size
	}

	if lsize >= rank {
		return getKeyByRank(&(*u).l, rank)
	} else if lsize+(*u).cnt >= rank {
		return (*u).key
	} else {
		return getKeyByRank(&(*u).r, rank-(*u).cnt-lsize)
	}
}
