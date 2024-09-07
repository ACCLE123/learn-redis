package main

import (
	"fmt"
	"math/rand"
)

type TreapNode struct {
	key, priority, size int
	value               string
	l, r                *TreapNode
}

type Treap struct {
	root *TreapNode
	size int
}

func NewTreapNode(key int, value string) *TreapNode {
	return &TreapNode{key: key, value: value, priority: rand.Int(), size: 1}
}

func NewTreap() *Treap {
	return &Treap{}
}

func pushUp(u *TreapNode) {
	var lsize, rsize int
	if u.l != nil {
		lsize = u.l.size
	}
	if u.r != nil {
		rsize = u.r.size
	}
	u.size = lsize + rsize + 1
}

func zig(p **TreapNode) {
	if (*p) == nil || (*p).l == nil {
		return
	}
	q := (*p).l
	(*p).l = q.r
	q.r = *p
	*p = q
	pushUp((*p).r)
	pushUp(*p)
}

func zag(p **TreapNode) {
	if (*p) == nil || (*p).r == nil {
		return
	}
	q := (*p).r
	(*p).r = q.l
	q.l = *p
	*p = q
	pushUp((*p).l)
	pushUp(*p)
}

func (t *Treap) Insert(key int, value string) (*TreapNode, bool) {
	node, ok := insert(&t.root, key, value)
	if ok {
		t.size++
		return node, ok
	}
	return nil, false
}

func insert(u **TreapNode, key int, value string) (*TreapNode, bool) {
	if *u == nil {
		*u = NewTreapNode(key, value)
		return *u, true
	}
	if (*u).key > key || ((*u).key == key && (*u).value > value) {
		node, ok := insert(&(*u).l, key, value)
		if (*u).l.priority > (*u).priority {
			zig(u)
		}
		pushUp(*u)
		return node, ok
	} else if (*u).key < key || ((*u).key == key && (*u).value < value) {
		node, ok := insert(&(*u).r, key, value)
		if (*u).r.priority > (*u).priority {
			zag(u)
		}
		pushUp(*u)
		return node, ok
	} else {
		pushUp(*u)
		return nil, false
	}
}

func (t *Treap) Erase(key int, value string) {
	t.size--
	erase(&t.root, key, value)
}

func erase(u **TreapNode, key int, value string) bool {
	if *u == nil {
		return false
	}
	if (*u).key > key || (*u).key == key && (*u).value > value {
		ok := erase(&(*u).l, key, value)
		pushUp(*u)
		return ok
	} else if (*u).key < key || (*u).key == key && (*u).value < value {
		ok := erase(&(*u).r, key, value)
		pushUp(*u)
		return ok
	} else {
		if (*u).r != nil || (*u).l != nil {
			if (*u).r == nil || (*u).l != nil && (*u).l.priority > (*u).r.priority {
				zig(u)
				ok := erase(&(*u).r, key, value)
				pushUp(*u)
				return ok
			} else {
				zag(u)
				ok := erase(&(*u).l, key, value)
				pushUp(*u)
				return ok
			}
		} else {
			*(u) = nil
			return true
		}
	}
}

func (t *Treap) GetNodeByRank(rank int) *TreapNode {
	if rank < 0 || rank > t.size {
		return nil
	}
	return getNodeByRank(t.root, rank)
}

func getNodeByRank(u *TreapNode, rank int) *TreapNode {
	var lsize int
	if u.l != nil {
		lsize = u.l.size
	}

	if rank <= lsize {
		return getNodeByRank(u.l, rank)
	} else if rank <= lsize+1 {
		return u
	} else {
		return getNodeByRank(u.r, rank-1-lsize)
	}
}

func (t *Treap) Bfs() {
	if t.root == nil {
		return
	}

	currentLevel := []*TreapNode{t.root}
	nextLevel := []*TreapNode{}

	for len(currentLevel) > 0 {
		for _, node := range currentLevel {
			if node == nil {
				//fmt.Printf("key: %d, value: %s, priority: %d, size: %d  ", 0, "", 0, 0)
				fmt.Printf("[%d]", 0)
			} else {
				//fmt.Printf("key: %d, value: %s, priority: %d, size: %d  ", node.key, node.value, node.priority, node.size)
				fmt.Printf("[%d]", node.key)
				nextLevel = append(nextLevel, node.l)
				nextLevel = append(nextLevel, node.r)
			}
		}
		fmt.Println()

		currentLevel = nextLevel
		nextLevel = []*TreapNode{}
	}
}

func (t *Treap) Inorder() {
	inorder(t.root)
}

func inorder(p *TreapNode) {
	if p == nil {
		return
	}
	inorder(p.l)
	fmt.Printf("key: %d, value: %s\n", p.key, p.value)
	inorder(p.r)
}
