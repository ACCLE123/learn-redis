### 考虑实现功能

1. rdb
2. epoll
3. another data struct
4. expired time
5. lua
6. del
7. graph

---

### zset

使用hreap

---

定义serialize obj接口 接口两个方法

1. marshal
2. serialize

----

定义comparable obj结构 实现方法

1. comparable

--- 

实现的方法

`zadd`
`zrange`
`zcard`
`zrem`

现在实现了
`zadd`
`zrange`
`zcard`
---
树

二叉搜索树:

1. 对于任意一个节点，该节点的值 大于 任意左子树节点的值，该节点的值 小于 任意右子树节点的值
2. 中序遍历为所有结点排序后的值

恢复二叉树:
1. 中序遍历+前序遍历
2. 中序遍历+后序遍历

前序遍历+后序遍历 无法恢复

---


### bug
