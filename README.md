# CMap
ConcurrentMap

### 简单介绍
> `cmap` 是首先分段锁的一个并发map, `Pair` 定义了映射, 使用`bucket` 实现了链表的操作
> 最后用`segment` 定义了多个`bucket`, 分段并发, 想要遍历也是很简单的， 了解她的基本
> `[ [ linkPair]bucket]Segment` 
