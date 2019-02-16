package cmap

import (
	"math"
	"sync/atomic"
)

type ConcurrentMap interface {
	// 并发的数量　
	Concurrency() int
	Put(key string, elem interface{}) (bool, error)
	Get(key string) interface{}
	Delete(key string) bool
	Len() uint64
}

type Cmap struct {
	concurrency int
	segments []Segment
	total uint64
}

func NewConcurrentMap(
	concurrency int, pairRedistributor PairRedistributor) (ConcurrentMap, error) {
	if concurrency <= 0 {
		return nil, newIllegalParameterError("concurrency is too small")
	}
	if concurrency > MAX_CONCURRENCY {
		return nil, newIllegalParameterError("concurrency is too large")
	}
	cmap := &Cmap{}
	cmap.concurrency = concurrency
	cmap.segments = make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		cmap.segments[i] = newSegment(DEFAULT_BUCKET_NUMBER, pairRedistributor)
	}
	return cmap, nil
}

func (cmap *Cmap) Concurrency() int {
	return cmap.concurrency
}

func (cmap *Cmap) Put(key string, elem interface{}) (bool, error) {
	p, err := newPair(key, elem)
	if err != nil {
		return false, err
	}
	s := cmap.findSegment(hash(key))
	ok, err := s.Put(p)
	if ok {
		atomic.AddUint64(&cmap.total, 1)
	}
	return ok, err
}

func (cmap *Cmap) Get(key string) interface{} {
	keyHash := hash(key)
	s := cmap.findSegment(keyHash)
	pair := s.GetWithHash(key, keyHash)
	if pair == nil {
		return nil
	}
	return pair.Element()
}

func (cmap *Cmap) Delete(key string) bool {
	// 删除一个元素，先找到segment，在定位bucket, 在删除
	s := cmap.findSegment(hash(key))
	if s.Delete(key) {
		atomic.AddUint64(&cmap.total, ^uint64(0))
		return true
	}
	return false
}

func (cmap *Cmap) Len() uint64 {
	return atomic.LoadUint64(&cmap.total)
}

func (cmap *Cmap) findSegment(keyHash uint64) Segment {
	if cmap.concurrency == 1 {
		return cmap.segments[0]
	}
	var keyHash32 uint32
	if keyHash > math.MaxInt32 {
		keyHash32 = uint32(keyHash32>>32)
	} else {
		keyHash32 = uint32(keyHash32)
	}
	return cmap.segments[int(keyHash32>>16) % (cmap.concurrency-1)]
}