package cmap

import "sync/atomic"

type BucketStatus uint8

const (
	//散列桶正常
	BUCKET_STATUS_NORMAL BucketStatus = 0
	// 散列桶过轻
	BUCKET_STATUS_UNDERWEIGHT BucketStatus = 1
	// 散列桶过重
	BUCKET_STATUS_OVERWEIGHT BucketStatus = 2
)

type PairRedistributor interface {
	UpdateThreshold(pairTotal uint64, bucketNumber int)
	CheckBucketStatus(pairTotal uint64, bucketSize uint64)(bucketStatus BucketStatus)
	Redistribe(bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool)
}

type myRedistributor struct {
	// 装在因子
	loadFactor float64
	// upperThreshold代表散列桶的数量上限
	upperThreshold uint64
	// 过重的散列桶的计数
	overweightBucketCount uint64
	emptyBucketCount uint64
}

func newDefautlPairRedistributor(loadFactor float64, bucketNumber int) PairRedistributor {
	if loadFactor < 0 {
		loadFactor = DEFAULT_BUCKET_LOAD_FACTOR
	}
	pr := &myRedistributor{}
	pr.loadFactor = loadFactor
	//
	return pr
}

var bucketCountTemplate = `Bucket count:
	pairTotal: %d
	bucketNumber: %d
	average: %f
	upperThreshold: %d
	emptyBucketCount: %d
`
var bucketStatusTemplate = `Check bucket status:
	pairTotal: %d
	bucketSize: %d
	upperThreshold: %d
	overweightBucketCount: %d
	emptyBucketCount: %d
	bucketStatus: %d
`
var redistributionTemplate = `Redistributing: 
    bucketStatus: %d
    currentNumber: %d
    newNumber: %d
`

func (pr *myRedistributor) UpdateThreshold(pairTotal uint64, bucketNumber int) {
	var average float64
	average = float64(pairTotal / uint64(bucketNumber))
	if average < 100 {
		average = 100
	}
	atomic.StoreUint64(&pr.upperThreshold, uint64(average*pr.loadFactor))
}

func (pr *myRedistributor) CheckBucketStatus(pairTotal uint64,
	bucketSize uint64) (bucketStatus BucketStatus) {
	if bucketSize > DEFAULT_BUCKET_MAX_SIZE ||
		bucketSize >= atomic.LoadUint64(&pr.upperThreshold) {
			atomic.AddUint64(&pr.overweightBucketCount, 1)
			bucketStatus = BUCKET_STATUS_OVERWEIGHT
			return
	}
	if bucketSize == 0 {
		atomic.AddUint64(&pr.emptyBucketCount, 1)
	}
	return
}

func (pr *myRedistributor) Redistribe(bucketStatus BucketStatus,
	buckets []Bucket) (newBuckets []Bucket, changed bool) {
		currentNumber := uint64(len(buckets))
		newNumber := currentNumber
		switch bucketStatus{
		case BUCKET_STATUS_OVERWEIGHT:
			if atomic.LoadUint64(&pr.overweightBucketCount) * 4 < currentNumber {
				return nil, false
			}
			newNumber = currentNumber << 1
		case BUCKET_STATUS_UNDERWEIGHT:
			if currentNumber < 100 ||
				atomic.LoadUint64(&pr.emptyBucketCount) * 4 < currentNumber {
					return nil, false
			}
			newNumber = currentNumber >> 1
			if newNumber < 2 {
				newNumber = 2
			}
		default:
			return nil, false
		}
		if newNumber == currentNumber {
			atomic.StoreUint64(&pr.overweightBucketCount, 0)
			atomic.StoreUint64(&pr.emptyBucketCount, 0)
			return  nil, false
		}
		var pairs []Pair
		for _, b := range buckets {
			for e := b.GetFirstPair(); e != nil ; e = e.Next() {
				pairs = append(pairs, e)
			}
		}
		if newNumber > currentNumber {
			for i := uint64(0); i < currentNumber; i ++ {
				buckets[i].Clear(nil)
			}
			for j := newNumber-currentNumber; j > 0; j-- {
				buckets = append(buckets, newBucket())
			}
		} else {
			buckets = make([]Bucket, newNumber)
			for i := uint64(0); i < newNumber; i ++{
				buckets[i] = newBucket()
			}
		}
		var count int
		for _, p := range pairs {
			index := int(p.Hash() % newNumber)
			b := buckets[index]
			b.Put(p, nil)
			count ++
		}
		atomic.StoreUint64(&pr.overweightBucketCount, 0)
		atomic.StoreUint64(&pr.emptyBucketCount, 0)
		return buckets, true
}