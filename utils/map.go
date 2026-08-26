package utils

import (
	"math/bits"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash"
)

type ShardMap[K comparable, V any] struct {
	mapGroup []*ConcurrentMap[K, V]
	hashFunc func(k K) uint64
	mask     uint64
}

func NewShardMap[K comparable, V any](bucketMaxNum uint64, hashFunc func(k K) uint64) *ShardMap[K, V] {
	if bucketMaxNum == 0 {
		return nil
	}
	// 不超过n的最大2^k-1数
	mask := uint64((1 << bits.Len(uint(bucketMaxNum))) - 1)
	if mask > bucketMaxNum {
		mask >>= 1
	}

	sm := ShardMap[K, V]{make([]*ConcurrentMap[K, V], mask+1), hashFunc, mask}
	for i := range sm.mapGroup {
		sm.mapGroup[i] = NewConcurrentMap[K, V]()
	}

	return &sm
}

func (sm *ShardMap[K, V]) Get(k K) (V, bool) {
	return sm.mapGroup[sm.hashFunc(k)&sm.mask].Get(k)
}

func (sm *ShardMap[K, V]) Set(k K, v V) {
	sm.mapGroup[sm.hashFunc(k)&sm.mask].Set(k, v)
}

func (sm *ShardMap[K, V]) Delete(k K) {
	sm.mapGroup[sm.hashFunc(k)&sm.mask].Delete(k)
}

type ConcurrentMap[K comparable, V any] struct {
	rwMu sync.RWMutex
	m    map[K]V
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{
		m: make(map[K]V),
	}
}

func (cm *ConcurrentMap[K, V]) Get(k K) (V, bool) {
	cm.rwMu.RLock()
	defer cm.rwMu.RUnlock()
	v, ok := cm.m[k]
	return v, ok
}

func (cm *ConcurrentMap[K, V]) Set(k K, v V) {
	cm.rwMu.Lock()
	defer cm.rwMu.Unlock()
	cm.m[k] = v
}

func (cm *ConcurrentMap[K, V]) Delete(k K) {
	cm.rwMu.Lock()
	defer cm.rwMu.Unlock()
	delete(cm.m, k)
}

func HashFunc[T any](t T) uint64 {
	return xxhash.Sum64(*(*[]byte)(unsafe.Pointer(&t)))
}
