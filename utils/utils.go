package utils

import "sync"

// TODO 改为分片Map
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
