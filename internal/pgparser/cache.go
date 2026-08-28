package pgparser

import (
	"container/list"
	"hash/maphash"
	"sync"
)

type lru struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	evicted    int
	order      *list.List
	index      map[string]*list.Element
	door       doorkeeper
}

type entry struct {
	key  string
	res  *ParseResult
	size int
}

// doorkeeper remembers, in fixed space, which statements have been seen
// once. A statement is admitted to the cache on its second sighting, so a
// stream of literal-varying one-hit queries cannot evict what is being
// reused. It holds hashes rather than the SQL, so a collision costs one
// early admission and nothing else.
type doorkeeper struct {
	seed maphash.Seed
	seen map[uint64]struct{}
	ring []uint64
	next int
	// filled counts the ring slots in use, so slot zero is not confused
	// with a hash that happens to be zero.
	filled int
}

func newDoorkeeper(size int) doorkeeper {
	return doorkeeper{seed: maphash.MakeSeed(), seen: make(map[uint64]struct{}, size), ring: make([]uint64, size)}
}

// admits reports whether key has been seen before, and records it if not.
func (d *doorkeeper) admits(key string) bool {
	h := maphash.String(d.seed, key)
	if _, ok := d.seen[h]; ok {
		delete(d.seen, h)
		return true
	}
	if d.filled == len(d.ring) {
		delete(d.seen, d.ring[d.next])
	} else {
		d.filled++
	}
	d.ring[d.next] = h
	d.next = (d.next + 1) % len(d.ring)
	d.seen[h] = struct{}{}
	return false
}

func newLRU(maxEntries, maxBytes int) *lru {
	return &lru{maxEntries: maxEntries, maxBytes: maxBytes, order: list.New(),
		index: map[string]*list.Element{}, door: newDoorkeeper(maxEntries)}
}

func (c *lru) get(key string) (*ParseResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*entry).res, true
}

func (c *lru) put(key string, res *ParseResult, size int) {
	if size > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.index[key]; ok {
		c.bytes += size - el.Value.(*entry).size
		el.Value = &entry{key, res, size}
		c.order.MoveToFront(el)
	} else {
		if !c.door.admits(key) {
			return
		}
		c.index[key] = c.order.PushFront(&entry{key, res, size})
		c.bytes += size
	}
	for c.order.Len() > c.maxEntries || c.bytes > c.maxBytes {
		back := c.order.Back()
		e := back.Value.(*entry)
		c.order.Remove(back)
		delete(c.index, e.key)
		c.bytes -= e.size
		c.evicted++
	}
}

// stats reports the live byte count and how many entries have been evicted.
func (c *lru) stats() (bytes, evicted int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bytes, c.evicted
}

func (c *lru) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
