package pgparser

import (
	"container/list"
	"sync"
)

type lru struct {
	mu         sync.Mutex
	maxEntries int
	maxBytes   int
	bytes      int
	order      *list.List
	index      map[string]*list.Element
}

type entry struct {
	key  string
	res  *ParseResult
	size int
}

func newLRU(maxEntries, maxBytes int) *lru {
	return &lru{maxEntries: maxEntries, maxBytes: maxBytes, order: list.New(), index: map[string]*list.Element{}}
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
		c.index[key] = c.order.PushFront(&entry{key, res, size})
		c.bytes += size
	}
	for c.order.Len() > c.maxEntries || c.bytes > c.maxBytes {
		back := c.order.Back()
		e := back.Value.(*entry)
		c.order.Remove(back)
		delete(c.index, e.key)
		c.bytes -= e.size
	}
}

func (c *lru) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
