package waitqueue

import (
	"errors"
	"sync"
	"sync/atomic"
	"runtime"
)

type QueueFinishFunc func()

type WaitQueue interface {
	Enqueue(value interface{})
	Dequeue() (interface{}, QueueFinishFunc)
	QueueLength() uint64
}

type ringItem struct {
	status       uint32
	lock       *sync.Mutex
	condVar    *sync.Cond
	value      interface {}
}

type ringQueue struct {
	ring []ringItem
	writeHead uint64
	readHead uint64
	size uint64
	mask uint64
}

func (q *ringQueue) Dequeue() (interface{}, QueueFinishFunc) {
	pos := atomic.LoadUint64(&q.readHead)
	curReadHead := pos

	for ;; {
		if pos == atomic.LoadUint64(&q.writeHead) {
			runtime.Gosched()
		}
		if atomic.CompareAndSwapUint32(&q.ring[pos].status, 2, 3) {
			break
		}
		pos = (pos + 1) & q.mask
	}
	retValue := q.ring[pos].value

	for !atomic.CompareAndSwapUint64(&q.readHead, curReadHead, (pos + 1 ) & q.mask) {
		newReadHead := atomic.LoadUint64(&q.readHead)
		if (newReadHead > curReadHead && newReadHead > (pos + 1)) ||
			(newReadHead < curReadHead && ((pos + 1) &^q.mask == 0 || (pos + 1) & q.mask < newReadHead)) {
			break
		}
		curReadHead = newReadHead
	}

	return retValue, func() {
		q.ring[pos].lock.Lock()
		defer q.ring[pos].lock.Unlock()
		atomic.StoreUint32(&q.ring[pos].status, 4)
		q.ring[pos].condVar.Signal()
	}
}

func (q *ringQueue) Enqueue(value interface{}) {
	pos := atomic.LoadUint64(&q.writeHead)
	curWriteHead := pos

	for !atomic.CompareAndSwapUint32(&q.ring[pos].status, 0, 1) {
		pos = (pos + 1) & q.mask
		if pos == atomic.LoadUint64(&q.readHead) {
			runtime.Gosched()
		}
	}
	q.ring[pos].value = value
	atomic.StoreUint32(&q.ring[pos].status, 2)

	for !atomic.CompareAndSwapUint64(&q.writeHead, curWriteHead, (pos + 1 ) & q.mask) {
		newWriteHead := atomic.LoadUint64(&q.writeHead)
		if (newWriteHead > curWriteHead && newWriteHead > (pos + 1)) ||
			(newWriteHead < curWriteHead && ((pos + 1) &^q.mask == 0 || (pos + 1) & q.mask < newWriteHead)) {
			break
		}
		curWriteHead = newWriteHead
	}

	q.ring[pos].lock.Lock()
	if atomic.LoadUint32(&q.ring[pos].status) != 4 {
		q.ring[pos].condVar.Wait()
	}

	atomic.StoreUint32(&q.ring[pos].status, 0)
	q.ring[pos].lock.Unlock()
	return
}

func (q *ringQueue) QueueLength() uint64 {
	if q.readHead > q.writeHead {
		return q.writeHead + q.size - q.readHead
	}
	return q.writeHead - q.readHead
}

func NewRingQueue(queueSize uint64) (WaitQueue,error) {
	if queueSize == 0 || (queueSize  & (queueSize-1)) != 0 {
		return nil,errors.New("Queuesize should be a positive power of 2")
	}

	queue := new(ringQueue)
	queue.size = queueSize
	queue.mask = queueSize - 1
	queue.ring = make([]ringItem, queueSize)

	for i := uint64(0); i < queueSize; i++ {
		queue.ring[i].lock = new(sync.Mutex)
		queue.ring[i].condVar = sync.NewCond(queue.ring[i].lock)
	}

	return queue, nil
}
