package window

import "container/heap"

// PriorityQueue implements heap.Interface and holds WindowAccumulator pointers,
// ordered by WindowEndNs ascending for O(1) expiration discovery.
type PriorityQueue []*WindowAccumulator

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Key.WindowEndNs < pq[j].Key.WindowEndNs
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].HeapIndex = i
	pq[j].HeapIndex = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*WindowAccumulator)
	item.HeapIndex = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // Avoid memory leak
	item.HeapIndex = -1
	*pq = old[0 : n-1]
	return item
}

// WindowPriorityQueue is a safe wrapper around PriorityQueue implementing heap operations.
type WindowPriorityQueue struct {
	pq PriorityQueue
}

// NewWindowPriorityQueue initializes an empty Min-Heap Priority Queue.
func NewWindowPriorityQueue() *WindowPriorityQueue {
	wpq := &WindowPriorityQueue{
		pq: make(PriorityQueue, 0),
	}
	heap.Init(&wpq.pq)
	return wpq
}

// Push adds an accumulator to the priority queue.
func (wpq *WindowPriorityQueue) Push(acc *WindowAccumulator) {
	heap.Push(&wpq.pq, acc)
}

// Pop removes and returns the earliest expiring accumulator from the priority queue.
func (wpq *WindowPriorityQueue) Pop() *WindowAccumulator {
	if len(wpq.pq) == 0 {
		return nil
	}
	return heap.Pop(&wpq.pq).(*WindowAccumulator)
}

// Peek returns the earliest expiring accumulator without removing it (O(1)).
func (wpq *WindowPriorityQueue) Peek() *WindowAccumulator {
	if len(wpq.pq) == 0 {
		return nil
	}
	return wpq.pq[0]
}

// Len returns the current number of windows in the priority queue.
func (wpq *WindowPriorityQueue) Len() int {
	return len(wpq.pq)
}
