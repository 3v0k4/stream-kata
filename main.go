package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Schedule[T comparable] struct {
	mu sync.Mutex
	m  map[T]chan any
	d  time.Duration
}

func NewSchedule[T comparable](d time.Duration) *Schedule[T] {
	return &Schedule[T]{
		m: make(map[T]chan any),
		d: d,
	}
}

func (s *Schedule[T]) RunLast(id T, f func()) {
	go func() {
		s.mu.Lock()
		if done, ok := s.m[id]; ok {
			close(done)
		}
		done := make(chan any)
		s.m[id] = done
		s.mu.Unlock()

		select {
		case <-done:
		case <-time.After(s.d):
			f()
		}
	}()
}

type TimedMap[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]V
	s  *Schedule[K]
}

func NewTimedMap[K comparable, V any](d time.Duration) TimedMap[K, V] {
	return TimedMap[K, V]{m: make(map[K]V), s: NewSchedule[K](d)}
}

func (tm *TimedMap[K, V]) Get(k K) (V, bool) {
	tm.mu.Lock()
	v, ok := tm.m[k]
	tm.mu.Unlock()
	return v, ok
}

func (tm *TimedMap[K, V]) Set(k K, v V) {
	tm.mu.Lock()
	tm.m[k] = v
	tm.mu.Unlock()

	go func() {
		f := func() {
			tm.mu.Lock()
			delete(tm.m, k)
			tm.mu.Unlock()
		}
		tm.s.RunLast(k, f)
	}()
}

type Event struct {
	ClientId  int
	Uuid      string
	CreatedAt time.Time
}

func NewEvent(id int, uuid string) Event {
	return Event{
		ClientId:  id,
		Uuid:      uuid,
		CreatedAt: time.Now(),
	}
}

type Distributor struct {
	queues   []chan Event
	uuidSeen TimedMap[string, bool]
}

func NewDistributor(d time.Duration, c int, f func(i int, e Event)) *Distributor {
	var queues []chan Event
	for i := 0; i < c; i++ {
		queues = append(queues, make(chan Event))
	}

	dist := &Distributor{
		queues:   queues,
		uuidSeen: NewTimedMap[string, bool](d),
	}

	for i, eventS := range queues {
		go func(i int, eventS <-chan Event) {
			for e := range eventS {
				f(i, e)
			}
		}(i, eventS)
	}

	return dist
}

func (d *Distributor) Enqueue(e Event) {
	if v, _ := d.uuidSeen.Get(e.Uuid); v {
		return
	}
	d.uuidSeen.Set(e.Uuid, true)
	d.queues[e.ClientId%len(d.queues)] <- e
}

type Stat struct {
	i   int
	val time.Duration
}

type Stats struct {
	start time.Time
	count int
	sum   time.Duration
}

// Need a bit more than 10 threads:
// ~110s for 100.000 events
// throughput ~1.1ms (instead of 1ms)
func main() {
	queued := make(chan int)
	go func() {
		ticker := time.Tick(10 * time.Second)
		count := 0
		for {
			select {
			case <-ticker:
				fmt.Printf("queue len: %d\n", count)
			case q := <-queued:
				count += q
			}
		}
	}()

	stats := make(chan Stat)
	go func() {
		metrics := Stats{time.Now(), 0, 0}
		ticker := time.Tick(10 * time.Second)
		for {
			select {
			case <-ticker:
				fmt.Printf("events: %d, latency: %d, thru: %d\n", metrics.count, metrics.sum/time.Duration(metrics.count), time.Since(metrics.start)/time.Duration(metrics.count))
			case s := <-stats:
				metrics.count++
				metrics.sum += s.val
			}
		}
	}()

	var wg sync.WaitGroup

	f := func(i int, e Event) {
		s := Stat{i, time.Since(e.CreatedAt)}
		go func() {
			queued <- -1
		}()
		go func() {
			stats <- s
		}()
		time.Sleep(10 * time.Millisecond)
		wg.Done()
	}
	d := NewDistributor(10*time.Second, 10, f) // try with 20 instead of 10

	now := time.Now()
	for j := 1; j <= 100; j++ {
		for i := 1; i <= 1000; i++ {
			js := fmt.Sprint(j)
			is := fmt.Sprint(i)
			yy := fmt.Sprintf("%s%s", "000"[:3-len(js)]+js, "0000"[:4-len(is)]+is)
			id, _ := strconv.Atoi(yy)
			go func() {
				queued <- 1
			}()
			wg.Add(1)
			go d.Enqueue(NewEvent(id, fmt.Sprint(id)))
		}
		time.Sleep(time.Second)
	}
	wg.Wait()
	fmt.Println(time.Since(now))
}
