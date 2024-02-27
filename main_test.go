package main_test

import (
	"sync"
	"testing"
	"time"

	e "example.com"
)

type Counter struct {
	count int
}

func (c *Counter) inc() {
	c.count++
}

func TestSchedule(t *testing.T) {
	t.Run("calls passed func", func(t *testing.T) {
		counter := Counter{}
		s := e.NewSchedule[string](time.Duration(0))

		s.RunLast("id", counter.inc)
		time.Sleep(time.Millisecond)

		got := counter.count
		want := 1
		if got != want {
			t.Errorf("got: %d, want: %d", got, want)
		}
	})

	t.Run("calls passed func once", func(t *testing.T) {
		counter := Counter{}
		s := e.NewSchedule[string](time.Millisecond)

		s.RunLast("id", counter.inc)
		s.RunLast("id", counter.inc)
		time.Sleep(time.Millisecond)

		got := counter.count
		want := 1
		if got != want {
			t.Errorf("got: %d, want: %d", got, want)
		}
	})

	t.Run("he expires a key", func(t *testing.T) {
		counter := Counter{}
		s := e.NewSchedule[string](time.Duration(0))

		s.RunLast("id", counter.inc)
		time.Sleep(time.Millisecond)
		s.RunLast("id", counter.inc)
		time.Sleep(time.Millisecond)

		got := counter.count
		want := 2
		if got != want {
			t.Errorf("got: %d, want: %d", got, want)
		}
	})
}

func TestTimedMap(t *testing.T) {
	t.Run("it sets and gets a key", func(t *testing.T) {
		tm := e.NewTimedMap[string, bool](time.Hour)

		key := "key"
		tm.Set(key, true)

		v, ok := tm.Get(key)
		if !v || !ok {
			t.Errorf("want to find key '%s' in the map %+v", key, tm)
		}
	})

	t.Run("it expires a key", func(t *testing.T) {
		tm := e.NewTimedMap[string, bool](time.Nanosecond)

		key := "key"
		tm.Set(key, true)
		time.Sleep(time.Millisecond)

		v, ok := tm.Get(key)
		if v || ok {
			t.Errorf("did not want to find key '%s' in the map %+v", key, tm)
		}
	})
}

type Map struct {
	mu sync.Mutex
	m  map[int]int
}

func (m *Map) inc(key int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[key]++
}

func (m *Map) get(key int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.m[key]
}

func TestDistributor(t *testing.T) {
	t.Run("consumes 2 events", func(t *testing.T) {
		e1 := e.NewEvent(1, "1")
		e2 := e.NewEvent(2, "2")
		consumed := Map{m: make(map[int]int)}
		f := func(i int, _ e.Event) {
			consumed.inc(i)
		}
		d := e.NewDistributor(time.Hour, 1, f)

		d.Enqueue(e1)
		d.Enqueue(e2)
		time.Sleep(time.Millisecond)

		got := consumed.get(0)
		want := 2
		if got != want {
			t.Errorf("got: %d, want: %d", got, want)
		}
	})

	t.Run("discards same uuid", func(t *testing.T) {
		e1 := e.NewEvent(1, "1")
		e2 := e.NewEvent(1, "1")
		e3 := e.NewEvent(2, "2")
		consumed := Map{m: make(map[int]int)}
		f := func(i int, _ e.Event) {
			consumed.inc(i)
		}
		d := e.NewDistributor(time.Hour, 1, f)

		d.Enqueue(e1)
		d.Enqueue(e2)
		d.Enqueue(e3)
		time.Sleep(time.Millisecond)

		got := consumed.get(0)
		want := 2
		if got != want {
			t.Errorf("got: %d, want: %d", got, want)
		}
	})

	t.Run("assigns same ClientId to same consumer", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			e1 := e.NewEvent(1, "1")
			e2 := e.NewEvent(1, "2")
			consumed := Map{m: make(map[int]int)}
			f := func(i int, _ e.Event) {
				consumed.inc(i)
			}
			d := e.NewDistributor(time.Hour, 1, f)

			d.Enqueue(e1)
			d.Enqueue(e2)
			time.Sleep(time.Millisecond)

			want := 2
			for _, c := range consumed.m {
				if c == want {
					return
				}
			}
			t.Errorf("got: %v, want: %d", consumed.m, want)
		}
	})
}
