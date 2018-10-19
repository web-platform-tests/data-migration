package mem

import (
	"sync"

	mapset "github.com/deckarep/golang-set"
)

type Binary func(mapset.Set, mapset.Set) mapset.Set

func And(l mapset.Set, r mapset.Set) mapset.Set {
	return l.Intersect(r)
}

func Or(l mapset.Set, r mapset.Set) mapset.Set {
	return l.Union(r)
}

func BinarySlice(b Binary, l []TestID, r []TestID) []TestID {
	res := make([]TestID, 0)
	for t := range b(setFromSlice(l), setFromSlice(r)).Iter() {
		res = append(res, t.(TestID))
	}
	return res
}

func BinaryChan(b Binary, l chan TestID, r chan TestID) chan TestID {
	res := make(chan TestID)
	go func() {
		var left, right mapset.Set
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			left = setFromChan(l)
		}()
		go func() {
			defer wg.Done()
			right = setFromChan(r)
		}()
		wg.Wait()

		for v := range b(left, right).Iter() {
			res <- v.(TestID)
		}
	}()
	return res
}

func AndSlice(l []TestID, r []TestID) []TestID {
	return BinarySlice(And, l, r)
}

func AndChan(l chan TestID, r chan TestID) chan TestID {
	return BinaryChan(And, l, r)
}

func OrSlice(l []TestID, r []TestID) []TestID {
	return BinarySlice(Or, l, r)
}

func OrChan(l chan TestID, r chan TestID) chan TestID {
	return BinaryChan(Or, l, r)
}

func setFromSlice(sl []TestID) mapset.Set {
	s := mapset.NewSet()
	for _, t := range sl {
		s.Add(t)
	}
	return s
}

func setFromChan(c chan TestID) mapset.Set {
	s := mapset.NewSet()
	for {
		t := <-c
		if t == testEOF {
			break
		}
		s.Add(t)
	}
	return s
}
