package mem

type RunID int64
type ResultID int64

type Results struct {
	ByRunTest map[RunID]map[TestID]ResultID
}

const (
	initialResultsCap = 10
	resultEOF         = ResultID(0)
)

func NewResults() *Results {
	return &Results{ByRunTest: make(map[RunID]map[TestID]ResultID)}
}

func (rs *Results) Add(ru RunID, re ResultID, t TestID) {
	if _, ok := rs.ByRunTest[ru]; !ok {
		rs.ByRunTest[ru] = make(map[TestID]ResultID)
	}
	rs.ByRunTest[ru][t] = re
}

func (rs *Results) QueryChan(ru RunID, re ResultID, in chan TestID) chan TestID {
	res := make(chan TestID)
	go func(byTest map[TestID]ResultID) {
		for {
			t := <-in
			if t == testEOF {
				break
			}
			if byTest[t] == re {
				res <- t
			}
		}
		res <- testEOF
	}(rs.ByRunTest[ru])
	return res
}

func (rs *Results) QueryAll(ru RunID, re ResultID) chan TestID {
	res := make(chan TestID)
	go func() {
		byTest, ok := rs.ByRunTest[ru]
		if !ok {
			res <- testEOF
			return
		}

		for t, result := range byTest {
			if result != re {
				continue
			}
			res <- t
		}
		res <- testEOF
	}()
	return res
}

func (rs *Results) EOF() TestID {
	return testEOF
}
