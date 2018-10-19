package mem

type RunID int64
type ResultID int64

type Results struct {
	ByRunResult map[RunID]map[ResultID][]TestID
}

const (
	initialResultsCap = 10
	resultEOF         = ResultID(0)
)

func NewResults() *Results {
	return &Results{ByRunResult: make(map[RunID]map[ResultID][]TestID)}
}

func (rs *Results) Add(ru RunID, re ResultID, t TestID) {
	if _, ok := rs.ByRunResult[ru]; !ok {
		rs.ByRunResult[ru] = make(map[ResultID][]TestID)
	}
	if _, ok := rs.ByRunResult[ru][re]; !ok {
		rs.ByRunResult[ru][re] = make([]TestID, 0, initialResultsCap)
	}
	rs.ByRunResult[ru][re] = append(rs.ByRunResult[ru][re], t)
}

func (rs *Results) QuerySlice(ru RunID, re ResultID) []TestID {
	if _, ok := rs.ByRunResult[ru]; !ok {
		return nil
	}
	if _, ok := rs.ByRunResult[ru][re]; !ok {
		return nil
	}
	return rs.ByRunResult[ru][re][0:]
}

func (rs *Results) QueryChan(ru RunID, re ResultID) chan TestID {
	if _, ok := rs.ByRunResult[ru]; !ok {
		return nil
	}
	if _, ok := rs.ByRunResult[ru][re]; !ok {
		return nil
	}

	res := make(chan TestID)
	go func(ts []TestID) {
		for _, t := range ts {
			res <- t
		}
		res <- testEOF
	}(rs.ByRunResult[ru][re][0:])
	return res
}

func (rs *Results) EOF() TestID {
	return testEOF
}
