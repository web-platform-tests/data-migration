package mem

type Filter func(*Tests, *Results, TestID) bool

func And(fs ...Filter) Filter {
	return func(ts *Tests, rs *Results, t TestID) bool {
		for _, f := range fs {
			if !f(ts, rs, t) {
				return false
			}
		}
		return true
	}
}

func Or(fs ...Filter) Filter {
	return func(ts *Tests, rs *Results, t TestID) bool {
		for _, f := range fs {
			if f(ts, rs, t) {
				return true
			}
		}
		return false
	}
}

func Not(f Filter) Filter {
	return func(ts *Tests, rs *Results, t TestID) bool {
		return !f(ts, rs, t)
	}
}
