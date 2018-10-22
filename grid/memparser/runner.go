package memparser

import "github.com/web-platform-tests/data-migration/grid/mem"

type Plannable interface {
	ToPlan() Plan
}

type Plan interface {
	RunAll(*mem.Tests, *mem.Results) chan mem.TestID
	RunChan(*mem.Tests, *mem.Results, chan mem.TestID) chan mem.TestID
}

type AndPlan struct {
	Parts []Plan
}

type ParallelAnd AndPlan
type SerialAnd AndPlan

func (nf *NameFragment) RunAll(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	return ts.QueryAll(nf.Name)
}

func (nf *NameFragment) RunChan(ts *mem.Tests, rs *mem.Results, in chan mem.TestID) chan mem.TestID {
	return ts.QueryChan(nf.Name, in)
}

func (nf *NameFragment) ToPlan() Plan {
	return nf
}

func (rf *ResultFragment) RunAll(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	if rf.Op.Name == "EQ" {
		return rs.QueryAll(rf.RunID, rf.ResultID)
	}
	// TODO: More operators and/or error handling.
	return nil
}

func (rf *ResultFragment) RunChan(ts *mem.Tests, rs *mem.Results, in chan mem.TestID) chan mem.TestID {
	if rf.Op.Name == "EQ" {
		return rs.QueryChan(rf.RunID, rf.ResultID, in)
	}
	// TODO: More operators and/or error handling.
	return nil
}

func (rf *ResultFragment) ToPlan() Plan {
	return rf
}

func (a *And) ToPlan() Plan {
	if len(a.Parts) == 0 {
		return nil
	}

	// Construct plan:
	//
	// (all tests)
	//   |
	//   +=> |test names|\
	//   |                => |results| => output
	//    => |nested    |/
	//
	// ... but optimize out empty or len(plans)==1 parts.

	names := make([]Plan, 0)
	results := make([]Plan, 0)
	misc := make([]Plan, 0)
	for _, p := range a.Parts {
		if _, ok := p.(*NameFragment); ok {
			names = append(names, p.ToPlan())
		} else if _, ok := p.(*ResultFragment); ok {
			results = append(results, p.ToPlan())
		} else {
			misc = append(misc, p.ToPlan())
		}
	}

	var namesPlan, resultsPlan, miscPlan, namesMiscPlan Plan
	if len(names) == 1 {
		namesPlan = names[0]
	} else if len(names) > 1 {
		namesPlan = &ParallelAnd{names}
	}

	if len(results) == 1 {
		resultsPlan = results[0]
	} else if len(results) > 1 {
		resultsPlan = &ParallelAnd{results}
	}

	if len(misc) == 1 {
		miscPlan = misc[0]
	} else if len(misc) > 1 {
		miscPlan = &ParallelAnd{misc}
	}

	if namesPlan == nil {
		namesMiscPlan = miscPlan
	} else if miscPlan == nil {
		namesMiscPlan = namesPlan
	} else {
		np, nand := namesPlan.(*ParallelAnd)
		mp, mand := miscPlan.(*ParallelAnd)
		if nand && mand {
			namesMiscPlan = &ParallelAnd{append(np.Parts, mp.Parts...)}
		} else if nand {
			namesMiscPlan = &ParallelAnd{append(np.Parts, miscPlan)}
		} else if mand {
			namesMiscPlan = &ParallelAnd{append(mp.Parts, namesPlan)}
		} else {
			namesMiscPlan = &ParallelAnd{[]Plan{namesPlan, miscPlan}}
		}
	}

	if namesMiscPlan == nil {
		return resultsPlan
	}

	return &SerialAnd{[]Plan{namesMiscPlan, resultsPlan}}
}

func (a *SerialAnd) RunAll(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	var c chan mem.TestID
	for _, p := range a.Parts {
		if c == nil {
			c = p.RunAll(ts, rs)
			continue
		}
		c = p.RunChan(ts, rs, c)
	}
	return c
}

func (a *SerialAnd) RunChan(ts *mem.Tests, rs *mem.Results, in chan mem.TestID) chan mem.TestID {
	var c chan mem.TestID
	for _, p := range a.Parts {
		if c == nil {
			c = p.RunChan(ts, rs, in)
			continue
		}
		c = p.RunChan(ts, rs, c)
	}
	return c
}

func (a *ParallelAnd) RunAll(ts *mem.Tests, rs *mem.Results) chan mem.TestID {
	var c chan mem.TestID
	for _, p := range a.Parts {
		if c == nil {
			c = p.RunAll(ts, rs)
			continue
		}
		c = mem.AndChan(c, p.RunAll(ts, rs))
	}
	return c
}

func (a *ParallelAnd) RunChan(ts *mem.Tests, rs *mem.Results, in chan mem.TestID) chan mem.TestID {
	var c chan mem.TestID
	for _, p := range a.Parts {
		if c == nil {
			c = p.RunChan(ts, rs, in)
			continue
		}
		c = mem.AndChan(c, p.RunChan(ts, rs, in))
	}
	return c
}
