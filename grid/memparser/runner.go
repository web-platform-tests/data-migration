package memparser

import "github.com/web-platform-tests/data-migration/grid/mem"

type Filterable interface {
	ToFilter() mem.Filter
}

func (nf *NameFragment) ToFilter() mem.Filter {
	return mem.TestFilter(nf.Name)
}
func (rf *ResultFragment) ToFilter() mem.Filter {
	return mem.ResultFilter(rf.RunID, rf.ResultID)
}

func (a *And) ToFilter() mem.Filter {
	names := make([]mem.Filter, 0)
	results := make([]mem.Filter, 0)
	misc := make([]mem.Filter, 0)
	for _, p := range a.Parts {
		if _, ok := p.(*NameFragment); ok {
			names = append(names, p.ToFilter())
		} else if _, ok := p.(*ResultFragment); ok {
			results = append(results, p.ToFilter())
		} else {
			misc = append(misc, p.ToFilter())
		}
	}

	// Results filters tend to scan a large number of tests; do them last.
	return mem.And(append(names, append(misc, results...)...)...)
}

func (o *Or) ToFilter() mem.Filter {
	fs := make([]mem.Filter, 0, len(o.Parts))
	for _, p := range o.Parts {
		fs = append(fs, p.ToFilter())
	}
	return mem.Or(fs...)
}

func (n *Not) ToFilter() mem.Filter {
	return mem.Not(n.Part.ToFilter())
}
