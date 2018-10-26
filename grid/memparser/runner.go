package memparser

import "github.com/web-platform-tests/data-migration/grid/mem"

type Filterable interface {
	ToFilter() mem.UnboundFilter
}

func (nf *NameFragment) ToFilter() mem.UnboundFilter {
	return mem.NewTestNameFilter(nf.Name)
}
func (rf *ResultFragment) ToFilter() mem.UnboundFilter {
	return mem.NewResultEQFilter(rf.RunID, rf.ResultID)
}

func (a *And) ToFilter() mem.UnboundFilter {
	names := make([]mem.UnboundFilter, 0)
	results := make([]mem.UnboundFilter, 0)
	misc := make([]mem.UnboundFilter, 0)
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
	return mem.NewAnd(append(names, append(misc, results...)...)...)
}

func (o *Or) ToFilter() mem.UnboundFilter {
	fs := make([]mem.UnboundFilter, 0, len(o.Parts))
	for _, p := range o.Parts {
		fs = append(fs, p.ToFilter())
	}
	return mem.NewOr(fs...)
}

func (n *Not) ToFilter() mem.UnboundFilter {
	return mem.NewNot(n.Part.ToFilter())
}
