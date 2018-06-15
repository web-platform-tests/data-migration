package grid

import "github.com/web-platform-tests/wpt.fyi/shared"

type Run struct {
	ID int32 `json:"id"`
	shared.TestRun
}

type Test struct {
	ID      int32  `json:"id"`
	Test    string `json:"test"`
	Subtest string `json:"subtest,omitempty"`
}
