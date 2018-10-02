package shared

import (
	"context"
	"time"

	"cloud.google.com/go/bigtable"
	log "github.com/sirupsen/logrus"
)

type TestQuery struct {
	ID       string
	Table    *bigtable.Table
	Context  context.Context
	RowSet   bigtable.RowSet
	IterFunc func(bigtable.Row) bool
	Opts     []bigtable.ReadOption
}

func RunTestQuery(ctx context.Context, tbl *bigtable.Table, q TestQuery, reps int) (ts []time.Duration, err error) {
	ts = make([]time.Duration, 0)
	for i := 0; i < reps; i++ {
		start := time.Now()
		rowCount := 0
		err = tbl.ReadRows(ctx, q.RowSet, func(r bigtable.Row) bool {
			rowCount++
			// for k, vs := range r {
			// 	log.Infof("Key: %s", k)
			// 	for _, v := range vs {
			// 		log.Infof("Value: %s", string(v.Value))
			// 	}
			// }
			return q.IterFunc(r)
		}, q.Opts...)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		end := time.Now()
		t := end.Sub(start)
		log.Printf("%s: query time: %v ; number of rows: %d", q.ID, t, rowCount)
		ts = append(ts, t)
	}
	return ts, nil
}
