# WPT Data Migration Scripts

This repository contains scripts that can be used or modified to correct
mistakes in the datastore that backs
[wpt.fyi](https://github.com/web-platform-tests/wpt.fyi).

## Running a script

First of all, run `gcloud auth application-default login` (you should already
have access to wptdashboard and/or wptdashboard-staging projects).

This repo does NOT use Go modules yet, so it is recommeneded to check out the
repo at `$GOPATH/src/github.com/web-platform-tests/data-migration`. Then run
`go get -u ./...` to get all the dependencies.

Finally, you can run most scripts with `go run`, e.g. `go run tagger/master.go
--help`.

## Writing a script

We have a few different categories of scripts.

### Datastore-only

This is the most common kind. These scripts do a pass of scan-check-modify over
all `TestRun`s in Datastore in parallel. Check-and-modify is done atomically in
a transaction.

The reusable logic is in [`processor/`](processor/). New scripts only need to
implement the [`Runs` interface][1].

[1]: https://github.com/web-platform-tests/data-migration/blob/cca6ab5d399b2767c429789edbaf75114a530965/processor/runs.go#L9-L12

Examples can be found in [`tagger/`](tagger/).

### Storage

The following scripts also download results from GCS, so they are a lot slower.

*add_run_info/* - used to backfill product and browser name metadata, as well as
switch to a new URL schema.

*add_time_start/* - used to backfill the `TimeStart` metadata for runs done
before that information was added.

*dedup_runs/* - used to deduplicate runs with the same `raw_results_url` from
before results-processor was idempotent.

### Bigtable

*grid/* - an experiment to load all results into Bigtable.
