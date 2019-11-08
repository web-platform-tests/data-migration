# WPT Data Migration Scripts

This repository contains scripts that can be used or modified to correct
mistakes in the datastore that backs
[wpt.fyi](https://github.com/web-platform-tests/wpt.fyi)

## Processor

Most new scripts should use the architecture defined in *processor/*, by
implementing the `Runs` interface and then calling `processor.MigrateData`.

## Legacy Directories

*add_run_info/* - used to backfill product and browser name metadata, as well as
switch to a new URL schema.

*add_time_start/* - used to backfill the 'TimeStart' metadata for runs done
before that information was added.

*dedup_runs/* - used to deduplicate runs with the same `raw_results_url` from
before results-processor was idempotent.

*grid/* - ???
