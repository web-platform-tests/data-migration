#!/bin/bash

set -evh

BRANCHES=(
  "bt__wpt-results"
  "bt__wpt-results-per-test"
  "bt__wpt-results-per-test-wide"
)

for BRANCH in "${BRANCHES[@]}"; do
  git checkout "${BRANCH}"
  go run grid/query/bigtable/query.go 2>&1 | tee "${BRANCH}.log"
done
