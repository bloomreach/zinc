#!/bin/bash

# Run unittests and regression tests. Specify log filenames for regression tests if desired.

set -e

full_log=${1:-tests-full.log}
clean_log=${2:-tests-clean.log}

dir=`dirname $0`

echo ----------
echo UNIT TESTS
echo ----------
$dir/unittests.py
echo
echo "Passed."

echo
echo ----------------
echo REGRESSION TESTS
echo ----------------
$dir/tests.sh >$full_log 2>&1 || echo "Regression test error!"
cat $full_log \
  | grep -v /zinc/zinc.py \
  | grep -v expect_error \
  | grep -v "^\+ echo " \
  | sed -r 's/\b[a-z0-9]{13}\b/_REV_/g' \
  | sed -r 's/mtime=[0-9.]+/mtime=_MTIME_/g' \
  | sed -r 's/^[0-9]{10}[.][0-9]/_TIMESTAMP_/g' \
  | sed -r 's/[0-9-]{10} [0-9:]{8} UTC/_DATE_/g' \
  | sed -r 's/@[0-9a-f]{6,9}/@_ID_/g' \
  | sed -r 's/at 0x[0-9a-f]{6,9}/at _ADDR_/g' \
  | sed -r 's!(s3|file)://.*zinc-testing!_REPO_ROOT_:/!g' \
  | sed -r 's/[ 0-9]+ Killed/_PID_ Killed/g' \
  | sed -r 's/\/private\/tmp\//\/tmp\//g' \
  | sed -r 's/\/mnt\/tmp\//\/tmp\//g' \
  > $clean_log

echo "Done."
echo
echo "Full log: $full_log"
echo "Clean log: $clean_log"
echo
echo "To compare regression test results with previously correct output, run:"
echo "git diff $clean_log"
