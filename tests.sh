#!/bin/bash

#
# Copyright 2011-2012 BloomReach, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Tests for Zinc.
#
# All basic commands are tested, with checkouts, commits, tags, etc. from
# multiple working directories.  Output of this script is intended to be
# relatively clean, so it can be post-processed and used as a regression test.

# Set this to an S3 bucket for testing.
s3_testing_bucket=br-tmp
# Set this to 1 to use local file repo; otherwise use S3.
run_local=1


repo_prefix=zinc-testing/repo
file_root=/tmp/$repo_prefix
s3_root=s3://$s3_testing_bucket/$repo_prefix
work=/tmp/zinc-testing/work
zinc_path=$(cd `dirname $0`; pwd)/zinc.py

# Tests can be done either way, but run faster on local filesystem.
if [[ "$run_local" == "1" ]]; then
  root_uri="file://$file_root"
else
  root_uri="$s3_root"
fi

#debug=--debug
#debug=-v
debug=$1

mkdir -p $work
work=$(cd $work; pwd)

# We turn on exit on error, so that any status code changes cause a test failure.
set -e
set -o pipefail

mkdir -p $file_root $work

zinc() {
  "$zinc_path" $debug "$@"
}

delete_work() {
  rm -rf $work/work_dir*
  rm -rf $work/copies
  rm -rf $work/forbidden
  rm -rf $work/logs
}

delete_repo() {
  if echo $root_uri | grep s3; then
    s3cmd del -r $root_uri >/dev/null
  else
    rm -rf $file_root
  fi
}

create_file() {
  echo "$2" > $1
}

create_file_of_size() {
  dd if=/dev/zero bs=1000000 count=$1 of=$2 status=noxfer
}

ls_pretty() {
  # Ensure deterministic ordering of revision paths.
  find $1 -type f -printf "%12s %p\n" | sed -r 's/\b[a-z0-9]{13}\b/_REV_/g' | sort -k2
}

ls_pretty_nosize() {
  # Ensure deterministic ordering of revision paths.
  find $1 -type f -printf "%p\n" | sed -r 's/\b[a-z0-9]{13}\b/_REV_/g' | sort -k2
}

# A trick to test for error conditions
expect_error() {
  echo "(got expected error: status $?)"
}

cd $work

delete_repo
delete_work

echo Running test against repository: $root_uri
echo Work directory: $work

# This will echo all commands as they are read. Bash commands plus their
# outputs will be used for validating regression tests pass (set -x is similar
# but less readable and sometimes not deterministic).
set -v

# Record Python version.
python -V

### Setup
zinc init $root_uri

zinc newscope -R $root_uri -u user0 test_scope
zinc checkout -s test_scope $root_uri work_dir

zinc newscope -R $root_uri -u user0 another_scope --date '2010-01-01'
zinc checkout -s another_scope $root_uri work_dir

cd work_dir


### First working dir
mkdir test_scope/subdir1
create_file test_scope/file1 "contents of file1"
create_file test_scope/subdir1/file2 "contents of file2"
# S3 has a minimum filesize of 5MB for part size in multipart
create_file_of_size 50 test_scope/subdir1/bigfile
zinc status -s test_scope
cd test_scope
zinc status
cd subdir1
zinc status
cd ../..
zinc commit -s test_scope -u user1 -m "first commit" --date '2010-01-01 9:00am'

sleep 1


mkdir test_scope/subdir2
create_file test_scope/subdir1/file3 "more contents"
create_file test_scope/subdir2/file4 "contents of file4, a different size"
rm test_scope/file1
create_file test_scope/file5 "more contents"
zinc status -s test_scope
zinc commit -s test_scope -u user2 -m "second commit"

zinc tags -s test_scope
zinc tag -s test_scope my-first-tag

sleep 1

create_file test_scope/subdir1/file2 "new contents"
create_file test_scope/file6 "file6 contents"
rm test_scope/file5
zinc status -s test_scope
zinc commit -s test_scope -u user3 -m "another commit!
with multiple lines"
sleep 2

# Try MD5 checks for file of unchanged size
touch test_scope/file6
zinc status -s test_scope
create_file test_scope/file6 "file6 contentX"
touch -d 2000-01-01 test_scope/file6
zinc status -s test_scope
create_file test_scope/file6 "file6 contents"
touch -d 2000-01-01 test_scope/file6
zinc status -s test_scope

sleep 1

zinc tag -s test_scope my-second-tag
zinc tag -s test_scope my-second-tag-duplicate

# Testing reverts
rm test_scope/subdir1/file2
create_file test_scope/file6 "modified contents"
create_file test_scope/file7 "new file7"
create_file test_scope/file8 "new file8"
zinc revert -s test_scope file6
zinc status -s test_scope
ls -1 test_scope
zinc revert -s test_scope subdir1/file2
zinc revert -s test_scope bad_filename || expect_error
zinc status -s test_scope
zinc revert -s test_scope
zinc status -s test_scope
ls -1 test_scope
zinc commit -s test_scope -u user3 -m "empty commit, will fail" || expect_error
zinc commit --suppress -s test_scope -u user3 -m "empty commit, will be ignored"

create_file test_scope/file7 "new file7, take 2"
zinc commit -s test_scope -u user3 -m "another commit"
test_scope_rev=`zinc id -s test_scope`
zinc tag -s test_scope -r $test_scope_rev my-third-tag

zinc status -s test_scope
zinc log -s test_scope
zinc tags -s test_scope
zinc id -s test_scope
zinc ids
zinc scopes

# Testing diff
create_file test_scope/file1 "contents of file1"
create_file test_scope/subdir1/file2 "contents of file2"
rm test_scope/subdir1/file3
zinc status -s test_scope
zinc diff -s test_scope
zinc diff -s test_scope file1
zinc diff -s test_scope subdir1/file4
zinc diff -s test_scope file1 subdir1/file2
zinc revert -s test_scope
zinc status -s test_scope
zinc diff -s test_scope -r my-first-tag
create_file test_scope/file1 "contents of file1"
create_file test_scope/subdir1/file2 "contents of file2"
rm test_scope/subdir1/file3
zinc status -s test_scope
zinc diff -s test_scope -r my-first-tag
zinc diff -s test_scope -r my-first-tag file1
zinc diff -s test_scope -r my-first-tag subdir1/file3
zinc diff -s test_scope -r my-first-tag:my-second-tag
zinc diff -s test_scope -r my-first-tag:my-second-tag file1
zinc status -s test_scope
zinc diff -s test_scope > test.patch
zinc revert -s test_scope
zinc status -s test_scope
patch -p0 < test.patch
zinc status -s test_scope
zinc revert -s test_scope
zinc diff -s test_scope
rm test_scope/file1.orig
rm test_scope/subdir1/file2.orig

### Second working dir
cd $work
# show the log starting from tip even without checking out
zinc log -R $root_uri -s test_scope
zinc checkout -s test_scope -r my-first-tag $root_uri work_dir_alt
cd work_dir_alt
zinc ids
zinc update -s test_scope -r my-second-tag
zinc update -s test_scope
create_file test_scope/added_file "an added file"
zinc commit -s test_scope -u user4 -m "commit from a different working dir"
zinc ids

diff -r --brief test_scope/subdir1 $work/work_dir/test_scope/subdir1

### Third working dir
cd $work
zinc checkout --all $root_uri work_dir_all
cd work_dir_all
zinc tag --work tagging-things-at-once
zinc update --work
zinc update --all

### First working dir again
cd $work
cd work_dir
create_file test_scope/new_file_from_another_work_dir "contents"
# show the log starting from the checkedout rev
zinc log -s test_scope
zinc update -s test_scope
# show the log starting from tip
zinc log -s test_scope
zinc commit -s test_scope -u user5 -m "committing from original working dir"
create_file test_scope/non_conflicting_file "modifying this file should be fine since it is new"
zinc update -s test_scope -r my-first-tag
create_file test_scope/file6 "modifying this file locally so next update will have conflicts"
zinc update -s test_scope -r tagging-things-at-once || expect_error
cat .zinc/checkout-state

### Third working dir again
cd $work
cd work_dir_all
zinc newscope -u user5 -R $root_uri new_scope
zinc update --all
rm test_scope/file6
create_file test_scope/file7 "modifying file7"
create_file test_scope/file8 "creating a new file"
create_file new_scope/file_in_new_scope "creating a new file in second scope"
zinc status --work
zinc commit --work -u user6 -m "committing multiple scopes at once"
cat .zinc/checkout-state

# Test error suppression
zinc newscope -u user5 -R $root_uri new_scope || expect_error
zinc newscope -u user5 -R $root_uri --suppress new_scope

### Compression
create_file test_scope/file9 "another file for compression"
zinc commit --work --compression=lzo -u user6 -m "committing with compression on"
create_file test_scope/file10 "another file no compression"
zinc commit --work --compression=raw -u user6 -m "committing with compression off"
zinc tag --work mixed-compression
zinc status --work --mod-all
zinc status -s test_scope --mod-all
zinc commit --work --mod-all --compression=lzo -u user7 -m "recommitting with compression on for all files"

### Ignore files
create_file test_scope/junk1.orig "this file should be ignored"
create_file test_scope/junk2.bak "this file should be ignored"
create_file test_scope/.junk3 "this file should be ignored"
create_file test_scope/.tmp.junk4 "this file should be ignored"
mkdir -p test_scope/.tmp-copy.e8q4rtgsjvkoy/some-path
create_file test_scope/.tmp-copy.e8q4rtgsjvkoy/some-path/junk5 "this whole directory should be ignored"
zinc status --work 

### Test track, untrack, and checkout --mode partial, and status --full
cd $work
zinc checkout -s test_scope --mode partial $root_uri work_dir_track
cd work_dir_track
zinc tag -s test_scope my-fourth-tag
zinc track -s test_scope subdir1/file3
zinc status -s test_scope --full
create_file test_scope/subdir1/file3 "modify contents"
zinc status -s test_scope --full
zinc commit -s test_scope -u user8 -m "partial commiting"
zinc status -s test_scope --full
zinc untrack -s test_scope subdir1/file3
zinc track -s test_scope subdir1/file3
zinc status -s test_scope --full #=> should not say that subdir1/file3 is modifiled since it is not modified
zinc track -s test_scope subdir1/file11 || expect_error #=> file11 does not exist yet
create_file test_scope/subdir1/file11 "new file"
zinc track -s test_scope subdir1/file11
create_file test_scope/untracked "this files is not tracked"
zinc status -s test_scope --full #=> test_scope/untracked is not tracked yet
zinc track -s test_scope --mode auto #=> switch to auto mode
zinc untrack -s test_scope subdir1/file11 || expect_error #=> untracking is not allowed in auto mode
zinc status -s test_scope --full #=> test_scope/untracked will show up as well as other changes
zinc track -s test_scope --mode partial #=> switch back to partial mode
zinc commit -s test_scope -u user8 -m "partial commiting2"
zinc status -s test_scope
rm test_scope/untracked
zinc untrack -s test_scope subdir1/file11
rm test_scope/subdir1/file11
zinc track -s test_scope subdir1/file11 --no-download #=> does not perform download
zinc status -s test_scope #=> shows that subdir1/file11 is deleted
zinc commit -s test_scope -u user8 -m "partial commiting3" #=> delete subdir1/file11 without downloading it
zinc update -s test_scope -r my-fourth-tag #=> go back to previous rev
zinc untrack -s test_scope subdir1/file3
zinc update -s test_scope
zinc track -s test_scope subdir1/file3
zinc status -s test_scope #=> shows that subdir1/file3 is modifiled
zinc update -s test_scope -r my-fourth-tag || expect_error #=> should fail since both local and remote subdir1/file3 are changed
cat test_scope/subdir1/file3 #=> should be "more contents"
zinc revert -s test_scope #=> this should download the newest version of subdir1/file3
cat test_scope/subdir1/file3 #=> should be "modify contents"
zinc status -s test_scope
#### wildcard test
zinc untrack -s test_scope "subdir1/*"
zinc status -s test_scope --full
zinc track -s test_scope "*file*" --recursive
zinc status -s test_scope --full


### Another working dir, but no caching
cd $work
zinc checkout --no-cache -s test_scope -r my-first-tag $root_uri work_dir_no_cache

### Check internal .zinc state
ls_pretty work_dir_all/.zinc
ls_pretty work_dir_no_cache/.zinc

### Repo-only commands
cd $work
zinc list -R $root_uri -s test_scope 
zinc list -R $root_uri -s test_scope subdir1
zinc list -R $root_uri -s test_scope --short-paths subdir1 
zinc list -R $root_uri -s test_scope -a
zinc list -R $root_uri -s test_scope -a --short-paths
zinc copy -R $root_uri -s test_scope subdir1/file2 copies/file2-copy
cd copies
zinc copy -R $root_uri -s test_scope subdir1/file2 file2-copy2
cd ..
zinc copy -R $root_uri -s test_scope -r my-second-tag subdir1/file3 copies/file3-copy
zinc copy -R $root_uri -s test_scope -r my-second-tag -a subdir1 copies/subdir1-copy
zinc log -R $root_uri -s test_scope -r :tip --date '> 2010-01-02'
zinc tags -R $root_uri -s test_scope -r :tip --date '>2010-01-02'
zinc locate -R $root_uri -s test_scope -r my-second-tag subdir1/file2
zinc locate -R $root_uri -s test_scope file9
zinc locate -R $root_uri -s test_scope file10
zinc list -R $root_uri -s test_scope subdir1 --recursive
zinc locate -R $root_uri -s test_scope subdir1 --recursive
zinc _manifest -R $root_uri -s test_scope -r my-third-tag 
zinc _manifest -R $root_uri -s test_scope -r mixed-compression
zinc _manifest -R $root_uri -s test_scope


### Error situations

# Nested checkouts.
cd $work/work_dir/test_scope
echo blah
zinc checkout -s test_scope $root_uri work_dir || expect_error
ls_pretty .

# Disk I/O error.
mkdir $work/forbidden
chmod a-wx $work/forbidden
zinc copy -R $root_uri -s test_scope subdir1/file2 $work/forbidden/file2-copy || expect_error
chmod +wx $work/forbidden

# Set up a big file so we can interrupt operations.
cd $work/work_dir_all
zinc update -s test_scope
create_file_of_size 100 test_scope/subdir1/bigfile-uncompressed
zinc commit -s test_scope -u user1 -m "another commit (big file, no compression)" --compression=raw

# Broken pipe on output.
# Output here is ugly in Python 2.7: http://bugs.python.org/issue11380
zinc log -R $root_uri -s test_scope | head -5
zinc log -R $root_uri -s test_scope | true

# Interruptions.
cd $work
mkdir -p $work/logs
zinc copy -R $root_uri -s test_scope subdir1/bigfile-uncompressed copies/bigfile-copy1 > logs/copy1.log 2>&1 &
sleep 0.2
jobs
kill -KILL %1
sleep 0.2
jobs
ls_pretty_nosize copies
wait


# SIGPIPE is important to test since S3 connections can sometimes get this
# (especially for big files).  It's not clear this accurately simulates a
# socket pipe failure, but including in any case for basic testing.
zinc copy -R $root_uri -s test_scope subdir1/bigfile-uncompressed copies/bigfile-copy2 > logs/copy2.log 2>&1 &
sleep 0.2
jobs
kill -PIPE %1
sleep 0.2
jobs
ls_pretty_nosize copies
wait

head -1000 logs/*.log

# Other test cases to consider adding:
# - actual S3 socket or other error (simulate S3 exceptions/errors?)
# - use https://github.com/jubos/fake-s3 for more extensive testing
# - keyboard interupt
# - kill of process group vs individual process?
