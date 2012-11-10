#!/usr/bin/env python


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

'''
Unit tests for Zinc.
'''

from __future__ import with_statement
import unittest, sys, os, commands, subprocess
import zinc

TMP_DIR = "/tmp/zinc-unittests"

def run(command):
  print "shell> %s" % command
  code = subprocess.call(command, shell=True)
  return code

def create_file(path, value):
  f = open(path, "w")
  f.write(value)
  f.close()

def create_zeroed_file(path, size_in_mb):
  run("dd if=/dev/zero bs=1000000 count=%s of=%s" % (size_in_mb, path))

class ZincUnitTests(unittest.TestCase):

  def setUp(self):
    if TMP_DIR.startswith("/tmp/"):
      run("rm -rf %s" % TMP_DIR)    
    run("mkdir -p %s" % TMP_DIR)    

  def test_md5(self):
    filename = "%s/somefile" % TMP_DIR
    create_file(filename, "some contents")
    val = zinc.file_md5(filename)
    val2 = commands.getoutput("md5sum %s" % filename).split()[0]
    self.assertEquals(val, val2)

  def test_md5_large(self):
    filename = "%s/bigfile" % TMP_DIR
    create_zeroed_file(filename, 40)
    val = zinc.file_md5(filename)
    val2 = commands.getoutput("md5sum %s" % filename).split()[0]
    self.assertEquals(val, val2)

  def test_shortvals(self):
    s = zinc.shortvals_to_str([("key1", "val1"), ("key2", 23), ("key3", "val3-foobar"), ("skipme", None)])
    d = zinc.shortvals_from_str("key1=val1;key2=23;key3=val3-foobar")
    self.assertEquals(s, zinc.shortvals_to_str(d))
  
  def test_decompose_store_path(self):
    def call(input, expected):
      result = zinc.Repo.decompose_store_path(input)
      self.assertEquals(result, expected)
      
    call(("znversion"), ("meta", "", "znversion"))
    call(("scopes"), ("meta", "", "scopes"))
    call(("_c/0000000000000/manifest"), ("manifest", "", "0000000000000/manifest"))
    call(("_f/0000000000000/filea1"), ("file", "filea1", "0000000000000/filea1"))
    call(("_s/dir1/tags"), ("tags", "dir1", "tags"))
    call(("_s/dir1/_b/main/commits"), ("commits", "dir1", "main/commits"))
    call(("_s/dir1/_f/0000000000001/fileb3"), ("file", "dir1/fileb3", "0000000000001/fileb3"))
    call(("_s/dir1/_s/subdir1/_f/0000000000001/fileb4"), ("file", "dir1/subdir1/fileb4", "0000000000001/fileb4"))
    call(("_s/dir1/_s/subdir1/_s/subdir2/_f/0000000000001/fileb4"), ("file", "dir1/subdir1/subdir2/fileb4", "0000000000001/fileb4"))
    call(("_s/dir1/_s/subdir1/_s/_s/_f/0000000000001/fileb4"), ("file", "dir1/subdir1/_s/fileb4", "0000000000001/fileb4"))

  def test_temp_output_dir(self):
    out_dir = "./out-dir"
    run("rm -rf %s" % out_dir)
    with zinc.temp_output_dir(out_dir) as temp_dir:
      assert os.path.isdir(temp_dir)
      run("mkdir -p %s/subdir/subsubdir" % temp_dir)
      run("mkdir -p %s/empty-subdir" % temp_dir)
      with open("%s/foo" % temp_dir, "w") as f: f.write("blah1")
      with open("%s/bar" % temp_dir, "w") as f: f.write("blah2")
      with open("%s/subdir/baz" % temp_dir, "w") as f: f.write("blah3")
      with open("%s/subdir/subsubdir/other" % temp_dir, "w") as f: f.write("blah4")
      temp_dir_save = temp_dir
      assert sorted(list(zinc.list_files_recursive(temp_dir))) == ["bar", "foo", "subdir/baz", "subdir/subsubdir/other"]
      assert sorted(list(zinc.list_dirs_recursive(temp_dir))) == ['empty-subdir', 'subdir', 'subdir/subsubdir']
    assert sorted(list(zinc.list_files_recursive(out_dir))) == ["bar", "foo", "subdir/baz", "subdir/subsubdir/other"]
    assert not os.path.isdir(temp_dir_save)

  def test_temp_output_file(self):
    out_file = "out-file"
    run("rm -rf %s" % out_file)
    with zinc.atomic_output_file(out_file) as temp_file:
      with open(temp_file, "w") as f: f.write("contents")
    with open(out_file) as f:
      assert f.read() == "contents"

  def test_ignore_path(self):
    ignore_path = zinc.WorkingDir.ignore_path
    assert not ignore_path("foo")
    assert not ignore_path("/foo/bar")
    assert ignore_path("foo.bak")
    assert ignore_path("blah/foo~")
    assert not ignore_path("foo.ba")
    assert ignore_path("foo.orig")
    assert not ignore_path("foo.")
    assert ignore_path(".foo")
    assert ignore_path("blah/blah/.tmp-copy.xxx/abc/def")
    assert not ignore_path("blah/blah/.tmp-keep/abc/def")

  def test_parent_dirs(self):
    assert list(zinc.parent_dirs("a")) == [("a", ""), ("", "a")]
    assert list(zinc.parent_dirs("/a/b/c")) == [("/a/b/c", ""), ("/a/b", "c"), ("/a", "b/c"), ("/", "a/b/c")]
    assert list(zinc.parent_dirs("a/b/c")) == [("a/b/c", ""), ("a/b", "c"), ("a", "b/c"), ("", "a/b/c")]

  def test_compress_lzo(self):
    filename = "%s/somefile" % TMP_DIR
    lzo_filename = filename + ".lzo"
    uncompressed_filename = filename + ".new"
    create_zeroed_file(filename, 5)
    zinc.compress_lzo(filename, lzo_filename)
    zinc.decompress_lzo(lzo_filename, uncompressed_filename)
    code = run("cmp %s %s" % (filename, uncompressed_filename))
    assert code == 0


if __name__ == '__main__':
  suite = unittest.TestLoader().loadTestsFromTestCase(ZincUnitTests)
  test_result = unittest.TextTestRunner(verbosity=2).run(suite)
  print "%s errors, %s failures" % (len(test_result.errors), len(test_result.failures))
  sys.exit(1 if len(test_result.errors) + len(test_result.failures) > 0 else 0)
