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
Zinc: Simple and scalable versioned data storage

Requirements: Python 2.5, Boto 1.3 (or 2.0+ for large file multipart upload support), lzop (for LZO compression)

Authors: Joshua Levy, Srinath Sridhar, Kazuyuki Tanimura
'''

#
# Revision history:
#
# 0.3.18  Add track/untrack commands, checkout --mode option, and status --full option
# 0.3.17  Zinc log command now shows the log from the checked out revision instead of tip.
# 0.3.16  Open source with Apache License.
# 0.3.15  Error for nested working directories. Fix error with copy to local file in local dir.
# 0.3.14  Very minor fixes.
# 0.3.13  Change signal handling; ignore SIGPIPE for better handling of socket errors.
# 0.3.12  Fix: Ignore .tmp-copy directories.
# 0.3.11  Turn on LZO compression by default.
# 0.3.10  Add --no-cache option.
# 0.3.9   Add --mod-all option.
# 0.3.8   Minor cleanup and docs.
# 0.3.7   Fix: Issue where unexpected item in cache aborts an update.
# 0.3.6   Adjust output of "locate" and "status".
# 0.3.5   Read S3 access keys from .s3cfg and environment.
# 0.3.4   Support LZO compression. Repo version 0.3.
#

from __future__ import with_statement
import sys, re, os, subprocess, shutil, hashlib, binascii, random, time, logging, optparse, functools, ConfigParser, calendar, cStringIO, errno
from datetime import datetime
from contextlib import contextmanager

import boto
from boto.s3.connection import S3Connection # XXX seems to be needed to initialize boto module correctly

# Version of this code.
ZINC_VERSION = "0.3.18"
# Version of this repository implementation.
REPO_VERSION = "0.3"
REPO_VERSIONS_SUPPORTED = ["0.2", "0.3"]
BOTO_VERSION = boto.Version

##
## Setup
##

# Directory for temporary files. This should only be used for small files
# (repository file copies are always made to working directory).
TMP_DIR = "/tmp"

# Bits of randomness used in revision ids.
REV_BITS = 64

# Format for displayed dates.
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S UTC'

UNKNOWN_USER = "unknown"
EMPTY_MESSAGE = ""

# Used when keeping a copy of a previous file (as with revert).
BACKUP_SUFFIX = ".orig"
# Name prefix used when creating a temporary directory.
TMP_DIR_PREFIX = ".tmp-copy"

# Used in output to indicate that path preceding is the scope.
SCOPE_SEP = "//"

# Max file size to upload without S3 multipart upload
S3_SINGLEPART_MAX_SIZE = 4000000000
# Chunk size when using S3 multipart upload
S3_MULTIPART_CHUNK_SIZE = 50 << 20

# Environment variable names for S3 credentials.
S3_ACCESS_KEY_NAME = "S3_ACCESS_KEY"
S3_SECRET_KEY_NAME = "S3_SECRET_KEY"

# Metadata header name we use for our own MD5 content hash. We use our own header since S3's etag is not really a reliable MD5 hash.
ZINC_S3_MD5_NAME = "zinc-md5"
INVALID_MD5 = "---"
INVALID_SIZE = -1
INVALID_MTIME = -1.0

# Default scheme for file storage.
SCHEME_DEFAULT = "lzo"

# Compression level for LZO.
LZO_LEVEL = 7


def log_setup(verbosity=1):
  '''Logging verbosity: 1 = normal, 2 = verbose, 3 = debug.'''
  global log
  log = logging.Logger("logger")

  # For convenient access elsewhere
  log.verbosity = verbosity
  log.stream = sys.stderr

  log_handler = logging.StreamHandler(log.stream)
  log.addHandler(log_handler)
  if verbosity == 3:
    log_handler.setFormatter(logging.Formatter('  (%(levelname).1s)%(filename)s:%(lineno)-4d %(message)s', DATETIME_FORMAT))
    log.setLevel(logging.DEBUG)
  elif verbosity == 2:
    log_handler.setFormatter(logging.Formatter('%(message)s', DATETIME_FORMAT))
    log.setLevel(logging.DEBUG)
  else:
    log_handler.setFormatter(logging.Formatter('%(message)s', DATETIME_FORMAT))
    log.setLevel(logging.INFO)

# Always run initially to facilitate use as a library.
log_setup()

def general_setup():
  # TODO fix deprecations about __new__ etc
  import warnings
  warnings.simplefilter("ignore", DeprecationWarning)

  log_setup()

##
## Exceptions
##

class Failure(RuntimeError):
  def __init__(self, message, suppressable=False):
    self.suppressable = suppressable
    RuntimeError.__init__(self, message)

class InvalidArgument(Failure):
  pass

class InvalidOperation(Failure):
  pass

class FileExists(InvalidOperation):
  pass

class ShellError(InvalidOperation):
  pass

class BadState(Failure):
  pass

class MissingFile(Failure):
  pass


##
## Basic utilities
##

class _Enum(object):
  key_dict = {}
  value_dict = {}

  @classmethod
  def has_key(cls, key):
    return key in cls.key_dict

  @classmethod
  def has_value(cls, value):
    return value in cls.value_dict

  @classmethod
  def value_for(cls, key):
    if key in cls.value_dict:
      return cls.key_dict[key]
    else:
      raise InvalidArgument("parse error: not a valid key for %s: %s" % (cls.__name__, key))

  @classmethod
  def key_for(cls, value):
    if value in cls.value_dict:
      return cls.value_dict[value]
    else:
      raise InvalidArgument("parse error: not a valid value for %s: %s" % (cls.__name__, value))

  @classmethod
  def check_value(cls, value):
    cls.key_for(value)
    return value

  @classmethod
  def values(cls):
    return cls.value_dict.keys()

def enum(name, **enum_dict):
  '''
  Create an enum class. Accepts name for the class and key-value pairs for each
  value. The returned type has the given keys as class variables, and some static
  utility methods. By convention, use uppercase for the keys, to avoid name
  conflicts.
  '''
  new_type = type(name, (_Enum,), enum_dict)
  new_type.key_dict = enum_dict
  new_type.value_dict = dict((v, k) for (k, v) in enum_dict.iteritems())
  return new_type

def fail(message=None, prefix="error", exc_info=None, status=1, suppress=False):
  '''Fatal error.'''
  if suppress:
    if message:
      log.info("warning: %s", message)
    sys.exit(0)
  else:
    if message:
      log.error("%s: %s", prefix, message)
    if exc_info and log.verbosity > 1: # Make number 0 to always print stack traces
      log.info("", exc_info=exc_info)
    sys.exit(status)

def log_calls_with(severity):
  '''Create a decorator to log calls and return values of any function, for debugging.'''
  def decorator(fn):
    @functools.wraps(fn)
    def wrap(*params, **kwargs):
      call_str = "%s(%s)" % (fn.__name__, ", ".join([repr(p) for p in params] + ["%s=%s" % (k, repr(v)) for (k, v) in kwargs.items()]))
      # TODO Extract line number from caller and use that in logging.
      log.log(severity, ">> %s", call_str)
      ret = fn(*params, **kwargs)
      log.log(severity, "<< %s: %s", call_str, repr(ret))
      return ret
    return wrap
  return decorator

# Convenience decorators for logging.
log_calls_info = log_calls_with(logging.INFO)
log_calls = log_calls_with(logging.DEBUG)

def split_uri(uri_str):
  '''Split "prefix://some/path" into ("prefix", "some/path").'''
  m = re.match("([a-zA-Z0-9]+)://(\S*)", uri_str)
  if not m:
    raise InvalidArgument("invalid URI: '%s'" % uri_str)
  return (m.group(1), m.group(2))

_WHITESPACE_RE = re.compile("\s+")

def split_whitespace(str):
  '''Split on whitespace.'''
  return re.split(_WHITESPACE_RE, str)

def parse_rev_range(rev_str):
  '''Parse a revision or revision range of the form xxx or xxx:yyy or xxx: or :yyy.'''
  l = [s.strip() for s in rev_str.split(":")]
  if len(l) == 2:
    return (l[0] if l[0] else None, l[1] if l[1] else None)
  elif len(l) == 1:
    return (l[0], None)
  else:
    raise InvalidArgument("invalid revision range: '%s'" % rev_str)

def determine_rev_range(options, config):
  '''
  Takes option parameters and return a (start, end) rev range tuple.
  If a user specifies the rev options, it simply returns the user's rev range.
  In case a user does not specify rev, it tries to return the checkedout rev.
  If the scope is not checkedout, it returns ("tip", None).
  '''
  rev = "tip"
  if options.work_dir:
    rev = WorkingDir(options.work_dir, config).get_checkout_rev(options.scope) or rev
  return parse_rev_range(options.rev) if options.rev else (rev, None)

def join_path(*elements):
  '''Join path elements: a, b, c -> a/b/c. Also handles case where initial path is empty.'''
  if elements and elements[0] == "":
    return join_path(*elements[1:])
  else:
    return "/".join(elements)

_RANDOM = random.Random()
_RANDOM.seed()

def new_uid(bits=64):
  '''
  Return a unique id. Actually just random value with at least the specified
  bits of randomness. Don't make it case sensitive, so we can use it in filenames,
  etc.
  '''
  return "".join(_RANDOM.sample("0123456789abcdefghijklmnopqrstuvwxyz", int(bits / 5.16) + 1))  # log(26 + 10)/log(2) = 5.16

def group_pairs(pairs):
  out = {}
  for (x, y) in pairs:
    if x in out:
      out[x].append(y)
    else:
      out[x] = [y]
  return out

def setattrs(self, others):
  ''' useful for saving others (function properties) as instance variables '''
  for (k, v) in others.iteritems():
    setattr(self, k, v)


##
## File utilities
##

def make_all_dirs(path):
  '''Ensure local dir, with all its parent dirs, are created.'''
  if not os.path.isdir(path):
    os.makedirs(path)

def make_parent_dirs(path):
  '''Ensure parent directories of a file are created as needed.'''
  dir = os.path.dirname(path)
  if dir and dir != "/":
    make_all_dirs(dir)

def set_file_mtime(path, mtime, atime=None):
  '''Set access and modification times on a file.'''
  if not atime:
    atime = mtime
  f = file(path, 'a')
  try:
    os.utime(path, (atime, mtime))
  finally:
    f.close()

def move_to_backup(path, backup_suffix=BACKUP_SUFFIX):
  if backup_suffix and os.path.exists(path):
    shutil.move(path, path + backup_suffix)

def parent_dirs(path):
  '''
  Iterate upward through all possible parent directories of a path: "/a/b/c"
  yields [("/a/b/c", ""), ("/a/b", "c"), ("/a", "b/c"), ("/", "a/b/c")].
  '''
  root = "/" if path.startswith("/") else ""
  path = path.strip("/")
  elements = path.split("/")
  right_elements = []
  while len(elements) > 0:
    parent = "/".join(elements)
    rel_path = "/".join(right_elements)
    yield (root + parent, rel_path)
    right_elements.insert(0, elements.pop())
  yield (root, path)

def new_temp_filename(prefix=None, based_on=None, with_nonce=True):
  '''
  Return a new, temporary filename. If based_on is provided, return a path
  adjacent to the given path, but starting with prefix; otherwise return a name
  from the global temporary directory. If with_nonce is true, include a random
  string to ensure uniqueness. XXX: This is not secure and shouldn't be used for
  privileged execution.
  '''
  if based_on:
    if not prefix:
      prefix = ".tmp"
    # Note dirname(based_on) will be empty if based_on is a path with no path component.
    temp_path = "%s/%s.%s" % (os.path.dirname(based_on) or ".", prefix, os.path.basename(based_on))
  else:
    temp_path = "%s/zinc-tmp" % TMP_DIR
    if prefix:
      temp_path = "%s.%s" % (temp_path, prefix)
  if with_nonce:
    temp_path = "%s.%s" % (temp_path, new_uid())
  return temp_path

@contextmanager
def atomic_output_file(dest_path, make_parents=False, backup_suffix=None, suffix=".partial.%s"):
  '''
  A context manager for convenience in writing a file in an atomic way. Set up
  a temporary name, then rename it after the operation is done, optionally making
  a backup of the previous file, if present.
  '''
  tmp_path = ("%s" + suffix) % (dest_path, new_uid())
  if make_parents:
    make_parent_dirs(tmp_path)
  yield tmp_path
  if not os.path.isfile(tmp_path):
    raise BadState("failure in writing file: %s (temp location %s)" % (dest_path, tmp_path))
  move_to_backup(dest_path, backup_suffix=backup_suffix)
  shutil.move(tmp_path, dest_path)

@contextmanager
def temp_output_file(prefix=None, based_on=None, with_nonce=True):
  '''
  A context manager for convenience in using a temporary file, which is deleted when exiting the context.
  '''
  tmp_path = new_temp_filename(prefix=prefix, based_on=based_on, with_nonce=with_nonce)
  yield tmp_path
  try:
    os.remove(tmp_path)
  except OSError, e:
    pass

def copyfile_atomic(source_path, dest_path, make_parents=False, backup_suffix=None):
  '''Copy file on local filesystem in an atomic way, so partial copies never exist. Preserves timestamps.'''
  with atomic_output_file(dest_path, make_parents=make_parents, backup_suffix=backup_suffix) as tmp_path:
    # TODO catch IOError and re-raise as MissingFile
    shutil.copyfile(source_path, tmp_path)
    set_file_mtime(tmp_path, os.path.getmtime(source_path))

def movefile(source_path, dest_path, make_parents=False, backup_suffix=None):
  '''Move file. With a few extra options.'''
  # TODO catch IOError and re-raise as MissingFile
  if make_parents:
    make_parent_dirs(dest_path)
  move_to_backup(dest_path, backup_suffix=backup_suffix)
  shutil.move(source_path, dest_path)

def read_string_from_file(path):
  '''Read entire contents of file into a string.'''
  with open(path, "rb") as f:
    value = f.read()
  return value

def write_string_to_file(path, string, make_parents=False, backup_suffix=BACKUP_SUFFIX):
  '''Write entire file with given string contents. Keeps backup by default.'''
  with atomic_output_file(path, make_parents=make_parents, backup_suffix=backup_suffix) as tmp_path:
    with open(tmp_path, "wb") as f:
      f.write(string)


def file_md5(path, blocksize=2**23):
  '''Compute MD5 of file, in hex format.'''
  md5 = hashlib.md5()
  size = os.path.getsize(path)
  if size > 2**23:
    log.debug("computing MD5 of file '%s' (size %s)", path, size)
  with open(path, "rb") as f:
    while True:
      data = f.read(blocksize)
      if not data:
        break
      md5.update(data)
  value = binascii.hexlify(md5.digest())
  return value

def strip_prefix(prefix, value):
  '''Strip prefix from string value, or None if value does not start with prefix.'''
  return value[len(prefix):] if value.startswith(prefix) else None

def list_files_recursive(path):
  '''
  Return all filenames, relative to path, in the current directory.
  '''
  for root, dirnames, filenames in os.walk(path):
    for filename in filenames:
      val = strip_prefix(path + "/", join_path(root, filename))
      assert val
      yield val

def list_dirs_recursive(path):
  '''
  Return all subdirectories, relative to path, in the current directory, bottom to top.
  '''
  for root, dirnames, filenames in os.walk(path, topdown=False):
    for dirname in dirnames:
      val = strip_prefix(path + "/", join_path(root, dirname))
      assert val
      yield val

@contextmanager
def temp_output_dir(output_dir, temp_dir=None, backup_suffix=None):
  '''
  If we want to write several files to a given output directory, and we would
  like all to appear at nearly the same time, we can put all files in a temporary
  location (by default a subdirectory of the output directory), and then move
  them to the final desired location at the end. This does not provide perfect
  transactionality, but helps to minimize chances of files being in an
  inconsistent state following an interruption.
  '''
  try:
    if not temp_dir:
      temp_dir = join_path(output_dir, "%s.%s" % (TMP_DIR_PREFIX, new_uid()))
    make_all_dirs(temp_dir)
    yield temp_dir

    # Now copy over all files, preserving paths. Note target may contain some
    # of the same subdirectories, so we have to do this file by file.
    for path in list_files_recursive(temp_dir):
      src = join_path(temp_dir, path)
      dest = join_path(output_dir, path)
      log.debug("moving from temp dir: %s -> %s", src, dest)
      movefile(src, dest, make_parents=True, backup_suffix=backup_suffix)
  finally:
    try:
      for dir in list_dirs_recursive(temp_dir):
        os.rmdir(join_path(temp_dir, dir))
      os.rmdir(temp_dir)
    except Exception, e:
      log.warn("error cleaning up temporary directory: %s: %s", temp_dir, e)

def shell_command(command):
  '''
  Call a shell command, translating common exceptions. Command is a list of
  items to pass to the shell, e.g. ['ls', '-l'].
  '''
  try:
    log.debug("shell command: %s", " ".join(command))
    subprocess.check_call(command, stderr=subprocess.STDOUT, shell=False)
  except subprocess.CalledProcessError, e:
    raise ShellError("shell command '%s' returned status %s" % (" ".join(command), e.returncode))
  except OSError, e:
    raise ShellError("shell command '%s' failed: %s" % (" ".join(command), e))


##
## Date and time utilities
##

def current_repo_time():
  '''
  Time to be used for including in manifests. In the future, could be taken
  from some service to ensure consistency. We use an integer, so the displayed
  time is always as accurate as the recorded time, in case the user specifies
  times on the command line.
  '''
  return int(time.time())

def format_seconds(seconds):
  return datetime.utcfromtimestamp(seconds).strftime(DATETIME_FORMAT)

DATETIME_FORMATS = [
  '%Y-%m-%d %H:%M:%S',
  '%Y-%m-%d %I:%M:%S%p',
  '%Y-%m-%d %H:%M',
  '%Y-%m-%d %I:%M%p',
  '%Y-%m-%d',
  '%a %b %d %H:%M:%S %Y',
  '%a %b %d %I:%M:%S%p %Y',
  '%a, %d %b %Y %H:%M:%S', # GNU coreutils "/bin/date --rfc-2822"
  '%b %d %H:%M:%S %Y',
  '%b %d %I:%M:%S%p %Y',
  '%b %d %H:%M:%S',
  '%b %d %I:%M:%S%p',
  '%b %d %H:%M',
  '%b %d %I:%M%p',
  '%b %d %Y',
  '%b %d',
  '%H:%M:%S',
  '%I:%M:%S%p',
  '%H:%M',
  '%I:%M%p',
]

# Raw date format is seconds since epoch
_DATETIME_SECONDS_RE = re.compile("([0-9]{9,10})")

def parse_timezone(tz_str):
  '''Parse a timezone suffix, as it appears on a date, returning offset in minutes.'''
  try:
    tz_str = tz_str.lower()
    if tz_str[0] in "+-" and len(tz_str) == 5 and tz_str[1:].isdigit():
      sign = 1 if (tz_str[0] == "+") else -1
      hours = int(tz_str[1:3])
      minutes = int(tz_str[3:5])
      return -sign * (60 * hours + minutes)
    elif tz_str == "gmt" or tz_str == "utc":
      return 0
    else:
      return None
  except (ValueError, IndexError):
    return None

def parse_datetime(datetime_str, formats=DATETIME_FORMATS, tz_offset=None, assume_utc=False):
  '''Parse a date in one of various formats, returning seconds since epoch.'''
  if _DATETIME_SECONDS_RE.match(datetime_str):
    return int(datetime_str)

  # Unfornately, strptime doesn't handle timezones well, so we do it ourselves.
  # If the last token looks like a timezone, put the offset into tz_offset.
  if tz_offset is None:
    bits = split_whitespace(datetime_str)
    if len(bits) > 1:
      tz_offset = parse_timezone(bits[-1])
      if tz_offset is not None:
        datetime_str = " ".join(bits[:-1])

  datetime_str = datetime_str.strip()
  for format in formats:
    try:
      tuple = time.strptime(datetime_str, format)
      if tz_offset is not None:
        # Specified offset
        seconds = calendar.timegm(tuple)
        seconds += tz_offset * 60
      elif assume_utc:
        # Unspecified offset, using UTC
        seconds = calendar.timegm(tuple)
      else:
        # Unspecified offset, using local time
        seconds = time.mktime(tuple)
    except (ValueError, OverflowError):
      continue
    break
  else:
    raise InvalidArgument("could not parse date: '%s'" % datetime_str) 
  return seconds

_DATESPEC_RE1 = re.compile("([<>])\s*{(.*)}")
_DATESPEC_RE1A = re.compile("([<>])\s*(.*)")
_DATESPEC_RE2 = re.compile("{(.*)}\s*to\s*{(.*)}")
_DATESPEC_RE2A = re.compile("(.*)\s+to\s+(.*)")

def parse_datespec(datespec_str, assume_utc=False):
  '''
  Parse a date spec and return a filter function that returns true if the
  supplied time satisfies the date spec.

  Formats: 
  < [date expression]
  > [date expression]
  [date expression] to [date expression]
  '''
  datespec_str = datespec_str.strip()
  m = None
  if not m:
    m = _DATESPEC_RE1.match(datespec_str) or _DATESPEC_RE1A.match(datespec_str) 
    if m:
      direction = m.group(1)
      time = parse_datetime(m.group(2), assume_utc=assume_utc)

      if direction == "<":
        filter = lambda t: t <= time
      else:
        filter = lambda t: t >= time
  if not m:
    m = _DATESPEC_RE2.match(datespec_str) or _DATESPEC_RE2A.match(datespec_str)
    if m:
      time1, time2 = parse_datetime(m.group(1)), parse_datetime(m.group(2))
      filter = lambda t: time1 <= t and t <= time2
  if not m:
    raise InvalidArgument("invalid date spec: '%s'" % datespec_str)

  return filter


##
## Common tools
##

# Simple serialization for short key/value pairs. We use our own for clarity, and so key order is deterministic.

def shortvals_to_str(shortvals):
  '''
  Convert [("key1", "val1"), ("key2", "val2")] or {"key1": "val1", "key2": "val2"} to "key1=val1;key2=val2".
  Values that are None are skipped.
  '''
  if type(shortvals) == dict:
    shortvals = [(k, shortvals[k]) for k in sorted(shortvals)]
  for (k, v) in shortvals:
    if ";" in str(k) or "=" in str(k) or (v is not None and (";" in str(v) or "=" in str(v))):
      raise InvalidArgument("invalid shortval string: '%s'" % shortvals)
  return ";".join(["%s=%s" % (str(k), str(v)) for (k,v) in shortvals if v is not None])

def shortvals_from_str(shortvals_str):
  '''
  Convert "key1=val1;key2=val2" to {"key1" : "val1", "key2" : "val2"}.
  '''
  return dict(pairstr.split("=") for pairstr in shortvals_str.split(";"))


class Values(object):
  '''
  Simple serialization for multiple named values. Like a property file, but
  where values are potentially longer multi-line strings.
  '''

  # A magic char sequence to indicate start of a new record 
  RECORD_MAGIC = "%%>>"
  VERSION = "znval 1.0"

  def __init__(self, values=None, comment=None):
    '''
    Create a new Values object. First arg may be a dictionary or list of
    pairs. Items with value None are skipped. If list of pairs is used, order is
    preserved.
    '''
    if not values:
      values = {}
    self.values = []
    if type(values) == dict:
      self.values = [(k, str(values[k])) for k in sorted(values) if values[k] is not None]
    elif type(values) == list:
      self.values = [(k, str(v)) for (k, v) in values if v is not None]
    else:
      raise InvalidArgument("invalid values of type %s" % type(values))
    self.comment = comment

  def from_str(self, str_val):
    lines = str_val.splitlines()
    cur_rec = None
    cur_val = "" # TODO use list
    for line in lines:
      line = line.strip()
      if line.startswith("#"):
        continue
      if line.startswith(self.RECORD_MAGIC):
        if cur_rec:
          self.values.append((cur_rec, cur_val[:-1]))
        cur_rec = line[len(self.RECORD_MAGIC):]
        cur_val = ""
        continue
      cur_val += line
      cur_val += "\n"
    # Drop final newline
    self.values.append((cur_rec, cur_val[:-1]))

  def to_str(self):
    out = "# %s\n" % self.VERSION
    if self.comment:
      for l in self.comment.splitlines():
        out += "# %s\n" % l
    for (key, value) in self.values:
      assert value is not None
      out += self.RECORD_MAGIC + key + "\n"
      out += value
      out += "\n"
    return out

  def clear(self):
    self.values = {}

def values_to_str(values, comment=None):
  '''Serialize dictionary or list of pairs in Values format.'''
  v = Values(values, comment)
  return v.to_str()

def values_from_str(values_str):
  '''Deserialize string in Values format into a dictionary.'''
  v = Values()
  v.from_str(values_str)
  return dict((key, value) for (key, value) in v.values)


class LockService(object):
  '''
  A lock service. Can acquire locks for any key, to make sure write operations
  are globally synchronized across all users.
  '''

  def acquire(self, key):
    raise NotImplementedError()

  def release(self, key):
    raise NotImplementedError()

  @contextmanager
  def context(self, key):
    yield self.acquire(key)
    self.release(key)


class NoLockService(LockService):
  '''
  Unimplemented lock service -- no locking at all is done.
  '''

  def acquire(self, key):
    log.debug("acquiring lock for key: %s", key)
    pass

  def release(self, key):
    log.debug("releasing lock for key: %s", key)
    pass


##
## Repository
##

# Type of an item. Currently we only support files, but in the future there could by symlinks etc.
ItemType = enum("ItemType", FILE="file")

# Storage schemes for items.
Scheme = enum("Scheme", RAW="raw", LZO="lzo")


class Config(object):
  '''
  The Config holds general settings, like the user, the locking service, etc.
  as well as per-repository or per-working directory settings.
  '''
  def __init__(self, options=None):
    if not options:
      options = {}
    self.lock_service = NoLockService()

    # Defaults read from options.
    self.user = options.user if options.user else UNKNOWN_USER
    self.compression = Scheme.check_value(options.compression) if options.compression else SCHEME_DEFAULT

    self.s3_fill_keys(options)
    self.s3_ssl = True if options.s3_ssl else False
    self.no_cache = True if options.no_cache else False

  def merge_in(self, values):
    for (k, v) in values.items():
      log.debug("config: '%s' = %s", k, repr(v))
      setattr(self, k, v)

  def s3_fill_keys(self, options):
    '''Fill in S3 keys into config, from any available sources.'''
    self.s3_access_key = None
    self.s3_secret_key = None
    keys = None
    if hasattr(options, "s3_access_key") and hasattr(options, "s3_secret_key"):
      log.debug("using S3 keys from options")
      keys = (options.s3_access_key, options.s3_secret_key)
    if not keys:
      keys = Config.s3_keys_from_env()
    if not keys and not options.no_s3cfg:
      keys = Config.s3_keys_from_s3cmd()
    if keys:
      self.s3_access_key = keys[0]
      self.s3_secret_key = keys[1]

  @staticmethod
  def s3_keys_from_env():
    '''Retrieve S3 access keys from the environment, or None if not present.'''
    env = os.environ
    if S3_ACCESS_KEY_NAME in env and S3_SECRET_KEY_NAME in env:
      keys = (env[S3_ACCESS_KEY_NAME], env[S3_SECRET_KEY_NAME])
      log.debug("read S3 keys from environment")
      return keys
    else:
      return None

  @staticmethod
  def s3_keys_from_s3cmd():
    '''Retrieve S3 access key settings from s3cmd's config file, if present; otherwise return None.'''
    s3cfg_path = None
    try:
      s3cfg_path = "%s/.s3cfg" % os.environ["HOME"]
      if not os.path.exists(s3cfg_path):
        return None
      config = ConfigParser.ConfigParser()
      config.read(s3cfg_path)
      keys = config.get("default", "access_key"), config.get("default", "secret_key")
      log.debug("read S3 keys from $HOME/.s3cfg file")
      return keys
    except Exception, e:
      log.info("could not read S3 keys from $HOME/.s3cfg file; skipping (%s)", e)
      return None


# Checkout mode for scopes.
# ToBeTracked is a state that a file locally exists and is tracked but not registered in manifest yet
FileState = enum("FileState", Tracked="tracked", Untracked="untracked", ToBeTracked="to_be_tracked")
FileStateTrackedList = [FileState.Tracked, FileState.ToBeTracked] # for convenience

class Fingerprint(object):
  '''
  The fingerprint of an item. Contains modification time (mtime), size, and/or
  MD5. A Fingerprint may be incomplete, and not (initially) include a hash of
  file contents, for efficiency. Generally, if an item's type, mtime, and size
  are unchanged, we assume it is unchanged. This is not perfectly correct since
  modification time has only one-second resolution on many filesystems, but it is
  reasonably safe.
  '''

  DIFFER = "differ"
  SAME = "match"
  UNCERTAIN = "uncertain"

  def __init__(self, type=ItemType.FILE, mtime=None, size=None, md5=None, state=FileState.Tracked, **others):
    self.type = type
    self.mtime = mtime
    self.size = size
    self.md5 = md5
    self.state = state
    # for future compatibility
    setattrs(self, others) # this is tested locally by adding random attributes and they passed through

  def __repr__(self):
    return self.to_str()

  @staticmethod
  def of_file(local_path, md5=None, compute_md5=False, state=FileState.Tracked):
    '''Get a Fingerprint for the item at the local path given. Uses supplied MD5 or computes MD5 if requested.'''
    fp = Fingerprint()
    fp.read_from_file(local_path, md5=md5, compute_md5=compute_md5, state=state)
    return fp
    
  def read_from_file(self, path, md5=None, compute_md5=False, state=FileState.Tracked):
    if not os.path.isfile(path):
      raise InvalidOperation("file not found: '%s'" % path)
    if not os.path.isfile(path):
      raise NotImplementedError("support only plan files: '%s'" % path)
    self.type = ItemType.FILE
    self.size = os.path.getsize(path)
    self.mtime = os.path.getmtime(path)
    if md5:
      self.md5 = md5
    elif compute_md5:
      self.md5 = file_md5(path)
    else:
      self.md5 = None
    self.state = state

  def compare(self, fp):
    '''Compare two FingerprintList. Returns DIFFER, SAME, or UNCERTAIN (if we are missing a needed MD5).'''
    if self.type == ItemType.FILE and fp.type == ItemType.FILE:
      if self.md5 and fp.md5:
        if self.md5 == fp.md5:
          return Fingerprint.SAME
        else:
          return Fingerprint.DIFFER
      elif self.mtime and fp.mtime and (self.size is not None) and (fp.size is not None):
        if self.mtime == fp.mtime and self.size == fp.size:
          return Fingerprint.SAME
        elif self.size != fp.size:
          return Fingerprint.DIFFER
        else:
          return Fingerprint.UNCERTAIN
    else:
      raise NotImplementedError("currently support only plain files")

  def to_str(self):
    return shortvals_to_str(vars(self)) # TODO change this when we need to use some private instance variables

  def from_str(self, vals_str):
    vals = shortvals_from_str(vals_str)
    for (k, v) in vals.iteritems():
      if k == "mtime":
        v = float(v)
      if k == "size":
        v = int(v)
      setattr(self, k, v)

  def invalidate(self):
    if self.md5:
      self.md5 = INVALID_MD5
    if self.size:
      self.size = INVALID_SIZE
    if self.mtime:
      self.mtime = INVALID_MTIME

class FileCache(object):
  '''
  A local cache of files by key (e.g. remote paths). Maintains Fingerprints of
  each. Does not support updates -- only use for immutable items.
  '''

  def __init__(self, root_path, cache_criterion):
    '''
    The cache_criterion is a function that determines whether an item is
    cachable, when called on the key.
    '''
    self.root_path = root_path.rstrip("/")
    self.cache_criterion = cache_criterion
    self.state_path = join_path(root_path, "state")
    self.contents_path = join_path(root_path, "contents")
    make_all_dirs(self.contents_path)
    self._read_state()

  def _read_state(self):
    self.fingerprints = FingerprintList()
    if os.path.isfile(self.state_path):
      log.debug("reading cache state from '%s'", self.state_path)
      self.fingerprints.from_str(values_from_str(read_string_from_file(self.state_path))["items"])
    else:
      log.debug("no file cache state at '%s'", self.state_path)

  @log_calls
  def _write_state(self):
    write_string_to_file(self.state_path,
                         values_to_str([("items", self.fingerprints.to_str())],
                                       comment="File cache for this working directory. Entire enclosing cache/ directory may be deleted and it will be regenerated."))

  def cache_path_for(self, remote_path):
    return join_path(self.contents_path, remote_path)

  @log_calls
  def lookup(self, remote_path):
    '''
    Return cache path for specified item, and whether it is already in the
    cache, in the form (cache_path, fingerprint). If not in cache, fingerprint is
    None.
    '''
    if not self.cache_criterion(remote_path):
      return (None, None)
    else:
      fp = self.fingerprints.lookup_fingerprint(remote_path)
      return (self.cache_path_for(remote_path), fp)

  @log_calls
  def add(self, remote_path, fingerprint):
    '''
    Add item to cache. Item must already be stored in cache path returned
    from lookup().
    '''
    if not self.cache_criterion(remote_path):
      raise AssertionError("should not add '%s' to cache as it does not meet cache criterion" % remote_path)
    cache_path = self.cache_path_for(remote_path)
    log.debug("checking local cache at '%s' for '%s'", cache_path, remote_path)
    if not os.path.isfile(cache_path):
      raise AssertionError("missing cached file at '%s' for '%s'" % (cache_path, remote_path))
    self.fingerprints.update_fingerprint(remote_path, fingerprint)
    self._write_state()


class Store(object):
  '''
  Basic file-type abstraction for a centralized data store.

  Items within a store have conventional paths. Store paths should never begin
  with '/'. They are always relative to root.

  Directory operations (mkdir, etc.) are not handled explicitly. Folders are
  created as needed based on paths.

  The file cache is an optional optimization so downloads are skipped when
  possible.
  '''

  def __new__(cls, root_uri, config, file_cache=None):
    '''
    Perform automatic instantiation of the appropriate store type, based on the URI prefix.
    '''
    (prefix, path) = split_uri(root_uri)
    for subclass in Store.__subclasses__():
      if subclass.prefix == prefix:
        return super(cls, subclass).__new__(subclass, root_uri, config, file_cache=file_cache)
    raise InvalidArgument("unsupported store type: %s" % prefix)

  def __init__(self, root_uri, config, file_cache=None):
    '''
    The root is the location of a store. It is a URI-type string, indicating
    the type of store, e.g. file:///path/to/file or s3://bucket/path.
    '''
    self.root_uri = root_uri.rstrip("/")
    self.lock_service = config.lock_service
    self.file_cache = file_cache

  def __repr__(self):
    return "Store@%s" % self.root_uri

  def read_string(self, store_path):
    temp_filename = new_temp_filename("read_string")
    self.download(store_path, temp_filename)
    with open(temp_filename, "rb") as fp:
      value = fp.read()
    os.remove(temp_filename)
    return value

  def write_string(self, store_path, value, clobber=False):
    temp_filename = new_temp_filename("write_string")
    with open(temp_filename, "wb") as fp:
      fp.write(value)
    self.upload(temp_filename, store_path, clobber=clobber)
    os.remove(temp_filename)

  def append_string(self, store_path, value):
    '''Append data to a file, creating if it does not exist. Takes a lock on the file.'''
    lock_key = join_path(self.root_uri, store_path)
    with self.lock_service.context(lock_key):
      with temp_output_file(prefix="append_string") as temp_filename:
        if self.exists(store_path):
          self.download(store_path, temp_filename)
        with open(temp_filename, "ab") as fp:
          fp.write(value)
        self.upload(temp_filename, store_path, clobber=True)

  def exists(self, store_path):
    '''Check if item exists.'''
    return len(self.list(store_path)) > 0

  def cache(self, store_path, show_info=False):
    '''Fetch item at store_path into local cache, or return item already in cache. Returns (cache_path, fingerprint).'''
    if not self.file_cache:
      return (None, None)
    (cache_path, fp) = self.file_cache.lookup(store_path)
    if not cache_path:
      return (None, None) # Not cachable
    if not fp:
      # Not in cache; download
      if os.path.exists(cache_path):
        log.warn("cache path exists unexpectedly; clobbering: %s", cache_path)
      fp = self.download(store_path, cache_path, clobber=True, backup_suffix=None, show_info=show_info)
      self.file_cache.add(store_path, fp)
    # TODO to avoid writing cache metadata repeatedly, accept a list of items, and add to cache all at once
    return (cache_path, fp)

  def cached_download(self, store_path, local_path, clobber=False, backup_suffix=None, show_info=False):
    '''
    Same as download(), but uses cached copy if appropriate.
    '''
    if not self.file_cache:
      return self.download(store_path, local_path, clobber=clobber, backup_suffix=backup_suffix, show_info=show_info)

    if not clobber and os.path.exists(local_path):
      raise FileExists("download aborted: file exists: %s" % local_path)
    (cache_path, fp) = self.cache(store_path, show_info=show_info)
    if not cache_path:
      return self.download(store_path, local_path, clobber=clobber, backup_suffix=backup_suffix, show_info=show_info)

    # Item is cachable and in cache, so copy it
    if show_info:
      log_fn = log.info
    else:
      log_fn = log.debug

    log_fn("copying from cache: %s from %s", local_path, cache_path)
    # TODO may not want to copy large files, as this doubles disk I/O; maybe
    # add option to move file and purge from cache (this would make diff costly
    # later, of course)
    # TODO download does check MD5, but we should also verify MD5 post-copy from cache
    copyfile_atomic(cache_path, local_path, make_parents=True, backup_suffix=backup_suffix)
    return fp

  def _log_download(self, store_path, local_path, show_info=False):
    if show_info:
      log_fn = log.info
    else:
      log_fn = log.debug
    log_fn("downloading: %s from %s/%s", local_path, self.root_uri, store_path)

  def _log_upload(self, local_path, store_path, show_info=False):
    if show_info:
      log_fn = log.info
    else:
      log_fn = log.debug
    log_fn("uploading: %s to %s/%s", local_path, self.root_uri, store_path)

  # --- abstract store interface ---

  def list(self, store_path):
    '''
    List directory contents, non-recursively. For non-existance, list should
    return empty; for a single file, result is one item; for a directory, all
    items.
    '''
    raise NotImplementedError()

  def download(self, store_path, local_path, clobber=False, backup_suffix=None, show_info=False):
    '''
    Download file, not clobbering by default. Return a Fingerprint for the local downloaded item.
    If backup_suffix is set, keep a backup file with that suffix appended.
    '''
    raise NotImplementedError()

  def upload(self, local_path, store_path, clobber=False, show_info=False):
    '''
    Upload file, not clobbering by default.
    '''
    raise NotImplementedError()


class FileStore(Store):
  '''
  Local file-based store.
  '''
  prefix = "file"

  def __init__(self, root_uri, config, file_cache=None):
    root_uri = root_uri.rstrip("/")
    Store.__init__(self, root_uri, config, file_cache=file_cache)
    self.root_uri = root_uri
    self.root_dir = split_uri(root_uri)[1]

  def _full_path(self, store_path):
    if not store_path.startswith("/"):
      store_path = "/" + store_path
    return self.root_dir + store_path;

  def list(self, store_path):
    full_path = self._full_path(store_path)
    if not os.path.exists(full_path):
      store_paths = []
    elif os.path.isfile(full_path):
      store_paths = [store_path]
    else:
      store_paths = os.listdir(self._full_path(store_path))
    return store_paths

  def download(self, store_path, local_path, clobber=False, backup_suffix=None, show_info=False):
    self._log_download(store_path, local_path, show_info=show_info)
    if not clobber and os.path.exists(local_path):
      raise FileExists("download aborted: file exists: %s" % local_path)
    # XXX For better performance, could copy and compute hash at same time
    copyfile_atomic(self._full_path(store_path), local_path, make_parents=True, backup_suffix=backup_suffix)
    return Fingerprint.of_file(local_path, compute_md5=True)
    
  def upload(self, local_path, store_path, clobber=False, show_info=False):
    self._log_upload(local_path, store_path, show_info=show_info)
    target = self._full_path(store_path)
    if not clobber and os.path.exists(target):
      raise FileExists("upload aborted: file exists: %s" % store_path)
    copyfile_atomic(local_path, target, make_parents=True)


class S3Store(Store):
  '''
  S3-based store.
  '''
  prefix = "s3"

  def __init__(self, root_uri, config, file_cache=None):
    if not config.s3_access_key or not config.s3_secret_key:
      raise Failure("missing S3 access keys: set %s/%s environment variables or add s3_access_key/s3_secret_key to your config" %
        (S3_ACCESS_KEY_NAME, S3_SECRET_KEY_NAME))
    self.connection = boto.s3.connection.S3Connection(config.s3_access_key, config.s3_secret_key, is_secure=config.s3_ssl)
    root_uri = root_uri.rstrip("/")
    Store.__init__(self, root_uri, config, file_cache=file_cache)
    elements = split_uri(root_uri)[1].split('/')
    self.root_uri = root_uri
    self.root_bucket = self.connection.get_bucket(elements[0])
    self.root_dir = '/'.join(elements[1:])

  def s3_copyfile_atomic(self, source_path, dest_path, make_parents=True, backup_suffix=None):
    '''Copy file on local filesystem in an atomic way, so partial copies aren't possible. Return file's MD5.'''
    # TODO catch S3ResponseError and re-raise as MissingFile
    with atomic_output_file(dest_path, make_parents=make_parents, backup_suffix=backup_suffix) as tmp_path:
      k = boto.s3.key.Key(self.root_bucket)
      k.key = join_path(self.root_dir, source_path)
      # TODO We should also validate the MD5 of the local path against what S3
      # lists, ideally computing the MD5 during the download.
      # Boto sets modification time on file.
      k.get_contents_to_filename(tmp_path)

      # Unfortunately, the logic to get the MD5 from S3 is a bit convoluted. First look for our own header.
      md5 = k.get_metadata(ZINC_S3_MD5_NAME)
      if md5:
        log.debug("S3 key %s has zinc MD5 header: %s", k.key, md5)
      else:
        # Otherwise, we can use the etag as well, but S3 only sets this correctly if it wasn't a multipart upload.
        md5 = str(k.etag).strip('"')
        if "-" in md5: # looks like "7387bb45d8098d7e573f7ced50e474cb-1", which is an invalid MD5
          log.info("S3 item does not have a zinc MD5 -- recomputing: %s", source_path)
          md5 = file_md5(tmp_path)
        else:
          log.debug("S3 key %s has etag header: %s", k.key, md5)

    return md5

  def list(self, store_path):
    # For non-existance, list should return empty; for a single file, result is one item; for a directory, all items.
    # Other file exceptions should propagate.
    rs = self.root_bucket.list(join_path(self.root_dir, store_path), delimiter='/')
    return [k.name for k in rs]

  @log_calls
  def download(self, store_path, local_path, clobber=False, backup_suffix=None, show_info=False):
    self._log_download(store_path, local_path, show_info=show_info)
    if not clobber and os.path.exists(local_path):
      raise FileExists("download aborted: file exists: %s" % local_path)
    md5 = self.s3_copyfile_atomic(store_path, local_path, backup_suffix=backup_suffix)
    return Fingerprint.of_file(local_path, md5=md5)

  @log_calls
  def upload(self, local_path, store_path, clobber=False, show_info=False):
    self._log_upload(local_path, store_path, show_info=show_info)

    remote_path = join_path(self.root_dir, store_path)
    if not clobber and self.exists(remote_path):
      raise FileExists("upload aborted: file exists: %s" % store_path)

    fsize = os.path.getsize(local_path)
    local_md5 = file_md5(local_path)
    if fsize < S3_SINGLEPART_MAX_SIZE:
      # Don't use multipart upload unless we need to (simpler, and also we might have an older version of boto)
      k = boto.s3.key.Key(self.root_bucket)
      k.key = remote_path
      k.set_metadata(ZINC_S3_MD5_NAME, local_md5)
      k.set_contents_from_filename(local_path)
    else:
      if not hasattr(self.root_bucket, "initiate_multipart_upload"):
        raise Failure("S3 multipart upload not supported for file of size %s (%s) -- boto library is not current?" % (fsize, local_path))
      mpu = self.root_bucket.initiate_multipart_upload(remote_path, metadata = {ZINC_S3_MD5_NAME: local_md5})
      part = 1
      bytes_done = 0
      try:
        with open(local_path, 'rb') as f:
          # For now this is single threaded; concurrency could speed it up, depending on disk/network performance.
          while bytes_done < fsize:
            log.debug("uploading part %s of %s -> %s", part, local_path, remote_path)
            mpu.upload_part_from_file(cStringIO.StringIO(f.read(S3_MULTIPART_CHUNK_SIZE)), part)
            bytes_done += S3_MULTIPART_CHUNK_SIZE
            part += 1
        cmpu = mpu.complete_upload()
        log.debug("uploading complete %s -> %s: %s (etag %s)", local_path, remote_path, cmpu, cmpu.etag)
      except Exception, e:
        mpu.cancel_upload()
        raise Failure("error uploading '%s' to S3 location '%s': %s" % (local_path, remote_path, e))


class Item(object):
  '''
  Reference to an item, with its logical path and its underlying store path and
  storage scheme. All paths are relative to some scope.
  '''

  def __init__(self, path, store_path, scheme, add_scheme_suffix=True):
    # Path shouldn't have leading or trailing /
    self.path = path.strip("/")
    self.store_path = store_path.strip("/")
    self.scheme = scheme

    # For convenience, we include compression scheme (.lzo, etc.) on the store
    # path as a suffix, so it's a good visual indication that it's compressed
    # in case we access it directly.
    if add_scheme_suffix and self.scheme != Scheme.RAW:
      assert self.scheme != "" and self.scheme.isalnum() and self.scheme.islower()
      self.store_path += "." + self.scheme
    if not Scheme.has_value(self.scheme):
      raise NotImplementedError("file storage scheme not supported (Zinc client out of date?): %s" % self.scheme)

  def __repr__(self):
    return "%s (%s:%s)" % (self.path, self.scheme, self.store_path)

  def display_path(self, prefix=None):
    return "%s%s%s" % (prefix, SCOPE_SEP, self.path) if prefix else self.path

  def full_store_path(self, repo, scope):
    return join_path(repo.store.root_uri, repo.store_path_dir(scope), self.store_path)


class Manifest(object):
  '''
  A list of items in a commit, together with a revision id, user, message,
  date, list of changed files, etc.
  '''

  def __init__(self, rev=None, user=UNKNOWN_USER, message=EMPTY_MESSAGE, items=None, changelist=None, time=None):
    self.rev = rev
    self.user = user
    self.message = message
    self.items = items if items else []
    self.time = time or current_repo_time()
    self.changelist = changelist

  def __repr__(self):
    return "Manifest(rev %s, %d items)" % (self.rev, len(self.items))

  def to_str(self):
    return values_to_str([("rev", self.rev),
                          ("user", self.user),
                          ("message", self.message),
                          ("time", self.time),
                          ("items", self._items_to_str()),
                          ("changelist", self.changelist.to_str() if self.changelist else None)
                         ], comment="manifest")

  def from_str(self, mf_str):
    values = values_from_str(mf_str)
    self.rev = values["rev"]
    self.user = values["user"]
    self.message = values["message"]
    self.time = float(values["time"])
    self._items_from_str(values["items"])
    if "changelist" in values:
      self.changelist = Changelist()
      self.changelist.from_str(values["changelist"])

  def to_summary(self, verbose=True):
    '''A readable summary of the manifest. If verbose, will include all items.'''
    out = []
    out.append("rev: %s %s\nuser: %s\nmessage: %s\n" % (self.rev, format_seconds(self.time), self.user, self.message))
    if verbose:
      if self.changelist is not None:
        out.append("changes: %s\n" % self.changelist.to_summary(sep=", ").rstrip(", "))
      # out.append("items: %s\n" % " ".join([item.path for item in self.items]))
    return "".join(out)

  def get_paths(self):
    return [item.path for item in self.items]

  def get_items(self, paths):
    '''Look up items from manifest. Returns list with None in place of any not found items.'''
    item_dict = dict((item.path, item) for item in self.items)
    return [(item_dict[p] if p in item_dict else None) for p in paths]

  def get_item(self, path):
    # TODO make faster: switch to using dict in memory for self.items (and sorting on serialization)
    return self.get_items([path])[0]

  def get_listing(self, path, recursive=False):
    '''Return items from manifest below path, which should be a directory.'''
    path = path.strip("/")
    prefix = path + "/" if path else ""
    match_rec = lambda p: (p.startswith(prefix) or p == path)
    # TODO list subdirectories too; this only lists files within a folder
    match_nonrec = lambda p: (p.startswith(prefix) and ('/' not in p[len(prefix):]) or p == path)
    match_func = match_rec if recursive else match_nonrec
    return [item for item in self.items if match_func(item.path)]
    
  def _items_to_str(self):
    out = []
    for item in self.items:
      # For backward compatibility, we use "file" instead of "file.raw".
      type_and_scheme = "%s.%s" % (ItemType.FILE, item.scheme)
      if item.scheme == Scheme.RAW:
        type_and_scheme = ItemType.FILE
      out.append("%s\t%s\t%s\n" % (type_and_scheme, item.path, item.store_path))
    return "".join(out)

  def _items_from_str(self, items_str):
    '''Each line is of the form: type.scheme<tab>path<tab>store_path'''
    for line in items_str.splitlines():
      (type_and_scheme, path, store_path) = line.split("\t")
      bits = type_and_scheme.split(".")
      # For backward compatibility, we use "file" instead of "file.raw".
      if bits[0] != ItemType.FILE:
        raise NotImplementedError("manifest entry type '%' not supported (old version of Zinc client?)" % type_and_scheme)
      if len(bits) == 1:
        (type, scheme) = (bits[0], Scheme.RAW)
      else:
        (type, scheme) = (bits[0], bits[1])
      self.items.append(Item(path, store_path, scheme, add_scheme_suffix=False))


class Changelist(object):
  '''
  A list of paths that have changed.
  '''

  Status = enum("Status", ADDED = "A", REMOVED = "R", MODIFIED = "M")

  def __init__(self, add_paths=None, rm_paths=None, mod_paths=None):
    self.add_paths = set(add_paths) if add_paths else set()
    self.rm_paths = set(rm_paths) if rm_paths else set()
    self.mod_paths = set(mod_paths) if mod_paths else set()

  def __repr__(self):
    return "Changelist@%x(%s)" % (hash(self), self.brief_summary())

  def update(self, path, status):
    if status == Changelist.Status.ADDED:
      self.add_paths.add(path)
    elif status == Changelist.Status.REMOVED:
      self.rm_paths.add(path)
    elif status == Changelist.Status.MODIFIED:
      self.mod_paths.add(path)
    else:
      raise InvalidArgument("invalid status: %s" % status)

  def status_dict(self):
    status = {}
    for p in self.add_paths:
      status[p] = Changelist.Status.ADDED
    for p in self.rm_paths:
      status[p] = Changelist.Status.REMOVED
    for p in self.mod_paths:
      status[p] = Changelist.Status.MODIFIED
    return status 

  def all_paths(self):
    return self.add_paths.union(self.rm_paths).union(self.mod_paths)

  def intersect(self, other_changelist):
    '''Return a changeset that lists only adds/removes/modifies that overlap with other_changelist's paths.'''
    int_changeset = Changelist(add_paths=self.add_paths, rm_paths=self.rm_paths, mod_paths=self.mod_paths)
    int_changeset.add_paths.intersection_update(other_changelist.all_paths())
    int_changeset.rm_paths.intersection_update(other_changelist.all_paths())
    int_changeset.mod_paths.intersection_update(other_changelist.all_paths())
    return int_changeset

  def difference(self, other_changelist):
    '''Return a new changelist with elements in the changelist that are not in the other changelist.'''
    int_changeset = Changelist(add_paths=self.add_paths, rm_paths=self.rm_paths, mod_paths=self.mod_paths)
    int_changeset.add_paths.difference_update(other_changelist.all_paths())
    int_changeset.rm_paths.difference_update(other_changelist.all_paths())
    int_changeset.mod_paths.difference_update(other_changelist.all_paths())
    return int_changeset

  def discard(self, path):
    self.add_paths.discard(path)
    self.rm_paths.discard(path)
    self.mod_paths.discard(path)
    
  def is_empty(self):
    return len(self.add_paths) == 0 and len(self.rm_paths) == 0 and len(self.mod_paths) == 0

  def brief_summary(self):
    if len(self.add_paths) + len(self.rm_paths) + len(self.mod_paths) == 0:
      return "no changes"
    else:
      return "%s adds, %s rms, %s mods" % (len(self.add_paths), len(self.rm_paths), len(self.mod_paths))

  def to_summary(self, prefix=None, sep="\n"):
    '''A readable summary of the change list.'''
    status = self.status_dict()
    out = []
    prefix = prefix + SCOPE_SEP if prefix else ""
    for p in sorted(status):
      out.append("%s %s%s%s" % (status[p], prefix, p, sep))
    return "".join(out)

  def to_str(self):
    status = self.status_dict()
    out = []
    for p in sorted(status):
      out.append("%s\t%s\n" % (status[p], p))
    return "".join(out)

  @log_calls
  def from_str(self, changelist_str):
    self.add_paths = []
    self.rm_paths = []
    self.mod_paths = []
    for line in changelist_str.splitlines():
      (status, path) = line.split("\t")
      if status == Changelist.Status.ADDED:
        self.add_paths.append(path)
      elif status == Changelist.Status.REMOVED:
        self.rm_paths.append(path)
      elif status == Changelist.Status.MODIFIED:
        self.mod_paths.append(path)
      else:
        raise InvalidArgument("invalid status: %s" % status)


def compress_lzo(src_path, target_path):
  with atomic_output_file(target_path, make_parents=True) as temp_target_path:
    shell_command(["lzop", "-%s" % LZO_LEVEL, "-o", temp_target_path, src_path])

def decompress_lzo(src_path, target_path):
  with atomic_output_file(target_path, make_parents=True) as temp_target_path:
    shell_command(["lzop", "-d", "-o", temp_target_path, src_path])

def compress_file(src_path, target_path, scheme):
  log.debug("compressing %s to %s with scheme %s", src_path, target_path, scheme)
  if scheme == Scheme.RAW:
    copyfile_atomic(src_path, target_path)
  elif scheme == Scheme.LZO:
    compress_lzo(src_path, target_path)
  else:
    raise NotImplementedError("storage scheme '%s' not supported (old version of Zinc client?)" % scheme)

def decompress_file(src_path, target_path, scheme):
  if scheme == Scheme.RAW:
    copyfile_atomic(src_path, target_path)
  elif scheme == Scheme.LZO:
    decompress_lzo(src_path, target_path)
  else:
    raise NotImplementedError("storage scheme '%s' not supported (old version of Zinc client?)" % scheme)
  # TODO Avoid recomputing MD5.
  fp = Fingerprint.of_file(target_path, compute_md5=True)
  return fp

@contextmanager
def compressed_local_file(local_path, scheme):
  '''Return a compressed version of the given file according to scheme, available within a context.'''
  if scheme == Scheme.RAW:
    # Don't bother copying file.
    yield local_path
  else:
    compressed_path = new_temp_filename(prefix=".tmp.compressed", based_on=local_path)
    compress_file(local_path, compressed_path, scheme)
    yield compressed_path
    # TODO For better performance, optionally accept a FileCache and move the compressed file to the cache.
    os.remove(compressed_path)   

class Fetcher(object):
  '''
  A utility to store to and fetch from the repository, compressing and
  decompressing if necessary.
  '''

  def __init__(self, store):
    self.store = store

  def fetch_item(self, scope, item, local_path, target_is_dir=False, clobber=False, backup_suffix=BACKUP_SUFFIX, show_info=True):
    remote_dir = Repo.store_path_dir(scope)
    remote_path = join_path(remote_dir, item.store_path)
    if target_is_dir:
      local_path = join_path(local_path, item.path)

    if item.scheme == Scheme.RAW:
      # Avoid extra copying when compression is off.
      fp = self.store.cached_download(remote_path, local_path, clobber=clobber, backup_suffix=backup_suffix, show_info=True)
    else:
      with temp_output_file(prefix=".tmp.compressed-download", based_on=local_path) as compressed_path:
        self.store.cached_download(remote_path, compressed_path, clobber=clobber, backup_suffix=backup_suffix, show_info=True)
        fp = decompress_file(compressed_path, local_path, item.scheme)
    return fp

  def store_item(self, scope, item, local_dir, show_info=True):
    local_path = join_path(local_dir, item.path)
    store_path = join_path(Repo.store_path_dir(scope), item.store_path)
    with compressed_local_file(local_path, item.scheme) as compressed_path:
      self.store.upload(compressed_path, store_path, show_info=show_info)


class Repo(object):
  '''
  TODO: Nested scopes as described here aren't implemented. We only currently
  support the special case where a repository has just a single list of disjoint
  scopes. This is workable for many situations. All other features below are
  working.

  Basics:
  - "Items" are like files stored in a directory tree of folders, and each has
    a unique logical path.
  - Folders may be specially marked as "scopes." Every item has a unique scope,
    which is the _first_ (smallest) enclosing folder marked as a scope.
  - Items are changed in "commits", atomic operations that affect multiple
    items.
  - Two items are "synchronized" if it is possible to commit changes to them
    together in a single transaction.
  - A commit is described by a manifest: a list of items with the same scope,
    and a list of subscopes -- scopes below the current scope.
  - All items within the manifest are synchronized with each other, but not
    with any item in any subscope.
  - A checkout corresponds to all items in a manifest, plus checkouts of each
    subscope listed in the manifest.
  - Internally, items may be stored according to a compression "scheme." As a
    visual convenience, store paths that are compressed have an extra, indicative
    suffix (e.g. ".lzo" for LZO).

  Operation:
  - Begin with one scope, the root.
  - Can mark any folder as a new scope -- this is must be done with care, and
    should elicit a warning if there is already content in the scope. The
    implication is, you are carving an an "independent" tree of files.
  - Commits must be to files that are all in the same scope.
  - You might commit files that reside in multiple scopes, but this is actually
    multiple commits, one for each scope. (In this case, scopes may be
    disjoint, or one may reside inside another.) Because the items in each commit
    are not synchronized, there are no guarantees about the order in which will
    they will occur.

  Concurrency:
  - Reads never require locks.
  - Data integrity is maintined during interruptions. Additional spurious 
    files may be present after an interruption, but this is harmless, as final
    writing of manifests and of new commits is atomic.
  - Atomic write operations only require a lock briefly on a single
    file. We don't modify multiple objects within an atomic transaction.
  - Repository integrity is always correct for local file repositories. For S3,
    repository integrity in the US Standard region is "eventual." It is
    immediate in other regions that have read-after-write consistency. See
    http://aws.amazon.com/s3/faqs/#What_data_consistency_model_does_Amazon_S3_employ

  Repo format:
  Given the files below, suppose we first commit filea1, then fileb[2-5] at
  once, then decide to mark dir1 and then dir2 as scopes, then add filec6, then
  add filed7.

  filea1
  fileb2
  dir1/fileb3
  dir1/subdir1/fileb4
  dir1/subdir1/filec6
  dir2/subdir2/fileb5
  dir3/filed7

  Then the structure will be (rev numbers are sequential here but are actually
  random unique ids):

  znversion
  scopes ** (all scopes in entire repository)
  _b/main/commits ** (holds list of commits for main branch)
  _c/0000000000000/manifest  (contains filea's)
  _c/0000000000001/manifest  (contains filea's and fileb's)
  _c/0000000000002/manifest  (removes items in dir1, for partition of dir1, and replaces it with a dir entry)
  _c/0000000000003/manifest  (removes items in dir2, for partition of dir2, and replaces it with a dir entry)
  _c/0000000000005/manifest  (add filed7)
  _f/0000000000000/filea1
  _f/0000000000001/fileb2
  _s/dir1/tags ** (holds tag name to revision mapping)
  _s/dir1/_b/main/commits **
  _s/dir1/_c/0000000000002/manifest  (for partition of dir1)
  _s/dir1/_c/0000000000004/manifest  (add filec6)
  _s/dir1/_f/0000000000001/fileb3
  _s/dir1/_s/subdir1/_f/0000000000001/fileb4
  _s/dir1/_s/subdir1/_f/0000000000004/filec6
  _s/dir2/tags **
  _s/dir2/_b/main/commits **
  _s/dir2/_c/0000000000003/manifest  (for partition of dir2)
  _s/dir2/_s/subdir2/_f/0000000000001/fileb5
  _s/dir3/_f/0000000000005/filed7

  Items that are modified are marked with ** (so require locking). All other
  items are written once only.
  '''

  def __init__(self, root_uri, config, check_valid=True, file_cache=None):
    '''
    The file_cache may be None to disable use of a file cache. The check_valid
    flag skips checking the repository for validity and is used to bootstrap a new
    repository.
    '''
    log.debug("repository at %s", root_uri)
    self.store = Store(root_uri, config, file_cache=file_cache)
    self.fetcher = Fetcher(self.store)
    self.config = config
    if check_valid:
      self.check_root()

  def __repr__(self):
    return "Repo@%s" % self.store

  @log_calls
  def check_root(self):
    '''Verify root is a valid data store.'''
    try:
      version_info = values_from_str(self.store.read_string(self._store_path_version()))
      found_repo_version = version_info["repo_version"]
      if found_repo_version not in REPO_VERSIONS_SUPPORTED:
        raise InvalidArgument("this code supports repository versions %s but this repository is version %s: %s" % (REPO_VERSIONS_SUPPORTED, found_repo_version, self.store.root_uri))
    except Exception, e:
      raise InvalidOperation("repository not valid at %s (expecting repo_version %s) (%s)" % (self.store.root_uri, REPO_VERSION, str(e).strip()))
    log.debug("valid repository, version %s", found_repo_version)

  @contextmanager
  def scope_lock(self, scope):
    '''
    A context manager to acquire a lock on the given scope. This is only needed
    for write operations involving multiple steps (i.e. commits).
    '''
    lock_key = join_path(self.store.root_uri, self.store_path_dir(scope))
    with self.config.lock_service.context(lock_key) as value:
      yield value

  @log_calls
  def init_repo(self):
    '''Initialize a new repository.'''
    version_str = values_to_str({"repo_version": REPO_VERSION})
    self.store.write_string(self._store_path_version(), version_str)

  @log_calls
  def create_scope(self, path, user=None, message=None, time=None):
    '''Initialize a new scope at the given path.'''
    # Initialize a scope by creating an empty commit
    # XXX In the future, could support adding scopes over existing content.
    # Then this first commit could include any existing content, too, and we'd
    # walk up the tree and remove those items from the parent manifest.
    if not user:
      user = UNKNOWN_USER
    if not message:
      message = "new scope at '%s'" % path
    if self.get_tip(path, expect=False) is not None:
      raise InvalidOperation("scope already exists: %s" % path, suppressable=True)
    rev = self._commit_items(path, None, [], Changelist(), user=user, message=message, time=time)
    # XXX There is a minor transactionality issue if we get interrupted before
    # append_string call -- scope will exist but not be recorded -- but this is
    # very unlikely and the consequnces of it are fairly minor (newscope will
    # abort next time and scope will have to be manually cleaned up).
    self.store.append_string(self._store_path_scopes(), path + "\n")
    log.info("new scope '%s' at revision %s", path, rev)

  def get_scopes(self):
    '''Return the list of all scopees for the repository.'''
    scopes_file = self._store_path_scopes()
    if not self.store.exists(scopes_file):
      raise InvalidOperation("scopes file not present: %s", scopes_file)
    scopes = sorted(self._parse_scopes(self.store.read_string(self._store_path_scopes())))
    return scopes

  def get_revs(self, scope, expect=True):
    '''Return the list of all revisions at this scope, in order. Returns None if scope is not valid.'''
    commits_file = self._store_path_commits(scope)
    if not self.store.exists(commits_file):
      log.debug("get_revs: commits file not present: %s", commits_file)
      return None
    revs = self._parse_commits(self.store.read_string(self._store_path_commits(scope)))
    return revs

  @log_calls
  def get_tags(self, scope, srev_start=None, srev_end=None, filter=None):
    '''
    Return the list of all tags at this scope, each in the form (tag, rev).
    Output is ordered historically, based on srev_start and srev_end. Multiple tags
    may be returned for a single revision. Returns empty if expect is false and
    scope is not valid.
    '''
    tags_file = self._store_path_tags(scope)
    if not self.store.exists(tags_file):
      log.debug("get_tags: tags file not present: %s", tags_file)
      # TODO should change so we always make empty tags file, and use this to raise exception only if invalid scope 
      return

    # Find all tags in the desired range (if supplied) that also match the filter (if supplied).
    # The range implies the direction.
    all_tags = self._parse_tags(self.store.read_string(self._store_path_tags(scope)))
    tag_dict = group_pairs((rev, name) for (name, rev) in all_tags)
    revs = self._rev_range(scope, srev_start, srev_end)
    for rev in revs:
      if rev in tag_dict:
        for tag in sorted(tag_dict[rev]):
          if not filter or filter(self.get_manifest(scope, rev)):
            yield (tag, rev)

  def _add_tag(self, scope, tag_name, rev):
    '''Write new tag to commit list.'''
    scope = scope.strip("/")
    # TODO validate rev is valid, catch duplicates
    # append_string handles locking
    self.store.append_string(self._store_path_tags(scope), rev + "\t" + tag_name + "\n")

  @staticmethod
  def _parse_tags(tags_str):
    tags = [tuple(line.split("\t")) for line in tags_str.splitlines()]
    return [(name, rev) for (rev, name) in tags]

  def get_tip(self, scope, expect=True):
    '''
    Return the tip revision.  Returns None if expect is false and scope is not valid.
    '''
    revs = self.get_revs(scope)
    if revs:
      tip = revs[-1]
    else:
      tip = None
    if expect and tip is None:
      raise InvalidArgument("invalid scope: '%s'" % scope)
    return tip

  @log_calls
  def get_manifest(self, scope, srev):
    '''Get the manifest at scope for given revision.'''
    rev = self.resolve_rev(scope, srev)
    mf = Manifest()
    mf.from_str(self.store.read_string(self._store_path_manifest(scope, rev)))
    return mf

  @log_calls
  def fetch_items(self, scope, items, local_dir, clobber=True, backup_suffix=None):
    '''
    Download list of items to a local directory. The items should have paths relative to scope,
    and local_dir should correspond to the scope. Returns a FingerprintList of items.
    '''
    out = {}
    # Fetch all files to a temporary directory, then move them to final locations together.
    with temp_output_dir(local_dir, backup_suffix=backup_suffix) as temp_dir:
      for item in items:
        fp = self.fetcher.fetch_item(scope, item, temp_dir, target_is_dir=True, clobber=clobber, backup_suffix=backup_suffix, show_info=True)
        out[item.path] = fp
    return FingerprintList(fingerprints=out)

  @log_calls
  def copy(self, scope, path, srev, local_path, clobber=False, recursive=False, backup_suffix=None):
    '''
    Download files from the repository directly.
    '''
    mf = self.get_manifest(scope, srev)
    if recursive:
      items = mf.get_listing(path, recursive=True)
      if not items:
        raise Failure("no items in path '%s' for revision '%s' of scope '%s' (try 'copy -a' instead?)" % (path, mf.rev, scope), suppressable=True)
      self.fetch_items(scope, items, local_path, clobber=clobber)
    else:
      if os.path.isdir(local_path):
        local_path = join_path(local_path, os.path.basename(path))
      item = mf.get_item(path)
      if not item:
        raise InvalidOperation("item '%s' not found in revision '%s' of scope '%s'" % (path, mf.rev, scope))
      self.fetcher.fetch_item(scope, item, local_path, target_is_dir=False, clobber=clobber, backup_suffix=backup_suffix, show_info=True)
    return mf.rev

  @log_calls
  def list(self, scope, path, srev, recursive=False):
    '''
    List files in the repository directly. Returns list of Items.
    '''
    mf = self.get_manifest(scope, srev)
    items = mf.get_listing(path, recursive=recursive)
    if not items:
      raise Failure("no items in path '%s' for revision '%s' of scope '%s' (try a recursive listing instead?)" % (path, srev, scope), suppressable=True)
    return items

  @log_calls
  def locate(self, scope, path, srev):
    '''
    Locate the store path of a single file in the repository. Returns (scheme, store_uri).
    '''
    items = self.list(scope, path, srev, recursive=False)
    assert len(items) == 1
    return (items[0].scheme, items[0].full_store_path(self, scope))

  @log_calls
  def commit(self, scope, local_dir, prev_rev, changelist, user=UNKNOWN_USER, message=EMPTY_MESSAGE, time=None):
    '''
    Write a new commit. Scope is the scope we are committing at, local_dir is a
    local directory containing all files, changelist is the set of changes with
    respect to prev_rev. All paths in changelist are relative to local_dir, which
    would typically be the local copy of files at scope.

    Returns the new revision.

    Currently, this will abort if prev_rev is not the tip.
    '''
    with self.scope_lock(scope):
      tip_rev = self.get_tip(scope)
      if prev_rev != tip_rev:
        raise InvalidOperation("working directory (%s) is out of date with new tip (%s) for scope '%s' (must update and re-commit)" % (prev_rev, tip_rev, scope))
      prev_mf = self.get_manifest(scope, prev_rev)
      return self._commit_items(scope, local_dir, prev_mf.items, changelist, user, message, time=time)

  @log_calls
  def _commit_items(self, scope, local_dir, prev_items, changelist, user=UNKNOWN_USER, message=EMPTY_MESSAGE, time=None):
    new_rev = self._new_rev(scope)
    mf = Manifest(rev=new_rev, user=user, message=message, changelist=changelist, time=time)
    (new_items, changed_items) = self._apply_changelist_to_items(prev_items, changelist, new_rev)
    mf.items = new_items
    new_paths = [item.path for item in changed_items]
    self._commit_from_manifest(scope, mf, new_paths, local_dir)
    return new_rev

  def _rev_range(self, scope, srev_start=None, srev_end=None):
    '''Get all revisions in a range. Time direction is inferred. Either or both may be None.'''
    rev_start = self.resolve_rev(scope, srev_start) if srev_start else srev_start
    rev_end = self.resolve_rev(scope, srev_end) if srev_end else srev_end

    all_revs = self.get_revs(scope)

    if not all_revs:
      raise InvalidArgument("unknown scope: '%s'" % scope)

    # Default is to go backward in time through whole list
    if not rev_start and not rev_end:
      rev_start_index = len(all_revs) - 1
      rev_end_index = 0
    if rev_start:
      try:
        rev_start_index = all_revs.index(rev_start)
      except ValueError, e:
        raise InvalidArgument("unknown revision %s for scope '%s'" % (rev_start, scope))
      # If rev_start is set and not rev_end, go backward in time ("tip:" is backwards from tip)
      if not rev_end:
        rev_end_index = 0
    if rev_end:
      try:
        rev_end_index = all_revs.index(rev_end)
      except ValueError, e:
        raise InvalidArgument("unknown revision %s for scope '%s'" % (rev_end, scope))
      # If rev_end is set and not rev_start, go forward in time (":tip" is forwards from start)
      if not rev_start:
        rev_start_index = 0

    log.debug("log for revs %s through %s (indices %s through %s out of %s)", rev_start, rev_end, rev_start_index, rev_end_index, len(all_revs))
    if rev_start_index <= rev_end_index:
      revs = all_revs[rev_start_index : rev_end_index + 1]
    else:
      revs = reversed(all_revs[rev_end_index : rev_start_index + 1])

    return revs

  @log_calls
  def log(self, scope, stream, srev_start=None, srev_end=None, filter=None):
    '''
    Write log of commits for scope to output stream. By default, logs run in
    order from srev_start to srev_end, inclusive, forward or backward in time, as
    appropriate. Default is backward in time.
    '''
    revs = self._rev_range(scope, srev_start, srev_end)

    for rev in revs:
      mf = self.get_manifest(scope, rev)
      if filter and not filter(mf):
        continue
      if not mf:
        log.warn("unknown revision: %s", rev)
        continue
      stream.write(mf.to_summary())
      # TODO add tag names to log listing
      stream.write("\n")
    
  @log_calls
  def tag(self, scope, tag_name, srev='tip'):
    '''
    Add a named tag for the specified revision.
    '''
    rev = self.resolve_rev(scope, srev)
    self._add_tag(scope, tag_name, rev)

  @log_calls
  def _apply_changelist_to_items(self, items, changelist, new_rev):
    '''
    Given a list of items, apply adds, removes, and modifications, updating
    store paths for each new item. Returns (new_items, changed_items), where
    new_items is the full list of items, with modifications, and changed_items
    includes only those that have changed (due to adds or modifies).
    '''
    all_paths = set([item.path for item in items])
    assert changelist.rm_paths.issubset(all_paths)
    assert changelist.mod_paths.issubset(all_paths)
    assert len(changelist.add_paths.intersection(all_paths)) == 0
      
    new_items = []
    changed_items = []
    for item in items:
      if item.path in changelist.rm_paths:
        log.debug("changes: removing: %s", item)
        pass
      elif item.path in changelist.mod_paths:
        new_item = Item(item.path, self._store_path_file(item.path, new_rev), self.config.compression)
        log.debug("changes: modifying: %s", new_item)
        new_items.append(new_item)
        changed_items.append(new_item)
      else:
        log.debug("changes: unchanged: %s", item)
        new_items.append(item)

    for path in changelist.add_paths:
      new_item = Item(path, self._store_path_file(path, new_rev), self.config.compression)
      log.debug("changes: adding: %s", new_item)
      new_items.append(new_item)
      changed_items.append(new_item)

    # Sort results so manifests etc. look pretty
    new_items.sort(key=lambda item: item.path)
    changed_items.sort(key=lambda item: item.path)

    return (new_items, changed_items)

  def _commit_from_manifest(self, scope, mf, new_paths, local_dir):
    '''
    Commit a new manifest. Manifest should list all paths (relative to scope)
    and have store paths for each item. The local_dir should be the local base of
    the scope, and contain copies of all new files.
    '''
    new_paths = set(new_paths)
    # First upload the changed files
    for item in mf.items:
      if item.path in new_paths:
        self.fetcher.store_item(scope, item, local_dir, show_info=True)
      else:
        log.debug("not uploading unchanged item: %s", item)
    # Next upload the manifest.
    self.store.write_string(self._store_path_manifest(scope, mf.rev), mf.to_str())
    # Finally write the commit. After this the commit is live. Interruptions
    # prior to this will just leave unused files in the store.
    self._add_commit(scope, mf.rev)

  def _add_commit(self, scope, rev):
    '''Write rev to commit list.'''
    scope = scope.strip("/")
    # append_string handles locking
    self.store.append_string(self._store_path_commits(scope), rev + "\n")

  @log_calls
  def resolve_rev(self, scope, srev, expect=True):
    '''Convert a possibly symbolic revision to a raw revision.'''
    # To improve speed, don't resolve if it's already a raw revision
    if Repo._is_rev(srev):
      return srev

    if srev == "tip":
      rev = self.get_tip(scope)
    else:
      rev = srev
    tags = dict(self.get_tags(scope))
    if tags and srev in tags:
      rev = tags[srev]
      log.debug("resolved srev '%s' to %s", srev, rev)
    # TODO validate rev is a known revision
    if expect and not rev:
      raise InvalidArgument("unknown revision '%s' for scope '%s'" % (srev, scope))
    return rev

  @staticmethod
  def _new_rev(scope):
    '''Return a new rev for a new commit at this scope.'''
    return new_uid(REV_BITS)

  REV_RE = re.compile("[a-z0-9]{%s}" % len(new_uid(REV_BITS)))

  @staticmethod
  def _is_rev(srev):
    '''String is a raw revision, not a symbolic name.'''
    return Repo.REV_RE.match(srev) is not None

  @staticmethod
  def _parse_commits(commits_str):
    return commits_str.splitlines()

  @staticmethod
  def _parse_scopes(scopes_str):
    return scopes_str.splitlines()

  @staticmethod
  def store_path_dir(path):
    '''The store directory for a given path: dir1/dir2 -> _s/dir1/_s/dir2'''
    if path == "":
      return ""
    else:
      path = path.strip("/")
      elements = path.split("/")
      return "".join(["/_s/" + p for p in elements]).lstrip("/")

  @staticmethod
  def _store_path_commits(scope, branch="main"):
    '''
    The store path to the commits file for the given scope:
    dir1/dir2 -> _s/dir1/_s/dir2/_b/main/commits
    '''
    return ("%s/_b/%s/commits" % (Repo.store_path_dir(scope), branch)).lstrip("/")

  @staticmethod
  def _store_path_tags(scope):
    '''
    The store path to the commits file for the given scope:
    dir1/dir2 -> _s/dir1/_s/dir2/tags
    '''
    return (Repo.store_path_dir(scope) + "/tags").lstrip("/")

  @staticmethod
  def _store_path_file(path, rev):
    '''
    The store path to file storage for the given file:
    dir1/dir2/filename -> _s/dir1/_s/dir2/_f/filename
    '''
    path = path.strip("/")
    elements = path.split("/")
    filename = elements[-1]
    dirname = "/".join(elements[:-1])
    return ("%s/_f/%s/%s" % (Repo.store_path_dir(dirname), rev, filename)).lstrip("/")

  @staticmethod
  def _store_path_manifest(scope, rev):
    return ("%s/_c/%s/manifest" % (Repo.store_path_dir(scope), rev)).lstrip("/")

  @staticmethod
  def _store_path_scopes():
    return "scopes"

  @staticmethod
  def _store_path_version():
    return "znversion"

  @staticmethod
  def store_path_is_cachable(path):
    (kind, _parents, _details) = Repo.decompose_store_path(path)
    return kind == "manifest" or kind == "file"

  @staticmethod
  def decompose_store_path(path):
    '''
    Takes a store path and identifies type, regular file path and details, in
    the form (kind, path, details).
    '''
    def classify(pieces, parents):
      head = pieces[0]
      if head in ["znversion", "scopes"]:
        assert len(pieces) == 1
        return ("meta", parents, head)
      elif head == 'tags':
        assert len(pieces) == 1
        return ("tags", parents, "tags")
      elif head == '_b':
        assert len(pieces) == 3
        return ("commits", parents, pieces[1:])
      elif head == '_c':
        assert len(pieces) == 3
        return ("manifest", parents, pieces[1:])
      elif head == '_f':
        assert len(pieces) == 3
        return ("file", parents + [pieces[-1]], pieces[1:])
      elif head == '_s':
        return classify(pieces[2:], parents + [pieces[1]])
      else:
        raise AssertionError("invalid store path: %s" % pieces)

    (kind, parents, details) = classify(path.strip("/").split("/"), [])

    parents = join_path(*parents)
    if type(details) == list:
      details = join_path(*details)
    return (kind, parents, details)
      

##
## Working directory
##

# Checkout mode for scopes.
Mode = enum("Mode", AUTO="auto", PARTIAL="partial")

class Checkout(object):
  '''A checkout of a scope.'''
  def __init__(self, scope, rev, mode, **others):
    self.scope = scope
    self.rev = rev
    self.mode = mode
    # for future compatibility
    setattrs(self, others) # this is tested locally by adding random attributes and they passed through

  def __repr__(self):
    return "%s at %s with mode %s" % (self.scope, self.rev, self.mode)


class CheckoutList(object):
  '''A set of checkouts.'''
  def __init__(self, checkout_list=None):
    if not checkout_list:
      checkout_list = []
    self.clear()
    for checkout in checkout_list:
      self.add(checkout)

  def __repr__(self):
    return "CheckoutList(%s scopes)" % len(self.checkouts)

  def clear(self):
    self.checkouts = {}

  def add(self, checkout):
    self.checkouts[checkout.scope] = checkout

  def has_scope(self, scope):
    return scope in self.checkouts

  def update_rev(self, scope, rev):
    if self.has_scope(scope):
      self.checkouts[scope].rev = rev
    else:
      self.checkouts[scope] = Checkout(scope, rev, Mode.AUTO)

  def rev(self, scope):
    return self.checkouts[scope].rev if scope in self.checkouts else None

  def as_list(self):
    return sorted(list(self.checkouts.itervalues()), key=(lambda c: c.scope))

  def to_str(self):
    out = []
    for scope in sorted(self.checkouts.keys()):
      out.append(shortvals_to_str(vars(self.checkouts[scope]))) # TODO change this when we need to use some private instance variables
    return "\n".join(out)

  def from_str(self, checkouts_str):
    self.clear()
    for line in checkouts_str.splitlines():
      line = line.strip()
      if line and not line.startswith("#"):
        if "\t" in line:
          # for older format
          # XXX Remove this at some point.
          (scope, rev) = line.split("\t")
          self.checkouts[scope] = Checkout(scope, rev, Mode.AUTO)
        else:
          init_property = shortvals_from_str(line)
          self.checkouts[init_property["scope"]] = Checkout(**init_property)


class FingerprintList(object):
  '''
  A set of local file paths with Fingerprints. Paths are relative to working
  directory root, and span all scopes.
  '''

  def __init__(self, fingerprints=None):
    '''We keep self.fingerprints as a dictionary keyed by local path (relative to some root).'''
    self.clear()
    if fingerprints:
      self.fingerprints = fingerprints

  def __repr__(self):
    return "FingerprintList(%s items)" % len(self.fingerprints)

  def clear(self):
    self.fingerprints = {}

  def lookup_fingerprint(self, local_path):
    return self.fingerprints[local_path] if local_path in self.fingerprints else None

  def update_fingerprint(self, local_path, fingerprint):
    self.fingerprints[local_path] = fingerprint

  def fill_md5s(self, other_fingerprints):
    '''Fill in MD5s from all matching fingerprints in another FingerprintList.'''
    count = 0
    other_fps = other_fingerprints.fingerprints
    for (path, fp) in self.fingerprints.iteritems():
      if path in other_fps:
        other_fp = other_fps[path]
        if other_fp.md5 and fp.compare(other_fp) == Fingerprint.SAME:
          fp.md5 = other_fp.md5
          count = count + 1
    log.debug("filled in MD5s for %s/%s files", count, len(self.fingerprints))
    return count

  def compute_md5s(self, root_path):
    '''Compute MD5s for all files that are missing them.'''
    count = 0
    for (path, fp) in self.fingerprints.iteritems():
      if not fp.md5:
        full_path = join_path(root_path, path)
        log.debug("computing MD5 for file: %s", full_path)
        new_fp = Fingerprint.of_file(full_path, compute_md5=True)
        if new_fp.compare(fp) == Fingerprint.SAME:
          # Add MD5, and also update time, to make it less likely we'll need to recompute
          fp.md5 = new_fp.md5
          fp.mtime = new_fp.mtime
        else:
          raise BadState("fingerprint '%s' does not match old fingerprint '%s' of file (bug or file modified by another process?): '%s'" % (new_fp, fp, full_path))
        count = count + 1
    log.debug("computed MD5s for %s/%s files", count, len(self.fingerprints))
    return count

  @staticmethod
  def of_files(root_path, path_list, compute_md5=False, state=FileState.Tracked):
    '''Return a FingerprintList for a list of files relative to root_path. Files must exist.'''
    fingerprints = FingerprintList()
    for path in path_list:
      full_path = join_path(root_path, path)
      fp = Fingerprint.of_file(full_path, compute_md5=compute_md5, state=state)
      fingerprints.update_fingerprint(path, fp)
    return fingerprints

  def to_str(self):
    out = []
    for path in sorted(self.fingerprints.keys()):
      fp = self.fingerprints[path]
      out.append("%s\t%s\n" % (path, fp))
    return "".join(out)

  def from_str(self, checkout_state_str):
    self.clear()
    # TODO use a parse_tsv utility
    for line in checkout_state_str.splitlines():
      line = line.strip()
      if line and not line.startswith("#"):
        (path, fingerprint_str) = line.split("\t")
        fp = Fingerprint()
        fp.from_str(fingerprint_str)
        self.update_fingerprint(path, fp)


class CheckoutState(object):
  '''
  The current state of the working directory's checkouts: the checked out
  revision for each scope, and fingerprints for all original files. All
  Fingerprints in a CheckoutState should be complete, with hashes. This is
  stored in .zinc/checkout-state.
  '''

  def __init__(self, work_dir, checkouts=None, fingerprints=None):
    if not checkouts:
      checkouts = CheckoutList()
    if not fingerprints:
      fingerprints = FingerprintList()
    self.work_dir = work_dir
    self.checkouts = checkouts
    self.fingerprints = fingerprints

  def __repr__(self):
    return "CheckoutState(%s with %s checkouts of %s files)" % (self.work_dir, len(self.checkouts.checkouts), len(self.fingerprints.fingerprints))

  def _check_file_exists(self, local_path):
    full_path = join_path(self.work_dir, local_path)
    if not os.path.isfile(full_path):
      if os.path.exists(full_path):
        raise InvalidOperation("only regular files are supported: '%s'" % full_path)
      else:
        raise InvalidOperation("missing file: '%s'" % full_path)

  def has_scope(self, scope):
    return self.checkouts.has_scope(scope)

  def scope_mode(self, scope, mode=None):
    if mode is None:
      # get scope mode
      return self.checkouts.checkouts[scope].mode if self.has_scope(scope) else None
    else:
      # update scope mode
      if self.has_scope(scope):
        self.checkouts.checkouts[scope].mode = mode
      else:
        raise InvalidOperation("scope '%s' has not been checkedout" % scope)

  def for_subdir(self, scope):
    '''Return all items within the given directory scope (or other directory prefix). Returned paths are trimmed of the scope.'''
    fingerprints = FingerprintList()
    prefix = scope.strip("/") + "/"
    for (local_path, fp) in self.fingerprints.fingerprints.iteritems():
      if local_path.startswith(prefix):
        if fp.state == FileState.Tracked:
          fingerprints.update_fingerprint(local_path[len(prefix):], fp)
        elif fp.state == FileState.ToBeTracked and self.scope_mode(scope) == Mode.PARTIAL:
          full_path = join_path(self.work_dir, local_path)
          if not os.path.exists(full_path):
            log.warn("Warning: '%s' is newly tracked but missing in the working directory; please untrack it." % full_path)
    return fingerprints

  def update_checkout_rev(self, scope, rev):
    self.checkouts.update_rev(scope, rev)

  def update_item(self, scope, path, fingerprint):
    local_path = join_path(scope, path)
    self._check_file_exists(local_path) # sanity check
    self.fingerprints.update_fingerprint(local_path, fingerprint)

  def update_fingerprints(self, scope, fingerprints):
    for (path, fp) in fingerprints.fingerprints.iteritems():
      self.update_item(scope, path, fp)

  def update_from_changelist(self, scope, changelist, new_fingerprints):
    '''
    Update checkout state to reflect adds/removes/modifies. New and modified
    files must have fingerprints in new_fingerprints.
    '''
    for path in changelist.add_paths:
      self.update_item(scope, path, new_fingerprints.fingerprints[path])
    for path in changelist.mod_paths:
      self.update_item(scope, path, new_fingerprints.fingerprints[path])
    for path in changelist.rm_paths:
      self.delete_item(scope, path)

  def delete_item(self, scope, path):
    local_path = join_path(scope, path)
    self.fingerprints.fingerprints.pop(local_path)

  def to_str(self):
    for (path, fp) in self.fingerprints.fingerprints.iteritems():
      if not fp or not fp.md5:
        raise AssertionError("missing Fingerprint or MD5 when serializing FingerprintList: %s: %s" % (path, fp))
    return values_to_str([("checkouts", self.checkouts.to_str()),
                          ("fingerprints", self.fingerprints.to_str())],
                          comment="Checkout state for this working directory, for all currently checked out scopes.")

  def from_str(self, checkout_state_str):
    values = values_from_str(checkout_state_str)
    self.checkouts = CheckoutList()
    self.checkouts.from_str(values["checkouts"])
    self.fingerprints = FingerprintList()
    self.fingerprints.from_str(values["fingerprints"])


def changelist_for_items(old_items, new_items):
  '''
  Compute Changelist on repository side: Given two lists of Items, return a
  Changelist with the adds, removes, and modifies needed to get to the second.
  Items with different store paths are considered changed.
  '''
  changelist = Changelist()

  old_dict = dict((item.path, item.store_path) for item in old_items)
  new_dict = dict((item.path, item.store_path) for item in new_items)

  # Added files
  for new_item in new_items:
    if new_item.path not in old_dict:
      changelist.update(new_item.path, Changelist.Status.ADDED)
  # Modified and removed files
  for old_item in old_items:
    if old_item.path not in new_dict:
      changelist.update(old_item.path, Changelist.Status.REMOVED)
    elif old_item.store_path != new_dict[old_item.path]:
      changelist.update(old_item.path, Changelist.Status.MODIFIED)

  return changelist

@log_calls
def changelist_for_fingerprints(old_fingerprints, new_fingerprints, fill_md5s=True, lazy_md5s=False, root_path=None, expect_exact=True, mod_all=False):
  '''
  Compute Changelist on working directory side: Given two FingerprintLists,
  return a Changelist with the adds, removes, and modifies needed to get to the
  second. Fingerprints may or may not have hashes. If fill_md5 is set, fill in
  hashes in both directions into FingerprintLists. If lazy_md5 is set, compute
  MD5 as needed to resolve uncertainties. If expect_exact is set, raise exception
  if any needed hashes are missing. If mod_all is set, skip all fingerprint
  checks and mark all files as modified.
  '''
  changelist = Changelist()

  if lazy_md5s and not root_path:
    raise InvalidOperation("root_path required to compute MD5s")

  if fill_md5s:
    old_fingerprints.fill_md5s(new_fingerprints)
    new_fingerprints.fill_md5s(old_fingerprints)
  
  # These are dictionaries of full path -> fingerprint
  old_dict = old_fingerprints.fingerprints
  new_dict = new_fingerprints.fingerprints

  # Added files
  for new_path in new_dict.keys():
    if new_path not in old_dict:
      changelist.update(new_path, Changelist.Status.ADDED)
  # Modified and removed files
  for old_path in old_dict.keys():
    if old_path not in new_dict:
      changelist.update(old_path, Changelist.Status.REMOVED)
    else:
      if mod_all:
        changelist.update(old_path, Changelist.Status.MODIFIED)
      else:
        old_fp = old_dict[old_path]
        new_fp = new_dict[old_path]
        c = old_fp.compare(new_fp)
        log.debug("fingerprint compare: %s: %s: %s %s", old_path, c, old_fp, new_fp)
        if c == Fingerprint.UNCERTAIN and lazy_md5s:
          if not old_fp.md5:
            old_fp.read_from_file(join_path(root_path, old_path), compute_md5=True)
          if not new_fp.md5:
            new_fp.read_from_file(join_path(root_path, old_path), compute_md5=True)
          c = old_fp.compare(new_fp)
          log.debug("lazy compare: %s: %s", old_path, c)
        if c == Fingerprint.DIFFER:
          changelist.update(old_path, Changelist.Status.MODIFIED)
        elif c == Fingerprint.UNCERTAIN:
          if lazy_md5s or expect_exact:
            raise AssertionError("should have computed MD5s in all Fingerprints already (old=%s, new=%s, lazy=%s, exact=%s)" % (old_fp, new_fp, lazy_md5s, expect_exact))
          else:
            log.debug("assuming uncertain file is modified: %s", old_path)
            changelist.update(old_path, Changelist.Status.MODIFIED)

  return changelist


class WorkingDir(object):
  '''
  Keep track of the state of local copy of files, for purposes of status,
  commits, etc.
  '''

  def __init__(self, cur_dir, config):
    (self.work_dir, _rel_path) = self.work_dirs_for_path(cur_dir, require=True)[0]
    self.config = config
    # The main config is a combination of global settings, plus the config
    # settings in the "main" section of the work dir config file
    wconfig = WorkingDir._work_dir_config(self.work_dir)
    if wconfig:
      config.merge_in(dict(wconfig.items("main")))
    if not config.root_uri:
      raise InvalidOperation("could not find repository root URI for directory: %s", cur_dir)
    if config.no_cache:
      file_cache = None
    else:
      file_cache = FileCache(self._path_for_file_cache(self.work_dir), cache_criterion=Repo.store_path_is_cachable)
    self.repo = Repo(config.root_uri, self.config, file_cache=file_cache)
    self._read_checkout_state()

  def __repr__(self):
    return "WorkingDir@%s" % self.work_dir

  def _get_relative_paths(self, file_list, scope):
    relative_paths = []
    prefix = join_path(self.work_dir, scope) + "/"
    for path in file_list:
      if path.startswith(prefix):
        relative_paths.append(path.replace(prefix, '', 1))
      else:
        relative_paths.append(path)
    return relative_paths

  def _walk_files(self, scope, force_mode=Mode.PARTIAL, print_untracked=False):
    '''
    Return all local files in scope recursively, returning relative paths.
    force_mode=Mode.PARTIAL avoids to return untracked files iff the scope is in partial mode.
    '''
    all_files = []
    scope_dir = join_path(self.work_dir, scope)
    log.debug("walking tree: %s", scope_dir)
    not_partial = not (force_mode == Mode.PARTIAL and self.get_scope_mode(scope) == Mode.PARTIAL)
    for root, dirnames, filenames in os.walk(scope_dir):
      for filename in filenames:
        full_path = join_path(root, filename)
        if not self.ignore_path(full_path) and (not_partial or self._is_tracking(scope, strip_prefix(self.work_dir + "/" + scope + "/", full_path), print_untracked=print_untracked)):
          all_files.append(full_path)
    return self._get_relative_paths(all_files, scope)

  def _get_absolute_path(self, scope, path):
    return join_path(self.work_dir, scope, path)

  def _delete_file(self, scope, filename, backup_suffix=None):
    log.info("deleting: %s", self._get_absolute_path(scope, filename))
    path = self._get_absolute_path(scope, filename)
    if backup_suffix:
      move_to_backup(path, backup_suffix=backup_suffix)
    else:
      os.remove(path)

  @contextmanager
  def _yield_fingerprint(self, scope, path):
    '''Expose fingerprint of an item specified with scope and path.'''
    local_path = join_path(scope, path)
    fp = self.checkout_state.fingerprints.lookup_fingerprint(local_path)
    yield (fp, local_path)
    if fp is not None:
      self.checkout_state.fingerprints.update_fingerprint(local_path, fp)

  def _read_checkout_state(self):
    # XXX Remove this at some point.
    legacy_path = join_path(self.work_dir, ".zinc", "status_info")
    if os.path.exists(legacy_path):
      raise InvalidOperation("unfortunately, this working directory is obsolete for this version of Zinc; please check out a fresh working directory")

    checkout_state = CheckoutState(self.work_dir)
    filename = self._path_for_checkout_state(self.work_dir)
    if os.path.exists(filename):
      log.debug("reading checkout state from %s", filename)
      checkout_state.from_str(read_string_from_file(filename))
    else:
      log.debug("no checkout state present")
    self.checkout_state = checkout_state

  def _write_checkout_state(self):
    filename = self._path_for_checkout_state(self.work_dir)
    log.debug("writing checkout state %s to %s", self.checkout_state, filename)
    # log.info("checkout state is\n%s", self.checkout_state.to_str())
    write_string_to_file(filename, self.checkout_state.to_str())

  def _is_tracking(self, scope, path, print_untracked=False):
    '''Check whether or not the full_path is tracked'''
    assert self.checkout_state
    tracking = False
    with self._yield_fingerprint(scope, path) as (fp, local_path):
      if fp is not None:
        tracking = fp.state in FileStateTrackedList
      if not tracking and print_untracked:
        log.info("Untracked local file: %s" % local_path)
    return tracking

  def checkouts(self, expect_nonempty=True):
    '''The current list of checkouts.'''
    checkouts = self.checkout_state.checkouts.as_list()
    if expect_nonempty and len(checkouts) < 1:
      raise InvalidOperation("no checked out scopes found", suppressable=True)
    return checkouts

  def get_checkout_rev(self, scope):
    return self.checkout_state.checkouts.rev(scope)

  def get_scope_mode(self, scope):
    return self.checkout_state.scope_mode(scope)

  def update_scope_mode(self, scope, mode):
    self.checkout_state.scope_mode(scope, mode)
  
  @log_calls
  def checkout(self, scope, srev="tip", force=False, mode=Mode.AUTO):
    '''
    Check out data at scope into working directory. If there are any local
    changes, then do not perform a checkout unless forced. Working directory
    status info is updated to reflect checked out revision.
    '''

    # Make sure scope and revision are valid, before mutating working directory
    self.repo.resolve_rev(scope, srev)

    self._ensure_dot_dir_present(self.work_dir)
    local_scope_path = join_path(self.work_dir, scope)
    make_all_dirs(local_scope_path)

    if mode is None:
      mode = Mode.AUTO
    mf = self.update(scope, srev=srev, force=force, is_checkout=True, mode=mode)

    return mf

  def _fingerprint_status(self, scope, compute_md5=False, force_mode=Mode.PARTIAL, print_untracked=False):
    '''
    Return FingerprintLists for original contents and current files.  Original
    contents will always have hashes. All hashes will be recomputed for current
    files if compute_md5 is set.
    '''
    if not self.checkout_state.has_scope(scope):
      raise InvalidArgument("scope is not checked out: '%s'" % scope)
    old_fingerprints = self.checkout_state.for_subdir(scope)
    
    file_list = self._walk_files(scope, force_mode=force_mode, print_untracked=print_untracked)
    local_scope_path = join_path(self.work_dir, scope)
    new_fingerprints = FingerprintList.of_files(local_scope_path, file_list, compute_md5=compute_md5)

    return (old_fingerprints, new_fingerprints)

  @log_calls
  def status(self, scope, mod_all=False, force_mode=Mode.PARTIAL, full=False):
    '''Return a Changelist for the given scope, or None if scope not recognized.'''
    if full:
      log.info("%s: scope '%s' at revision %s in tracking mode '%s'", "status", scope, self.get_checkout_rev(scope), self.get_scope_mode(scope))
    local_scope_dir = join_path(self.work_dir, scope)
    (old_fingerprints, new_fingerprints) = self._fingerprint_status(scope, compute_md5=False, force_mode=force_mode, print_untracked=full)
    changelist = changelist_for_fingerprints(old_fingerprints, new_fingerprints, lazy_md5s=True, root_path=local_scope_dir, mod_all=mod_all)
    # TODO: Store newly computed fingerprints in a cache, so we needn't compute them next time
    return changelist

  def _update_local_files(self, scope, items, changelist, delete_all=False, delete_items=False, backup_suffix=None):
    '''
    Update local files that appear in changelist, downloading adds/modifies and
    optionally deleting removed files. Returns FingerprintList, with hashes. Only
    specified items are updated. Delete policy is either to delete nothing,
    delete all rms in changelist, or delete only rms that are listed in items.
    '''
    # Apply changes. First adds and modifies.
    items_to_fetch = []
    for item in items:
      if item.path in changelist.mod_paths or item.path in changelist.add_paths:
        items_to_fetch.append(item)
    local_scope_path = join_path(self.work_dir, scope)
    new_fingerprints = self.repo.fetch_items(scope, items_to_fetch, local_scope_path, clobber=True, backup_suffix=backup_suffix)
    # Now deletes. (It's nice to do these last just in case we abort earlier.)
    if delete_all:
      for path in sorted(list(changelist.rm_paths)):
        self._delete_file(scope, path, backup_suffix=backup_suffix)
    elif delete_items:
      for item in items:
        if item.path in changelist.rm_paths:
          self._delete_file(scope, item.path, backup_suffix=backup_suffix)
    return new_fingerprints

  @log_calls
  def update(self, scope, srev="tip", force=False, is_checkout=False, mode=None):
    '''
    Update working directory. Checkouts are also implemented here, as if they were updates from an empty manifest.
    mode is only for checkout
    '''
    partial_mode = mode == Mode.PARTIAL
    is_partial_checkout = is_checkout and partial_mode

    # Get manifest for working dir
    cur_rev = self.get_checkout_rev(scope)
    if is_checkout:
      if cur_rev and not force:
        raise InvalidOperation("scope '%s' is already checked out; use update instead" % scope)
      old_mf = Manifest()
    else:
      if mode is not None:
        raise InvalidOperation("mode option '%s' is used with update" % mode)
      old_mf = self.repo.get_manifest(scope, cur_rev) if cur_rev else Manifest()
    # Get new manifest. Doing this first also validates scope and srev.
    new_mf = self.repo.get_manifest(scope, srev)

    # Find out what files we need to update.
    changelist = changelist_for_items(old_mf.items, new_mf.items) if not is_partial_checkout else Changelist()
    log.debug("update from %s to %s, changing %s", old_mf, new_mf, changelist)

    # If in partial tracking mode, keep only FileState.Tracked files
    if partial_mode or self.get_scope_mode(scope) == Mode.PARTIAL:
      for path in changelist.all_paths():
        with self._yield_fingerprint(scope, path) as (fp, local_path):
          if fp is None or fp.state not in FileStateTrackedList:
            if fp is not None:
              fp.invalidate() # we have to invalidate this fingerprint since the remote file might be updated.
            changelist.discard(path)

    # Check for local uncommitted changes. If there is no overlap, the update is safe.
    status_changelist = self.status(scope) if not is_checkout else Changelist()
    overlap_changelist = status_changelist.intersect(changelist)
    log.debug("update changelist:\n%s", changelist.to_summary())
    log.debug("status changelist:\n%s", status_changelist.to_summary())
    if not overlap_changelist.is_empty():
      log.error("local changes conflict with update for scope '%s':", scope)
      log.stream.write(overlap_changelist.to_summary())
      raise InvalidOperation("Zinc does not currently support automatic merges: must revert locally changed files, update, and then manually merge")

    log.info("%s: scope '%s' at revision %s (%s files total)",
             "checkout" if is_checkout else "update", scope, new_mf.rev, len(new_mf.items))
    conflict_str = "no conflicting local changes" if status_changelist.is_empty() else "have %s of non-conflicting local changes" % status_changelist.brief_summary()
    if not changelist.is_empty():
      log.info("updating files: %s (%s)", changelist.brief_summary(), conflict_str)

    # Update and delete files
    new_fingerprints = self._update_local_files(scope, new_mf.items, changelist, delete_all=True)

    # Changes to files are done. Update checkout state. We already have hashes in new_fingerprints.
    self.checkout_state.update_from_changelist(scope, changelist, new_fingerprints)
    self.checkout_state.update_checkout_rev(scope, new_mf.rev)
    if is_partial_checkout:
      self.update_scope_mode(scope, Mode.PARTIAL)
    self._write_checkout_state()

    return new_mf

  @log_calls
  def track(self, scope, track_path_list=[], mode=None, no_download=False):
    '''Update working directory. track_path_list is a list of filenames to track.'''
    # If mode is unspecified, use the mode that the given scope currently in
    if mode is None:
      partial_mode = self.get_scope_mode(scope) == Mode.PARTIAL
      if not partial_mode and track_path_list:
        raise InvalidOperation("scope %s is not in tracking mode %s; cannot track files" % (scope, Mode.PARTIAL))
    else:
      partial_mode = mode == Mode.PARTIAL and (len(track_path_list) > 0 or self.get_scope_mode(scope) != Mode.PARTIAL)
    partial2auto_mode = self.get_scope_mode(scope) == Mode.PARTIAL and mode == Mode.AUTO and len(track_path_list) == 0
    # Error checking
    if not (partial_mode or partial2auto_mode):
      raise InvalidOperation("scope %s is already in %s mode; specyfing mode %s does not make much sense" % (scope, Mode.AUTO, mode))
    # Get manifest for working dir
    cur_rev = self.get_checkout_rev(scope)
    mf = self.repo.get_manifest(scope, cur_rev)

    # Find out what files we need to update.
    changelist = changelist_for_items([], mf.items)
    log.debug("update from %s, changing %s", mf, changelist)
    track_changelist = Changelist()
    # Filter down the changelist if checking out / track only specified files
    if partial_mode:
      self.update_scope_mode(scope, Mode.PARTIAL)
      log.info("%s: scope '%s' at revision %s in tracking mode '%s'", "track", scope, mf.rev, Mode.PARTIAL)
      for path in track_path_list:
        with self._yield_fingerprint(scope, path) as (fp, local_path):
          if fp is not None and fp.state in FileStateTrackedList:
            raise InvalidOperation("scope %s path %s is already %s" % (scope, path, fp.state))
        track_changelist.update(path, Changelist.Status.ADDED)
      changelist = changelist.intersect(track_changelist)

    # if changing mode from auto to partial, register it
    if partial2auto_mode:
      self.update_scope_mode(scope, Mode.AUTO)
      log.info("%s: scope '%s' at revision %s in tracking mode '%s'", "track", scope, mf.rev, Mode.AUTO)

    # We need any local file changes even we are not tracking them in order to avoid overwriting
    # the files that have the same name in the scope; I.e., use mode=Mode.AUTO when in partial_mode
    status_changelist = self.status(scope, force_mode=Mode.AUTO)
    # mod_changelist is for files that manifest has but locally modified or deleted
    mod_changelist = status_changelist.intersect(changelist)
    # add_changelist is for files that manifest does no have but locally newlly added
    add_changelist =  track_changelist.difference(changelist)
    # retrack_changelist is for files that has been untracked but brought back to be tracked
    retrack_changelist = changelist.difference(mod_changelist)
    retrack_changelist_paths = retrack_changelist.all_paths()
    for path in retrack_changelist_paths:
      with self._yield_fingerprint(scope, path) as (fp, local_path):
        if fp is not None and fp.state in FileStateTrackedList:
          # do not include paths that are already tracked since the file should be there
          self.checkout_state._check_file_exists(local_path) # sanity check
          retrack_changelist.discard(path)
    log.debug("update changelist:\n%s", changelist.to_summary())
    log.debug("status changelist:\n%s", status_changelist.to_summary())
    log.debug("modified changelist:\n%s", mod_changelist.to_summary())
    log.debug("added changelist:\n%s", add_changelist.to_summary())
    log.debug("retrack changelist:\n%s", retrack_changelist.to_summary())

    # Update mod_changelist checkout state; do not update local files
    if mod_changelist.is_empty():
      log.info("no conflicting local changes")
    else:
      log.info("preserving files: %s (assuming these changes in working directory are ahead of those in repository)", mod_changelist.brief_summary())
      for path in mod_changelist.all_paths():
        with self._yield_fingerprint(scope, path) as (fp, local_path):
          if fp is None:
            assert False # since it is picked-uped by status change, there must be a fingerprint
          fp.state = FileState.Tracked
          fp.md5 = INVALID_MD5 # for precaution since this md5 could be outdated. No need to invalidate size. This file is marked as MODIFIED or REMOVED anyway.

    # Update add_changelist checkout state; do no update local files
    if not add_changelist.is_empty():
      add_changelist_paths = add_changelist.all_paths()
      for path in add_changelist_paths:
        local_path = join_path(scope, path)
        self.checkout_state._check_file_exists(local_path) # sanity check
      log.info("newly tracking: %s (files that are not in repository yet)", add_changelist.brief_summary())
      local_scope_path = join_path(self.work_dir, scope)
      new_fingerprints = FingerprintList.of_files(local_scope_path, add_changelist_paths, compute_md5=True, state=FileState.ToBeTracked)
      self.checkout_state.update_from_changelist(scope, add_changelist, new_fingerprints)

    # Update retrack files
    if retrack_changelist.is_empty():
      if no_download:
        log.warn("Warning: --no-download is used, even though there is nothing to download")
    else:
      log.info("updating files: %s", retrack_changelist.brief_summary())
      if no_download:
        for path in retrack_changelist_paths:
          local_path = join_path(scope, path)
          fp = Fingerprint(md5=INVALID_MD5, mtime=INVALID_MTIME, size=INVALID_SIZE, state=FileState.Tracked) # fake MD5 in order to pass sanity checks later
          self.checkout_state.fingerprints.update_fingerprint(local_path, fp)
      else:
        new_fingerprints = self._update_local_files(scope, mf.items, retrack_changelist, delete_all=True)
        # Changes to files are done. Update checkout state. We already have hashes in new_fingerprints.
        self.checkout_state.update_from_changelist(scope, retrack_changelist, new_fingerprints)

    self._write_checkout_state()

    return mf

  @log_calls
  def untrack(self, scope, untrack_path_list, mode):
    if Mode.PARTIAL not in [self.get_scope_mode(scope), mode]:
      raise InvalidOperation("scope %s is not in %s tracking mode; cannot untrack files" % (scope, Mode.PARTIAL))
    # make sure this scope is in partial tracking mode first
    self.update_scope_mode(scope, Mode.PARTIAL)
    for path in untrack_path_list:
      with self._yield_fingerprint(scope, path) as (fp, local_path):
        if fp is None or fp.state == FileState.Untracked:
          raise InvalidOperation("scope %s path %s is already %s" % (scope, path, FileState.Untracked))
        fp.state = FileState.Untracked
    # write back trackinglist status
    self._write_checkout_state()

  @log_calls
  def revert(self, scope, path_list=None, backup_suffix=BACKUP_SUFFIX):
    '''Revert specified files in working directory to original state. If path_list is empty or None, revert all files.'''
    full_revert = not path_list
    rev = self.get_checkout_rev(scope)
    mf = self.repo.get_manifest(scope, rev)
    local_scope_dir = join_path(self.work_dir, scope)

    # Pick all items unless we have a list
    items = mf.get_items(path_list) if path_list else mf.items
    if None in items:
      raise InvalidArgument("item not found in rev %s of scope '%s': %s" % (rev, scope, path_list[items.index(None)]))

    # Get changelist needed to revert, and update files
    (old_fingerprints, new_fingerprints) = self._fingerprint_status(scope, compute_md5=False)
    changelist = changelist_for_fingerprints(new_fingerprints, old_fingerprints, lazy_md5s=True, root_path=local_scope_dir)

    # TODO support reverting to a differetn revision: compute changelist for update,
    # and compose with changelist for working dir revert

    # Revert command will only delete explicitly listed items, unless we are doing a full revert.
    new_fingerprints = self._update_local_files(scope, items, changelist, delete_items=True, delete_all=full_revert, backup_suffix=BACKUP_SUFFIX)

    # Update checkout states to reflect new mtimes, so hashes aren't recomputed later
    self.checkout_state.update_fingerprints(scope, new_fingerprints)
    self._write_checkout_state()

  @log_calls
  def commit(self, scope, user=UNKNOWN_USER, message=EMPTY_MESSAGE, time=None, empty_ok=False, mod_all=False):
    '''Commit current changes to the repository.'''
    repo_tip = self.repo.get_tip(scope)
    prev_rev = self.repo.resolve_rev(scope, self.get_checkout_rev(scope))
    if prev_rev != repo_tip:
      raise InvalidOperation("repository (rev %s) is ahead of working directory (rev %s) for scope '%s': try update first" \
        % (repo_tip, prev_rev, scope))

    local_scope_dir = join_path(self.work_dir, scope)
    (old_fingerprints, new_fingerprints) = self._fingerprint_status(scope, compute_md5=False)
    changelist = changelist_for_fingerprints(old_fingerprints, new_fingerprints, lazy_md5s=True, root_path=local_scope_dir, mod_all=mod_all)
    # Last call filled in MD5s into new_fingerprints if we already knew them; now compute the others.
    new_fingerprints.compute_md5s(local_scope_dir)

    if changelist.is_empty():
      if empty_ok:
        return
      else:
        raise InvalidOperation("nothing to commit at scope '%s'" % scope, suppressable=True)

    # if a new file is tracked and simultaneously removed, raise an error (suppressable).
    for local_path, fp in self.checkout_state.fingerprints.fingerprints.iteritems():
      if fp.state == FileState.ToBeTracked:
        self.checkout_state._check_file_exists(local_path) # sanity check

    log.info("committing changes (%s)", changelist.brief_summary())
    log.stream.write(changelist.to_summary(prefix=scope))

    # Now perform the real repo commit, uploading each new file
    rev = self.repo.commit(scope, local_scope_dir, prev_rev, changelist, user=user, message=message, time=time)

    # And update the checkout state
    self.checkout_state.update_from_changelist(scope, changelist, new_fingerprints)
    self.checkout_state.update_checkout_rev(scope, rev)
    self._write_checkout_state()

  @staticmethod
  def ignore_path(full_path):
    '''Do we want to ignore this file, based on its filename?'''
    # TODO: Make this a config setting
    dirname = os.path.dirname(full_path)
    filename = os.path.basename(full_path)
    def ignore_path_elem(c):
      return c.startswith(TMP_DIR_PREFIX)
    ignore_dir = any(ignore_path_elem(c) for c in dirname.split("/"))
    return ignore_dir \
           or filename.startswith(".") or filename.startswith(".tmp") \
           or filename.endswith("~") or filename.endswith(".bak") or filename.endswith(".tmp") or filename.endswith(BACKUP_SUFFIX)

  @staticmethod
  def _ensure_dot_dir_present(work_dir):
    dot_dir = WorkingDir._path_for_dot_dir(work_dir)
    if not os.path.exists(dot_dir):
      log.debug("creating dot dir: %s", dot_dir)
      os.makedirs(dot_dir)
    else:
      log.debug("using dot dir: %s", dot_dir)

  @staticmethod
  def initialize_work_dir(path, root_uri):
    '''
    Create a new working directory at path, initializing dot dir and config file
    with given root_uri. This may be called on an already initialized working
    directory.
    '''
    pairs = WorkingDir.work_dirs_for_path(path)
    if len(pairs) > 1:
      raise InvalidOperation("cannot nest Zinc repositories: %s" % ", ".join([work_dir for (work_dir, rel_path) in pairs]))
    elif len(pairs) == 1:
      (work_dir, _rel_path) = pairs[0]
      if path != work_dir:
        raise InvalidOperation("cannot nest Zinc repositories: trying to check out %s within %s" % (path, work_dir))
    make_all_dirs(path)
    return WorkingDir._update_work_dir_config(path, {"root_uri": root_uri})

  @staticmethod
  def work_dirs_for_path(path, require=False):
    '''
    Given a path, return all enclosing Zinc working directories. This should be
    zero or one except in erroneous situations. Returns a list of (work_dir,
    rel_path) pairs, where rel_path is the relative path within the working
    directory.
    '''
    out = []
    all_dot_dirs = []
    # Use absolute path, so we walk all the way up to the root.
    abs_path = os.path.abspath(path)
    # We will simplify returned paths to be relative, for readability, but only if the input is relative.
    return_abs_paths = os.path.isabs(path)
    def simplify_path(p):
      return p if return_abs_paths else os.path.relpath(p)
    for (parent, rel_path) in parent_dirs(abs_path):
      dot_dir = WorkingDir._path_for_dot_dir(parent)
      if os.path.isdir(dot_dir):
        log.debug("found working dir '%s' (with relative path '%s') for path '%s'", parent, rel_path, path)
        out.append((simplify_path(parent), rel_path))
        all_dot_dirs.append(dot_dir)
    if require and not out:
      raise InvalidOperation("path is not within a Zinc working directory: %s" % path)
    if len(all_dot_dirs) > 1:
      log.error("found nested Zinc woking directories, which should never happen: %s", ", ".join(all_dot_dirs))
    return out

  @staticmethod
  def _work_dir_config(work_dir):
    '''Utility to parse the config for a working directory. Returns None if not present.'''
    path = WorkingDir._path_for_config(work_dir)
    wconfig = None
    if os.path.exists(path):
      wconfig = ConfigParser.ConfigParser()
      log.debug("reading config from path: %s", path)
      wconfig.read(path)
    else:
      log.debug("failed to read config from path: %s", path)
    return wconfig

  @log_calls
  def enclosing_scope(self, path):
    # XXX A bit slow.
    scopes = set([c.scope for c in self.checkouts()])
    # Walk upward, so we pick the smallest enclosing scope.
    for (parent_path, _rel_path) in parent_dirs(path):
      if parent_path in scopes:
        return parent_path
    return None

  @staticmethod
  def work_dir_context_for_path(path, config):
    '''
    Retrieve a WorkingDir enclosing a given path. Also return the relative
    path and enclosing scope for the given path. Useful for defaults for
    commands.
    '''
    pairs = WorkingDir.work_dirs_for_path(path, require=False)
    if len(pairs) > 0:
      (work_dir, rel_path) = pairs[0]
      work = WorkingDir(work_dir, config)
      scope = work.enclosing_scope(rel_path)
      return (work, rel_path, scope)
    else:
      return (None, None, None)

  @staticmethod
  def _update_work_dir_config(work_dir, values):
    '''
    Write new working directory config file with new main settings. Returns
    True if this was first initialization of working directory.
    '''
    # TODO take a lock on the local config file
    wconfig = WorkingDir._work_dir_config(work_dir)
    first_time = False
    if not wconfig:
      first_time = True
      wconfig = ConfigParser.ConfigParser()
    if not wconfig.has_section("main"):
      wconfig.add_section("main")
    for (k, v) in values.items():
      wconfig.set("main", k, v)
    WorkingDir._ensure_dot_dir_present(work_dir)
    config_path = WorkingDir._path_for_config(work_dir)
    log.debug("writing config: %s", config_path)
    with open(config_path, "w") as handler:
      wconfig.write(handler)
    return first_time

  @staticmethod
  def _path_for_dot_dir(work_dir):
    return join_path(work_dir, ".zinc")

  @staticmethod
  def _path_for_config(work_dir):
    return join_path(work_dir, ".zinc", "config")

  @staticmethod
  def _path_for_checkout_state(work_dir):
    return join_path(work_dir, ".zinc", "checkout-state")

  @staticmethod
  def _path_for_file_cache(work_dir):
    return join_path(work_dir, ".zinc", "cache")


##
## Commands
##

# Commands that work directly on the repository 
_COMMAND_LIST_REPO = ["init", "newscope", "scopes", "log", "tags", "tag", "list", "copy", "locate", "_manifest"]
# Commands that require a working directory
_COMMAND_LIST_WORK = ["checkout", "update", "revert", "id", "ids", "status", "commit", "track", "untrack"]

# Allowed command abbreviations
_COMMAND_ABBREVS = { "cp": "copy", "ls": "list" }


def require_opt(command, value, options, test_func=lambda x: x is not None):
  '''
  Verify options value is set correctly. Assumes options is an optparse.Values object.
  '''
  if not test_func(getattr(options, value)):
    fail("command '%s' requires a value for '%s'" % (command, value))

def require_arg(command, position, description, command_args):
  '''
  Verify argument is provided.
  '''
  if len(command_args) < position:
    fail("command '%s' requires argument %s for '%s'" % (command, position, description))
  return command_args[position-1]

def run_command(command, command_args, options):
  '''Handle command-line invocation.'''

  cwd = os.getcwd()

  if command in _COMMAND_ABBREVS:
    command = _COMMAND_ABBREVS[command]

  config = Config(options=options)

  def populate_options_from_cwd(cwd, options):
    # Get fallback values for root_uri and scope settings from cwd, but only
    # use them if we don't have them already provided.
    (cwd_work, cwd_rel_path, cwd_scope) = WorkingDir.work_dir_context_for_path(cwd, config)
    cwd_work_dir = cwd_work.work_dir if cwd_work else None
    cwd_root_uri = cwd_work.config.root_uri if cwd_work else None
    if options.root_uri and cwd_root_uri and cwd_root_uri != options.root_uri:
      fail("something is wrong: configured root URI '%s' does not match provided root URI '%s'" % (cwd_root_uri, options.root_uri))
    if not options.root_uri and cwd_root_uri:
      options.root_uri = cwd_root_uri
    if not options.scope and cwd_scope:
      options.scope = cwd_scope
    if not options.work_dir and cwd_work_dir:
      options.work_dir = cwd_work_dir

  if command in _COMMAND_LIST_REPO:
    if command == "init":
      options.root_uri = require_arg(command, 1, "root_uri", command_args)
      repo = Repo(options.root_uri, config, check_valid=False)
      repo.init_repo()
    else:
      populate_options_from_cwd(cwd, options)
      require_opt(command, "root_uri", options)
      repo = Repo(options.root_uri, config, check_valid=True)
      if command == "newscope":
        scope = require_arg(command, 1, "scope", command_args)
        require_opt(command, "user", options)
        commit_time = parse_datetime(options.date) if options.date else None
        repo.create_scope(scope, user=options.user, message=options.message, time=commit_time)
      if command == "scopes":
        scopes = repo.get_scopes()
        for scope in scopes:
          print "%s" % scope
      elif command == "log":
        require_opt(command, "scope", options)
        (start, end) = determine_rev_range(options, config)
        time_filter = parse_datespec(options.date, assume_utc=options.assume_utc) if options.date else None
        filter = (lambda mf: time_filter(mf.time)) if time_filter else None
        repo.log(options.scope, sys.stdout, srev_start=start, srev_end=end, filter=filter)
      elif command == "tag":
        # Tag command doesn't require a repository, but if we have one, it's
        # needed to identify checked out tip (for default revisions)
        work = None
        if options.work_dir:
          log.debug("using working dir for tag command revs: %s", options.work_dir)
          work = WorkingDir(options.work_dir, config)
        tag_name = require_arg(command, 1, "tag name", command_args)
        if options.all:
          raise InvalidOperation("tag --all doesn't make much sense and is not supported")
        if options.work:
          if options.rev:
            raise InvalidOperation("cannot tag specific revision with --work option")
          if not work:
            raise InvalidOperation("must have working directory for 'tag --work'")

          checkouts = work.checkouts()
          for checkout in checkouts:
            log.info("tagging scope '%s' with tag '%s' at revision %s", checkout.scope, tag_name, checkout.rev)
            repo.tag(checkout.scope, tag_name, checkout.rev)
        else:
          require_opt(command, "scope", options)
          if not options.rev and work:
            rev = work.get_checkout_rev(options.scope)
          else:
            # Make sure revision is explicit: we don't want to tag against tip, since it's not well defined.
            # Using "-r tip" will still force.
            require_opt(command, "rev", options)
            rev = options.rev
          log.info("tagging scope '%s' with tag '%s' at revision %s", options.scope, tag_name, rev)
          repo.tag(options.scope, tag_name, rev)
      elif command == "tags":
        require_opt(command, "scope", options)
        (start, end) = parse_rev_range(options.rev) if options.rev else ("tip", None)
        time_filter = parse_datespec(options.date, assume_utc=options.assume_utc) if options.date else None
        filter = (lambda mf: time_filter(mf.time)) if time_filter else None
        tags = repo.get_tags(options.scope, srev_start=start, srev_end=end, filter=filter)
        for (name, rev) in tags:
          print "%s\t%s" % (name, rev)
      elif command == "list":
        require_opt(command, "scope", options)
        rev = options.rev if options.rev else "tip"
        path = command_args[0] if len(command_args) > 0 else ""
        items = repo.list(options.scope, path, rev, recursive=options.recursive)
        prefix = None if options.short_paths else options.scope
        for item in items:
          print item.display_path(prefix=prefix)
      elif command == "locate":
        # TODO Rename to _locate and also include compression scheme in output.
        require_opt(command, "scope", options)
        rev = options.rev if options.rev else "tip"
        path = command_args[0] if len(command_args) > 0 else ""
        print "%s\t%s" % repo.locate(options.scope, path, rev)
      elif command == "copy":
        require_opt(command, "scope", options)
        rev = options.rev if options.rev else "tip"
        src_path = require_arg(command, 1, "source path", command_args)
        target_path = require_arg(command, 2, "target path", command_args)
        repo.copy(options.scope, src_path, rev, target_path, clobber=options.force, recursive=options.recursive)
      elif command == "_manifest":
        require_opt(command, "scope", options)
        rev = options.rev if options.rev else "tip"
        mf = repo.get_manifest(options.scope, rev)
        print mf.to_str()

  elif command in _COMMAND_LIST_WORK:

    # check the mode option
    if options.mode is not None:
      options.mode = options.mode.lower()
      if options.mode not in Mode.values():
        raise InvalidArgument("--mode has to be one of %s" % Mode.values())

    # Set up a new working dir, if necessary. Do this now so we can create a WorkingDir object.
    if command == "checkout":
      rev = options.rev if options.rev else "tip"
      options.root_uri = require_arg(command, 1, "root URI", command_args)
      options.work_dir = command_args[1] if len(command_args) > 1 else cwd
      if not options.all:
        # Validate all arguments are right _before_ mutating working directory
        require_opt(command, "scope", options)
        Repo(options.root_uri, config, check_valid=True).resolve_rev(options.scope, rev)
      first_time = WorkingDir.initialize_work_dir(options.work_dir, options.root_uri)
      if first_time:
        log.info("initialized new working directory at: %s", options.work_dir)
      work = WorkingDir(options.work_dir, config)
      if options.all:
        if rev != "tip":
          raise InvalidArgument("can only check out with --all using tip revision")
        scopes = work.repo.get_scopes()
        if len(scopes) < 1:
          raise InvalidOperation("no scopes found", suppressable=True)
        log.info("checking out %d scopes", len(scopes))
        for scope in scopes:
          work.checkout(scope, rev, force=options.force, mode=options.mode)
      else:
        require_opt(command, "scope", options)
        work.checkout(options.scope, rev, force=options.force, mode=options.mode)
    else:
      populate_options_from_cwd(cwd, options)
      work = WorkingDir(options.work_dir, config)

      if command == "id":
        require_opt(command, "scope", options)
        rev = work.get_checkout_rev(options.scope)
        if not rev:
          raise InvalidOperation("scope '%s' is not checked out" % options.scope)
        print "%s" % rev
      elif command == "ids":
        checkouts = work.checkouts()
        for checkout in checkouts:
          print "%s\t%s" % (checkout.scope, checkout.rev)
      elif command == "status":
        if options.all:
          raise InvalidOperation("status --all doesn't make much sense; try status --work instead")
        elif options.work:
          checkouts = work.checkouts()
          for checkout in checkouts:
            prefix = None if options.short_paths else checkout.scope
            sys.stdout.write(work.status(checkout.scope, mod_all=options.mod_all, full=options.full).to_summary(prefix=prefix))
        else:
          require_opt(command, "scope", options)
          log.debug("status for scope '%s'", options.scope)
          prefix = None if options.short_paths else options.scope
          sys.stdout.write(work.status(options.scope, mod_all=options.mod_all, full=options.full).to_summary(prefix=prefix))
      elif command == "commit":
        if options.mode is not None:
          raise InvalidOperation("%s --mode is not allowed; use track/untrack commands first" % command)
        require_opt(command, "user", options)
        require_opt(command, "message", options)
        commit_time = parse_datetime(options.date, assume_utc=options.assume_utc) if options.date else None
        if options.all:
          raise InvalidOperation("commit --all doesn't make much sense; try commit --work instead")
        elif options.work:
          checkouts = work.checkouts()
          for checkout in checkouts:
            log.info("committing scope '%s'", checkout.scope)
            work.commit(checkout.scope, user=options.user, message=options.message, time=commit_time, empty_ok=True, mod_all=options.mod_all)
        else:
          require_opt(command, "scope", options)
          work.commit(options.scope, user=options.user, message=options.message, time=commit_time, mod_all=options.mod_all)
      elif command == "update":
        rev = options.rev if options.rev else "tip"
        if not options.all and not options.work:
          require_opt(command, "scope", options)
          work.update(options.scope, rev, force=options.force)
        else:
          # For --work or --all, first update all known scopes
          if rev != "tip":
            raise InvalidArgument("can only update with --all or --work using tip revision")
          checkouts = work.checkouts()
          scopes_to_checkout = None
          if options.work:
            log.info("updating %d scopes", len(checkouts))
          else:
            # update --all: also do a checkout on other new scopes
            scopes_checked_out = set(checkout.scope for checkout in checkouts)
            scopes_to_checkout = [scope for scope in work.repo.get_scopes() if scope not in scopes_checked_out]
            log.info("updating %d scopes and checking out %d new scopes", len(scopes_checked_out), len(scopes_to_checkout))
          for checkout in checkouts:
            work.update(checkout.scope, rev, force=options.force)
          if scopes_to_checkout:
            for scope in scopes_to_checkout:
              work.checkout(scope, rev, force=options.force, mode=options.mode)
      elif command == "revert":
        if options.mode is not None:
          raise InvalidOperation("%s --mode is not allowed; use track/untrack commands first" % command)
        if options.all:
          raise InvalidOperation("revert --all doesn't make much sense; try commit --work instead")
        elif options.rev:
          raise NotImplementedError("sorry, revert to specified revision not yet implemented")
        elif options.work:
          checkouts = work.checkouts()
          if len(command_args) > 0:
            raise InvalidOperation("revert --work takes no arguments")
          for checkout in checkouts:
            log.info("reverting scope '%s'", checkout.scope)
            work.revert(checkout.scope)
        else:
          # TODO support revert to specified revision
          require_opt(command, "scope", options)
          work.revert(options.scope, path_list=command_args)
      elif command in ["track", "untrack"]:
        if options.all:
          raise InvalidOperation("%s --all doesn't make much sense; try --work instead" % command)
        elif options.rev:
          raise NotImplementedError("sorry, %s to specified revision not yet implemented" % command)
        elif (options.mode is None and not command_args) or (options.mode == Mode.AUTO and (command == "untrack" or (command == "track" and len(command_args) > 0))):
          raise InvalidOperation("%s doesn't make much sense" % " ".join([command, "--mode " + options.mode if options.mode else ""] + command_args))
        elif options.force:
          raise InvalidOperation("do not use force option with %s command" % command)
        elif options.work:
          checkouts = work.checkouts()
        else:
          require_opt(command, "scope", options)
          checkouts = [Checkout(options.scope, work.get_checkout_rev(options.scope), None)]
        if command == "track":
          if len(command_args) > 0:
            # track some files
            for checkout in checkouts:
              work.track(checkout.scope, track_path_list=command_args, mode=options.mode, no_download=options.no_download)
          else:
            # Change mode
            for checkout in checkouts:
              work.track(checkout.scope, mode=options.mode, no_download=options.no_download)
        elif command == "untrack":
          # untrack some files
          for checkout in checkouts:
            work.untrack(checkout.scope, command_args, options.mode)
        else:
          assert False
  else:
    assert False

def main():
  general_setup()

  if not sys.argv[0]: sys.argv[0] = ""  # Workaround for running with optparse from egg

  version_str = "version %s (repository version %s, Boto version %s)" % (ZINC_VERSION, REPO_VERSION, BOTO_VERSION)
  commands_str = "|".join(sorted(list(_COMMAND_LIST_REPO + _COMMAND_LIST_WORK), key=lambda s: s.replace("_", "~")))

  usage = "%%prog %s [options] [arg1] [arg2]\n\n" % commands_str + \
          "Zinc: Simple and scalable versioned data storage\n%s" % version_str

  parser = optparse.OptionParser(usage=usage)
  parser.add_option('-w', '--work-dir', help='working directory (current dir is default)', dest='work_dir', action='store', type='string', default=None)
  parser.add_option('-R', '--repository', help='URI of repository (e.g. s3://mybucket/path)', dest='root_uri', action='store', type='string', default=None)
  parser.add_option('-s', '--scope', help='scope', dest='scope', action='store', type='string')
  parser.add_option('-r', '--rev', help='revision or revision range', dest='rev', action='store', type='string', default=None)
  parser.add_option('-d', '--date', help='date specifier, e.g. <2010-10-10 13:30', dest='date', action='store', type='string', default=None)
  parser.add_option('-u', '--user', help='user name, to appear in commit messages', dest='user', action='store', type='string')
  parser.add_option('-m', '--message', help='commit message', dest='message', action='store', type='string', default=None)
  parser.add_option('-f', '--force', help='force destructive operations', dest='force', action='store_true')
  parser.add_option('-v', '--verbose', help='verbose output', dest='verbose', action='store_true')
  parser.add_option('-a', '--recursive', help='copy recursively (for list, copy)', dest='recursive', action='store_true')
  parser.add_option('--all', help='perform operation on all scopes (for checkout, update)', dest='all', action='store_true')
  parser.add_option('--work', help='perform operation on checked out (working) scopes (for status, update, commit, tag, track, untrack)', dest='work', action='store_true')
  parser.add_option('--mod-all', help='mark every file as modified, skipping content checks (for commit, status)', dest='mod_all', action='store_true')
  parser.add_option('--compression', help='compression scheme to use (e.g. "raw" or "lzo")', dest='compression')
  parser.add_option('--short-paths', help='do not include scope part of path in "list", "status", etc.', dest='short_paths', action='store_true')
  parser.add_option('--suppress', help='suppress harmless failures (e.g. empty commits)', dest='suppress', action='store_true')
  parser.add_option('--no-cache', help='do not cache files locally', dest='no_cache', action='store_true')
  parser.add_option('--no-s3cfg', help='do not look for S3 keys in $HOME/.s3cfg (default: do this as a fallback)', dest='no_s3cfg', action='store_true')
  parser.add_option('--s3-ssl', help='use SSL with S3 (slower)', dest='s3_ssl', action='store_true')
  parser.add_option('--utc', help='when parsing times, assume UTC if not specified', dest='assume_utc', action='store_true')
  parser.add_option('--debug', help='verbose output with debugging details', dest='debug', action='store_true')
  parser.add_option('--version', help='show version information', dest='version', action='store_true')
  parser.add_option('--mode', help='specify tracking mode: %s (for checkout, track, untrack)' % Mode.values(), dest='mode', action='store', type='string', default=None)
  parser.add_option('--no-download', help='track files without copying them to working directory (for track)', dest='no_download', action='store_true')
  parser.add_option('--full', help='show full status when in "%s" tracking mode (for status)' % Mode.PARTIAL, dest='full', action='store_true')

  (options, args) = parser.parse_args()

  if options.version:
    print "%s" % version_str
    sys.exit(0)

  if options.debug:
    log_setup(verbosity=3)
    log.debug("this is %s", version_str)
  elif options.verbose:
    log_setup(verbosity=2)

  if len(args) < 1:
    fail("specify a command, or run with -h for help")

  command = args[0]
  command_args = args[1:]

  if command not in _COMMAND_LIST_REPO and command not in _COMMAND_LIST_WORK and command not in _COMMAND_ABBREVS:
    fail("unrecognized command: run with -h for help")

  try:
    run_command(command, command_args, options)  
  except InvalidArgument, e:
    log.debug("exception in command '%s'", command, exc_info=e)
    fail(str(e), prefix="invalid argument", exc_info=e, suppress=(options.suppress and e.suppressable))
  except InvalidOperation, e:
    log.debug("exception in command '%s'", command, exc_info=e)
    fail(str(e), prefix="invalid operation", exc_info=e, suppress=(options.suppress and e.suppressable))
  except Failure, e:
    log.debug("exception in command '%s'", command, exc_info=e)
    fail(str(e), prefix="error", exc_info=e, suppress=(options.suppress and e.suppressable))
  except AssertionError, e:
    log.debug("assertion error in command '%s'", command, exc_info=e)
    fail("assertion failure: %s\nrun with --debug for details" % e.message, exc_info=e)
  except boto.exception.S3ResponseError, e:
    log.debug("S3 error in command '%s'", command, exc_info=e)
    fail(str(e), prefix="S3 error", exc_info=e)
  except IOError, e:
    log.debug("I/O error in command '%s'", command, exc_info=e)
    if e.errno == errno.EPIPE:
      fail(status=1)
    else:
      fail(str(e), status=1)
  except KeyboardInterrupt, e:
    log.debug("(interrupt)")
    fail(status=2)
  except Exception, e:
    log.debug("exception in command '%s'", command, exc_info=e)
    fail("%s: %s" % (type(e).__name__, e), exc_info=e)

if __name__ == '__main__':
  main()


# TODO:
# major missing features:
#   support nested scopes: manage creation, regenerations of manifests, etc.; also automatic recursive scope updates/commits
#   locking during commit and tag operations (NoLockingService is used by default, need to acquire locks for whole operation).
#   use sqlight3 instead of writing checkout-state in order to make zinc multi-process safe
#   implement s3 retry logic on top of boto retry logic, E.g., boto does not retry for 404 but it could be due to s3 eventual consistency
#   support wildcards *
# other important features:
#   annotations (like tags, but key-value pairs; for example "deploy_date=20120801")
#   extend the Changelist class to include Unmodified and Invalid states so that we can show a list of tracked files for example.
# error message when lzop not installed could be more helpful
# run on all applicable scopes for --work (and --all), and fail or succeed for each
# tolerate (with warning) a "update --work" when scope has been deleted; also a delscope command
# post-upload hook, so that with LZO compression can compute .index files, for use with Hadoop
# option to sanity check configuration (e.g. boto version, lzop command is present)
# make sure we have custom output format with just revisions e.g. for "zinc log"
# make log output cleaner/briefer (don't log about copying from cache, etc.)
# "commit -u" that does update and commit if there are no conflicts
# revert to other revisions
# make arguments substitutable by options; handle multiple scopes as arguments in commands
# ignore trailing / on scope names (if provided with -s)
# how to handle update after a client deletes a file and it's also deleted in repo (currently this is a conflict and aborts)
# in an update, should remove a directory when all files in the directory are deleted (but what if .orig files are present?)
# switch to using argparse instead of optparse and make options command-specific
# accept file arguments to commit and perhaps other commands
# fix copy command to take multiple args, use only last for target
# handle argument path names that are relative to cwd
# zinc diff command
# file cache: add to cache on upload as well as download
# don't error with "checkout -s scope" followed by "checkout --all"
# support deleting tags; catch invalid or duplicate tags to tags command
# support "fat" tags that have multiple keys and values (e.g. deploy dates)
# list scope and tags in log command output
# log of individual/sets of files
# allow update|checkout --work|--all to work with a shared tag name (--force to override if only some scopes have tag)
# fast checkout feature for with dummy checkout files (say over a given size) to ease checkout process
# additional integrity checks: we already do mtime/size/md5 checks, but there are some corner cases to cover:
#   with S3, post upload, get MD5 and validate it matches the working copy MD5
#   with S3, compute MD5s on download and validate they match S3 MD5
# implement S3-based locking
# add locking for working dir to prevent two commands from running at once
# management of working state (zinc add, zinc remove, etc.) (must also rethink logic of revert command)
# consider different signal handlers and finally clauses to minimize corrupt working dir state on interruption (especially checkout-state)
# --long variation on id and ids commands, to list last commit msg etc.
# heuristic to not compress files of certain size or that are already compressed
# store and restore file modification times reliably
# add a transaction log, with timestamps, for all repo transactions (such as adding/removing tags)
# mail-out hook -- implement through transaction log
# when deleting, keep backups as .orig (and add to ignore list), or leave in cache
# add config settings in ~/.zincrc
# finish basic branch support: add -b option, newbranch command, and "branch" argument across all function calls that take a scope
# multi-scope support: for commits, automatically identify all enclosing scopes for a commit, and commit each one
# list and copy for paths that span more than one scope
# file sizes (and optionally hashes, dates, etc.) in manifest; will make detailed listings fast
# nicer error messages for invalid revisions
# support symlinks
