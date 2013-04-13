# Zinc

### Simple and scalable versioned data storage

Joshua Levy  
2012-11-06 (covers Zinc version 0.3.16)

Kazuyuki Tanimura
Update on 2013-04-11 (covers Zinc version 0.3.18)


## Motivation

Zinc is a simple, versioned store for files. It operates much like a revision
control system for source code, but with an emphasis on scalability and
simplicity in managing large or numerous data or configuration files.

The key goals are:

- **Revision control for data**: The goal is to manage revisions of data
  files, including support for atomic commits and metadata such as commit
  history, commit messages, and tags.

- **Cloud storage**: Direct support for [S3](http://aws.amazon.com/s3/) (and
  potentially any other file storage layer).

- **Efficiency and scalability**: The ability to manage numerous (millions) and large
  (multi-gigabyte) files. Support sharding of data to avoid excessive copying.

- **Simplicity**: A simple implementation and an easy-to-understand internal
  storage structure for files, so you have a good understanding of how your
  data is kept safe.

So why would you use Zinc over other existing systems? What are the alternatives?

- Revision control systems (such as Subversion, Mercurial, or Git) give a great
  set of features for versioning but tend to fail with even moderately large
  files. They also become unmanageable when there are many files. Distributed
  systems copy the entire repository to every clone, which is prohibitively
  expensive if you don't have enough bandwidth.  

- Storage systems (such as
  [S3 with versioning support enabled](http://docs.amazonwebservices.com/AmazonS3/latest/dev/Versioning.html),
  or enterprise solutions like [JCR](http://en.wikipedia.org/wiki/Content_repository_API_for_Java))
  do support versioning and larger files, but they do not support the same
  versioning features, especially atomic commits of many files in a transaction.

- The closest alternatives are extensions to existing revision control systems
  to store file contents externally, such as
  [git-annex](http://git-annex.branchable.com/) or
  [Bigfiles](http://mercurial.selenic.com/wiki/BigfilesExtension).
  Also [Boar](http://code.google.com/p/boar/) has a similar goal. These vary in their
  features and approach, and may be a better fit for some applications;
  however, we're not aware of one that has the same emphasis of support for S3,
  terabyte scale, and some of the additional features below.


## Features

Zinc supports both scale and transactionality, making it well suited for
managing potentially large sets of data and configuration files. It also does
this fairly simply via direct support for S3. Some other nice features:

- For read-only use, you can copy or access any file or set of files directly,
  without "checking out" a whole repository. This is key for large
  repositories, to minimize the amount of data downloaded or copied.

- You can access the underlying content directly. For example, you may look up
  the location of a particular file, then point another piece of software,
  such as Hadoop, to the underlying compressed blobs in S3. (LZO compression
  is used for compatibility with Hadoop.)

- Data is broken into "scopes" to better scale to big data problems. With any
  large data set, it helps to split your data into shards that can be stored or
  updated independently. Zinc scopes offer a way to do this. Potentially, you
  can have thousands of scopes, each with thousands of files, operating
  independently.

- Transactionality is flexible -- and not global. Scopes also delineate
  transactionality. This means you can easily support commits to different
  parts of your repository in parallel (without the numerous trivial merge
  operations common to systems like Git or Mercurial). 

- For write use, you need only check out the scope in the repository
  you are interested in and modify and commit those files. This reduces data
  copying.

- The internal format is transparent, consisting of simple text files for
  metadata, and unmodified or LZO compressed blobs for all files. It's also 
  designed in a way that it is robust to interrupted transactions; an
  interruption or other failure cannot leave the shared repository in a state
  that will ever confuse another client.

- The full implementation is fairly simple -- just one Python file. Scalability
  is left to S3 (or any other backing storage layer). Dependencies are
  minimal: Python, [Boto](http://code.google.com/p/boto/), and 
  [lzop](http://www.lzop.org/).


However, before we get too excited here, there are a number of key limitations:

- Zinc is a small and basic piece of software, without a lot of the comforts of a
  mature and complex system like Git or Mercurial. Nor does it support any of the
  more advanced features of a revision control system, like branching or merging.
  (The reasoning for this is that with large data files, and especially binary
  ones, you rarely want automated merge support at this layer.)

- Zinc does not support delta compression. Files are compressed individually,
  not delta compressed, for simplicity and to allow direct access to underlying
  files from other programs.

- There are a number of other caveats, notably to do with the need for a locking
  service (see below).


## Basics

### Repository

A central repository (on disk or in S3) holds all versions of all files.
Versions of some or all of the files can be checked out or directly copied.
Items stored are just files and may be of any size (limited only by the backing
store, such as S3). They are stored in a global folder hierarchy, just like the
filesystem. With a large or distributed filesystem like S3, the set of files
could be very large.

### Scopes

Scopes are a way to keep the number of files users deal with routinely to a
manageable number and total size, considering that the whole repository may be
large. Folders may be specially marked as scopes. Every item has a unique
scope, which is the smallest enclosing folder marked as a scope. As with other
versioning systems, items are changed via atomic commits that add, remove, or
modify files. What is different from other versioning systems is that the files
and the transactionality guarantees of a commit are limited to one scope. The
reason for this is that with large data sets, it's important to reduce
centralized bookkeeping: working in separate scopes reduces synchronization and
merging challenges when many files are in the repository.  Effectively, Zinc
encourages you to partition your data into small pieces that require more
limited transactionality, which means fewer merges and less global contention.

### Working with data

Data for any file or folder can be read or copied from the repository directly.
To modify data, you must check out a working directory, then modify files, and
make a commit.


## Setup

To use, make sure zinc.py is in your path as `zinc` (copy or symlink it
-- it's just one file).

If you plan to use Zinc with S3, you should put your S3 credentials in your
environment variables `S3_ACCESS_KEY` and `S3_SECRET_KEY`. Alternately, as a
convenience for folks who already use s3cmd, Zinc will by default "borrow" your
access keys from s3cmd's configuration file in `~/.s3cfg`, if it is present and
readable.


## Common Commands
        
Some commands work directly with the repository, with no local state on the
filesystem. Others require a working directory.

Below is a quick summary of sample commands and what they do. For illustration,
we'll assume you have a repository in S3 at `s3://my-bucket/zinc/my-repo`
and that this holds lots of data. In these examples we'll assume data is
sharded by customer, each within paths like `customer/acme`.

These are setup commands you'd only run occasionally:

    # Create a new repository -- do this only once ever:
    zinc init s3://my-bucket/zinc/my-repo

    # Create a new scope (log message username is supplied) -- do this for each new scope (customer in this case):
    zinc newscope -R s3://my-bucket/zinc/my-repo -u levy customer/acme

Now some routine commands:

    # Get help:
    zinc -h
    # Make our own working copy:
    zinc checkout -s customer/acme s3://my-bucket/zinc/my-repo
    # (Or, can use --all option to check out all scopes at once, though this may be way too much data, for large repositories.)
    cd customer/acme
    # (Now add some files to this directory.)
    # View what files have changed:
    zinc status
    # Now commit them; all files will be added:
    zinc commit -u levy -s customer/acme -m "Changed some stuff."
    # View history:
    zinc log -s customer/acme
    # Add a tag for this scope, operating directly on repository:
    zinc tag -s customer/acme deploy-20110806
    # View tags:
    zinc tags -s customer/acme
    # For this scope, or each scope we have checked out, remember what revision we have:
    zinc id
    zinc ids
    # Update our working copy, in case someone else has made changes:
    zinc update -s customer/acme
    # Or, update all scopes
    zinc update --all
    # Update to a particular revision or tag
    zinc update -r l9q43b51f2kht -s customer/acme
    zinc update -r my-tag -s  customer/acme


(Once you're done, if you're curious, you can see the actual internal structure
of the repository within `s3://my-bucket/zinc/my-repo` at any time. It's very
simple and holds all version of all the files along with various pieces of
metadata.)

You can also work directly on the repository, without a working directory. This
is good for scripts or situations where you only want to examine a few files.

    # List recursively (-a) all files within a scope, for current tip:
    zinc list -a -R s3://my-bucket/zinc/my-repo -s customer/acme
    # Or for a previous revision:
    zinc list -a -R s3://my-bucket/zinc/my-repo -s customer/acme -r l9q43b51f2kht
    # Tag names work for this too:
    zinc list -a -R s3://my-bucket/zinc/my-repo -s customer/acme -r deploy-20110806
    # Copy recursively (-a) all files within a scope:
    zinc copy -a -R s3://my-bucket/zinc/my-repo -s customer/acme related my-acme/related
    # Other commands work directly on the repository too:
    zinc tags -R s3://my-bucket/zinc/my-repo -s customer/acme my-acme
    zinc log -R s3://my-bucket/zinc/my-repo -s customer/acme my-acme

## Partial Checkout (New in version 0.3.18)
###Objectives:
Checking out an entire scope sometimes requires GB order of file transfer in total. The new version of zinc allows partial checkouts

Here, we will have an idea of partial tracking mode v.s. auto tracking mode (i.e. current behavior, all local changes will be picked up). In partial tracking mode, only tracked files will show up in status or be allowed to be committed. Zinc checkout command with "--mode partial" option puts the checked-out scope in partial tracking mode. The default is "--mode auto" We can also switch between these two modes using track/untrack commands. Moreover, we can track/untrack only specific files as well.

###Example Usage:
    # checkout in tracking-mode
    zinc checkout -s <scope> s3://my-bucket/zinc/my-repo --mode partial # => nothing will be downloaded

    # we can track files after checking out using track command
    zinc track -s <scope> dir1/file1 # => This path needs to be relative path(s) from scope dir

    # even you add a random file, zinc status does not pick it up
    touch scope/test.txt
    zinc status -s <scope> # => will not show test.txt
    zinc status -s <scope> --full # => the new --full option will show test.txt as an untracked file

    # we can untrack files so that zinc update will ignore the files
    zinc untrack -s <scope> dir1/file2

    # "zinc commit" commits only tracked files and changed files
    zinc commit -s <scope> -u <user> -m <message>

    # when we track a file that already exists in local file, it preserve the file and your local change is safe.
    zinc track -s <scope> dir1/file2 # => zinc assumes that your local change is ahead of repository
    zinc status # => this should show that dir1/file2 is modified if it was modified

    # we can delete it or rename it
    rm scope/dir1/file2
    zinc untrack -s <scope> dir1/file2 # => we can also untrack first before deleting it

    # --no-download helps deleting files faster
    zinc track -s <scope> dir1/file2 --no-download # => it does track file2; however, local copy won't be created (fast!)
    zinc status -s <scope> # => status will show that file2 is removed
    zinc commit -s <scope> # => commit it so that file2 will be removed from remote repository

    # put all local checked out scopes with --work option
    zinc track --work <maybe with filenames>

    # Let's go back to auto tracking mode
    zinc track -s <scope> --mode auto

    # Now you see test.txt that we added earlier in the status
    zinc status # => it picks up test.txt as a newly added file

###What is the catch??
* After updating Zinc, Zinc migrates its internal logging file "checkout-state" to a new format. After this migration, older version of Zinc cannot pick up the checked out states/files. Mixed usage of old version and new version of Zinc is prohibited!!
* If there is a local file with the same name as the file you are trying to track, Zinc assumes that the local copy in working directory is the newest. It does not do automatic rebase. The author is responsible to make sure that the change is on top of the current data.
* "Create a new file => track it => remove it" puts the file in an invalid state. Currently we can catch it only when we commit it (zinc status gives you a warning). The remote repo is safe and we leave this behavior for now.


## Limitations and Notes

Unfortunately, the program is really simple right now and has a lot of
limitations, including:

- There are no commands to explicitly add or remove files from within the
  working directory (like `git add` or `git rm`). All files are added or
  removed exactly as they appear, so use caution and check `zinc status` before
  committing.

- Merging of changes to the same file is not supported. If someone commits to
  the same scope as you and to the same file, between your last update and your
  current commit, you have to save your work somewhere, revert, merge manually
  yourself, and commit again.

- There is a file cache at the top of any working directory, in `.zinc/cache`.
  This can grow large over time, and can be deleted with `rm -rf .zinc/cache` at
  any time. Or use `--no-cache` to avoid caching altogether.

- Simultaneous commits by different clients to different scopes is always
  handled correctly. And commits themselves are always transactional, so either
  succeed or fail in entirety. However, resolving conflicts between simultaneous
  commits to the same scope requires some sort of global locking service (see
  code). By default this is not enabled. What this means is that if two users
  commit to the same scope at once, and no locking service is used, one user's
  commit may be lost without an error.

- There is no support for deleting tags or removing or rolling back commits
  in the repository. (But you can always just make a new commit.)

- The working directory is not designed to be used by many clients at once (it
  does not use file locks). For multiple clients, use multiple working directories.

- It is worth being aware that S3's consistency model occasionally causes issues
  in rare situations where an one object may not appear for minutes or hours from
  some locations. This is most common in
  [S3's Standard Region](http://aws.amazon.com/s3/faqs/#What_data_consistency_model_does_Amazon_S3_employ).
  These may cause an exception and abort during an update, but should never result in
  corrupt data being checked out.

For more details, see the long list of TODOs the end of zinc.py.


## Further Information

- See `tests.sh` for example commands.

- Read pydocs on the Repo class in `zinc.py` for documentation on the internal
  repository format.
