This document details two versions of qsplit:

1. Original **[qsplit](#qsplit-create-manifest-files-for-parallel-copy-using-rsync-or-robocopy)** which 
creates rsync and robocopy manifests
2. Newer **[qsplit rsync only](#qsplit-rsync-only-create-manifest-files-for-parallel-copy-using-rsync)** which is 
better and handling situations where you maybe be doing multiple iterations of rsync.

# qsplit: Create manifest files for parallel copy using rsync or robocopy

The qsplit utility is used to move data from a qumulo cluster by using Qumulo 
file and directory aggregates from the REST API. Qsplit uses the 
read_dir_aggregates API to build a list of paths (in ~log(n) time) that can 
be piped to rsync in order to optimize a migration *from* a Qumulo cluster to 
another target path.

Using theis approach you should see a significant performance improvement 
over running rsync in the traditional way `rsync -av -r [src] [dest]`. The 
performance should be better for two reasons:

1. No file crawl needed by rsync because we're passing a filespsec in --files-from
2. Running multiple instances of rsync in parallel
3. Different client machines avoid burying the NIC and keep things busy and active.


Example usage: `python3 qsplit.py --ip 192.168.1.88 -u admin -b 4 /media`


-----


## A detailed qsplit example

First, a little about the "algorithm":

1. Divide a qumulo cluster into N equal partitions. A partition is a list of 
paths. The partitioning is based on the capacity (block count), which is obtained 
from fs_read_dir_aggregates. (You can also specify partitioning using the argument
`-a files`).
2. Feed each partition to an rsync client

As an example, I run the command like this:

`python3 qsplit.py --ip 192.168.1.88 -b 4 /music`

This will create four 'bucket files' for host '192.168.1.88' and path '/music': 
a bucket is a list of filepaths using naming convention `split_bucket_[n].txt` 
where 'n' is # from 1..[# of buckets specified, above it is four]. If you do 
not specify a '-b' param it will create a single bucket with all of the 
filepaths for the specified source and path.

Once the files are created you can copy them to different machines/NICs to 
perform rsyncs (or robocopies) in parallel. You could also run the rsyncs on a 
single machine with separate processes but you'd likely bury the machine NIC 
with traffic that way. So one way to use these manifests is:

1. Copy the results of qsplit/text files to somewhere client machines can resolve them
2. ssh to [n] different client machines with separate NICs
3. Mount the cluster [src] and [dest] on each machine
4. On each machine run rsync in the following fashion:

`rsync -av -r --files-from=split_bucket_[n].txt [src qumulo cluster mount] [target cluster mount]`

**NOTE** that the file paths in the bucket text files are all relative to the 
path specified when running qsplit so if you created filepaths for '/music' 
then that should be your [src cluster mount] point so that the relative 
filepaths can resolve.


### Windows/robocopy option 
qsplit.py now also offers a `--robocopy` (or `-r`) option for Windows 
environments which writes out file specs using backslashes rather 
than forward slashes:

`python3 qsplit.py -r --ip 192.168.1.88 -u admin -b 4 /media`


-----

# qsplit rsync only: Create manifest files for parallel copy using rsync

Example usage: `python3 qsplit-rsync-only.py --host 192.168.1.88 -b 4 /music`

This will create four files that can be used with a command like the following:

`rsync --filter '. rsync-filter-001.txt' -a Q/ T/`


-----

## Prerequisites

* Python 2.7


```
  pip install -r requirements.txt
```

You can verify that you have the Qumulo REST API installed by running
the following command at a command prompt:
```
  pip list
```
You should see something like the following output:

```
...
qumulo-api (2.6.10)
...
```
