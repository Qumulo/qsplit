# qsplit -- creates manifest files for parallel rsync and other scenarios

The qsplit utility supports two use cases:
Optimized Migration using dir aggregates/REST API
Replication using max_ctime via REST API

**WARNING**: At present, we cannot provide a guarantee to users that replication based on max_ctime will pick up all changed files that are due for replication; this is due to how we currently propogate metadata about filesystem changes.  This will be addressed in a subsequent release.  NOTE that you can use the since parameter but proceed with caution as metadata for change time may not have
been propagated to a given directory at the time of the API call. 

Example usage:

./qsplit.py --host music --buckets 4 --since "2016-01-20 00:00:00" /media/

  
This python sample will use the read_dir_aggregates API to build a list of paths (in ~ log(n) time) that can be piped to rsync in order to optimize a migration *from* a qumulo cluster to another disk target.  It could also easily be adapted to build a file list for RoboCopy in Windows environments.

Approach:

- divide a qumulo cluster into N equal partitions. A partition is a list of paths. The partitioning is based on the block count, which is obtained from fs_read_dir_aggregates

- feed each partition to an rsync client

As an example, I run the command like this:

./qsplit.py --host music /music/ --buckets 4

This will create four 'bucket files' for host 'music' and path '/music/': a bucket is a list of filepaths using naming convention

qsync_[YYYYMMDDHHMM]_bucket[n].txt

where 'n' is # from 1..[# of buckets specified, above it is four]

If you do not specify a '--buckets' param it will create a single bucket with all of the filepaths for the specified source and path.

Once the files are created you can copy them to different machines/NICs to perform rsyncs in parallel.  You could also run the rsyncs on a single machine with separate processes but you'd likely bury the machine NIC with traffic that way.  So one way to use these manifests is:

1. Copy the results of qsplit/ text files to somewhere client machines can resolve them
2. ssh to [n] different client machines with separate NICs
3. Mount the cluster [src] and [dest] on each machine
4. On each machine run rsync in the following fashion:

rsync -av -r --files-from=qsync_[YYYYMMDDHHMM]_bucket[n].txt [src qumulo cluster mount] [target cluster mount]

**NOTE** that the file paths in the bucket text files are all relative to the path specified when running qsplit so if you created filepaths for '/music/' then that should be your [src cluster mount] point so that the relative filepaths can resolve.

Using the above approach you should see a significant performance improvement over running rsync in the traditional way:

rsync -av -r [src] [dest] 

The performance should be better for two reasons:

1. No file crawl needed by rsync because we're passing a filespsec in --files-from
2. running multiple instances of rsync in parallel

In addition by running each instance on a different client machine we avoid burying the NIC for a single machine and keep things nice and busy/active.

## Prerequisites

* Python 2.7

if you're developing on a current version of Mac OSX, you should already have a 2.7 version of python.  you can check which version of python you have by opening a command promopt and typing

  python -V

To install Python 2.7 please visit the [Python Software Foundation
Download Page](https://www.python.org/downloads/)  and select the most
current version (at time of writing it is version 2.7.10)


### 2. Install the Qumulo REST API Python Wrapper library

Navigate to the folder where you installed qsplit locally, and run

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
astroid (1.3.8)
logilab-common (1.1.0)
nose (1.3.7)
pip (7.1.2)
pylint (1.4.4)
qumulo-api (1.2.14)
setuptools (17.0)
six (1.10.0)
wheel (0.24.0)

```
