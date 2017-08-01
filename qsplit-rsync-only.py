#!/usr/bin/env python
# Copyright (c) 2017 Qumulo, Inc. All rights reserved.
#
# NOTICE: All information and intellectual property contained herein is the
# confidential property of Qumulo, Inc. Reproduction or dissemination of the
# information or intellectual property contained herein is strictly forbidden,
# unless separate prior written permission has been obtained from Qumulo, Inc.

'''
== Description:
This python sample will use the read_dir_aggregates API to build a set of rsync
filters to optimize migration to or from a qumulo cluster.

Approach:
- Divide a qumulo cluster into N approximately equal partitions
- Qumulo aggregates are used for partitioning (capacity or file count)
- A partition is a set of rsync filter rules.
- The union of all partitions should cover all data under the target or
  source path, even for items created after the partitioning

Example: assume you have / from a Qumulo cluster mounted at Q/ and the target
directory is T/

  % qsplit-rsync.py --host qumulo-host --buckets=2 /

  This will produce two partitions base on space usage (change bucket count or
  add '--aggregate files' as desired):

    rsync-filter-001.txt
    rsync-filter-002.txt

Feed each partition filter file to an rsync client.

  client-1% rsync --filter '. rsync-filter-001.txt' -a Q/ T/

To generate the approximate namespace IO load on the source, you can add the
rsync --dry-run argument. XXX not sure what load this does on the target.

XXX escaping of wildcards and stuff.

'''

import argparse
import time

import qumulo.lib.auth as libauth
from qumulo.lib.request import Connection, RequestError
import qumulo.rest.auth as auth
import qumulo.rest.fs as fs

QUERY_ORDER_BY = {
    'capacity': 'total_blocks',
    'files':    'total_files'
    }

DIR_AGGREGATE_KEY = {
    'capacity': 'total_capacity',
    'files':    'total_files'
}

ENTRY_AGGREGATE_KEY = {
    'capacity': 'capacity_usage',
    'files':    'num_files'
}

class Dirent(object):
    def __init__(self, name, is_dir, size):
        self.name = name
        self.is_dir = is_dir
        self.size = size

    def __repr__(self):
        return "Dirent(%s, %s, %s)" % (self.name, self.is_dir, self.size)

class Directory(object):
    def __init__(self, result, aggregate):
        self.entries = []
        self.total = 0
        for entry in reversed(result.data['files']):
            size = int(entry[ENTRY_AGGREGATE_KEY[aggregate]])
            is_dir = entry['type'] == 'FS_FILE_TYPE_DIRECTORY'
            name = "%s%s" % (entry['name'], '/' if is_dir else '')
            self.entries.append(Dirent(name, is_dir, size))
            self.total += size

        # The aggregates query iterates over all directory entries and
        # sums the metrics. Not all entries may be returned, so keep
        # track of the delta for bucket accounting.
        total_size = int(result.data[DIR_AGGREGATE_KEY[aggregate]])
        self.extra = total_size - self.total

    def pop(self):
        dirent = self.entries.pop()
        self.total -= dirent.size
        return dirent

    def empty(self):
        return len(self.entries) == 0

class Filter(object):
    def __init__(self, size):
        self.size = size
        self.free = self.size
        self.entries = []
        self.last_path = []
        self.included = set()
        self.excluded = set()

    def __repr__(self):
        return "Filter(%s)" % (self.entries)

    def used(self):
        return self.size - self.free

    def add_rule(self, rule):
        self.entries.append(rule)

    def add_include(self, fullpath, suffix=''):
        self.add_rule('+ ' + fullpath + suffix)
        self.included.add(fullpath)

    def add_exclude(self, fullpath):
        if fullpath not in self.excluded and fullpath not in self.included:
            self.add_rule('- ' + fullpath)
            self.excluded.add(fullpath)

    def add_create_dir(self, fullpath):
        self.add_include(fullpath)

    def add_needed_dirs(self, path, handled):
        for i, p in enumerate(path):
            if len(self.last_path) <= i or p != self.last_path[i]:
                self.add_create_dir(''.join(path[0:i+1]))
            for e in handled[i]:
                self.add_exclude(e)

        self.last_path = list(path)

    def include_item(self, path, handled, dirent):
        self.add_needed_dirs(path, handled)

        #suffix = '***' if dirent.is_dir else ''
        suffix = ''
        self.add_include(''.join(path) + dirent.name, suffix)
        self.free -= dirent.size

    def include_remaining(self, path, handled, size):
        self.add_needed_dirs(path, handled)
        self.add_include(''.join(path), '*')
        self.free -= size

    def finish(self, path):
        for i in range(len(path), 0, -1):
            self.add_exclude(''.join(path[0:i]) + '*')

    def save(self, filename):
        with open(filename, 'w') as bucket_file:
            for entry in self.entries:
                bucket_file.write(entry.encode('utf8') + '\n')

class RestConnection(object):
    def __init__(self, host, port, user, password, creds_store):
        self.connection = Connection(host, port)

        if not creds_store:
            creds_store = libauth.credential_store_filename()
        self.creds = libauth.get_credentials(creds_store)

        try:
            auth.who_am_i(self.connection, self.creds)
        except RequestError:
            results, _ = auth.login(self.connection, None, user, password)
            self.creds = libauth.Credentials.from_login_response(results)

    def get_aggregates(self, path, aggregate):
        start = time.time()
        res = fs.read_dir_aggregates(self.connection, self.creds, path=path,
                                     order_by=QUERY_ORDER_BY[aggregate],
                                     max_entries=5000)
        print "Read directory aggregates in %7.3f seconds at path %s" % (
                time.time() - start, path)
        return res


class Partitioner(object):
    def __init__(self, rest, buckets, aggregate, no_wildcards):
        self.rest = rest
        self.num_buckets = buckets
        self.aggregate = aggregate
        self.no_wildcards = no_wildcards

        self.handled = None
        self.path = None
        self.buckets = None
        self.max_bucket_size = None

    def create_bucket(self):
        assert len(self.buckets) < self.num_buckets
        bucket = Filter(self.max_bucket_size)
        self.buckets.append(bucket)
        return bucket

    def on_last_bucket(self):
        return len(self.buckets) == self.num_buckets

    def current_bucket(self):
        return self.buckets[-1]

    def start(self, start_path):
        print "Gathering data at %s for %d buckets" % (
            start_path, self.num_buckets)

        res = self.rest.get_aggregates(start_path, self.aggregate)
        total_size = int(res.data[DIR_AGGREGATE_KEY[self.aggregate]])
        self.max_bucket_size = total_size / self.num_buckets

        self.handled = []
        self.path = []

        self.buckets = []
        self.create_bucket()
        self.process_folder('/', start_path, res, '/')

    def process_folder(self, name, qpath, res, rpath):

        self.handled.append([])
        self.path.append(name)

        folder = Directory(res, self.aggregate)

        while True:
            bucket = self.current_bucket()

            # Remaining items, perhaps all, fit in bucket or last bucket
            if not self.no_wildcards and (folder.total <= bucket.free or \
                    self.on_last_bucket() or \
                    folder.empty()):
                total = folder.total + folder.extra
                bucket.include_remaining(self.path, self.handled, total)
                break

            dirent = folder.pop()

            if dirent.size <= bucket.free:
                bucket.include_item(self.path, self.handled, dirent)
                self.handled[-1].append(rpath + dirent.name)
            elif dirent.is_dir:
                new_qpath = qpath + dirent.name
                new_rpath = rpath + dirent.name
                new_res = self.rest.get_aggregates(new_qpath, self.aggregate)
                self.process_folder(dirent.name, new_qpath, new_res, new_rpath)
                self.handled[-1].append(rpath + dirent.name)
            else:
                bucket.finish(self.path)
                bucket = self.create_bucket()
                if not folder.empty():
                    bucket.include_item(self.path, self.handled, dirent)
                    self.handled[-1].append(rpath + dirent.name)
                else:
                    # Otherwise we wrap around to include the remaining above.
                    # Add back in the size we popped off.
                    assert folder.total == 0
                    folder.total += dirent.size

        self.handled.pop()
        self.path.pop()

    def output_filters(self, filter_basename):
        for i, bucket in enumerate(self.buckets):
            filename = "%s-%03d.txt" % (filter_basename, i + 1)
            bucket.save(filename)

            print "Output Filter %s size %12d / %d" % (
                    filename, bucket.used(), bucket.size)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True,
                        help="Required: Specify cluster hostname")
    parser.add_argument("-P", "--port", type=int, default=8000,
                        help="Specify port on cluster; defaults to 8000")
    parser.add_argument("--credentials-store",
                        help="Read qumulo_api credentials from a custom path")
    parser.add_argument("-u", "--username", default="admin",
                        help="User name for login; defaults to admin")
    parser.add_argument("--password", default="admin",
                        help="User password for login, defaults to admin")
    parser.add_argument("-b", "--buckets", type=int, default=1,
                        help="Number of partition buckets; defaults to 1")
    parser.add_argument("-a", "--aggregate", default="capacity",
                        choices=["capacity", "files"],
                        help="Aggregate used for partitioning")
    parser.add_argument('-o', '--filter-basename', default='rsync-filter',
                        help='Basename for output filter files')
    parser.add_argument('--no-wildcards', action='store_true',
                        help='Do not use wildcards on filters')

    parser.add_argument("start_path", action="store",
                        help="Path on the cluster for file info")

    args = parser.parse_args()

    connection = RestConnection(args.host, args.port,
                                args.username, args.password,
                                args.credentials_store)

    partitioner = Partitioner(connection, args.buckets, args.aggregate,
                              args.no_wildcards)
    partitioner.start(args.start_path)
    partitioner.output_filters(args.filter_basename)

# Main
if __name__ == '__main__':
    main()
