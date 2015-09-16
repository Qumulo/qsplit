#!/usr/bin/env python
# Copyright (c) 2013 Qumulo, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

'''
== Description:
This python sample will use the read_dir_aggregates API to build a list of paths
that can be piped to tools such as rsync in order to optimize a migration
*from* a qumulo cluster to another disk target.
Approach:
- divide a qumulo cluster into N equal partitions based on . A partition is a list of paths.
The partitioning is based on the block count, which is obtained from
fs_read_dir_aggregates
- feed each partition to an rsync client
== Typical Script Usage:
qfiles.py --host ip_address|hostname [options] SRC DEST
=== Required:
[-i | --ip | --host] ip|hostname    An ip address or hostname of a node in
                                        the cluster; use 'localhost' when
                                        running directly on the node.
                                        (Defaults to 'localhost')
SRC                                 source path for copy
=== Options:
DEST                                dest path for copy.  If not specified, lists source files
[-u | --user] username              Use 'username' for authentication
                                        (defaults to 'admin')
[-p | --passwd] password            Use 'password' for authentication
                                        (defaults to 'admin')
[-P | --port] number                Use 'number' for the API server port
                                        (defaults to 8000)
[-b | --buckets] buckets                        (Integer) number of buckets to create to parallelize the list of files
                                        (defaults to 1)
[-d]                                debug mode; shopwdw file sizes along with files in the lists
-h | --help                         Print out the script usage/help
=== Examples:
- Run the script against the localhost, single bucket
qfiles.py --host music
- Run against host music, 12 buckets
qfiles.py --host music -t 12
'''

# Import python libraries
import argparse
import datetime
import sys
import os

import arrow

# Import Qumulo REST libraries
# Leaving in the 'magic file path' for customers who want to run these scripts
# from cust_demo as before
import qumulo.lib.auth
import qumulo.lib.request
import qumulo.rest.fs as fs

class Bucket:

    def __init__(self, size, start_time):
        self.size = size
        self.free_space = self.size
        self.entries = []
        self.start_time = start_time

    def add(self, entry, current_path, size):
        ''' add an entry to the current bucket.  If there isn't space for the entry
            in the bucket such that we'll exceed max_bucket_size, create a new bucket
            and make it the current one. '''

        path = current_path + entry['name']
        bucket_entry = { "path" : path, "size" : size }
        self.entries.append(bucket_entry)
        self.free_space -= size

    def remaining_capacity(self):
        return self.free_space

    def print_contents(self):
        print self.free_space, self.size
        for entry in self.entries:
            print entry

        self.print_bucket_size()

    def bucket_count(self):
        return len(self.entries)

    def print_bucket_size(self):
        total_size = 0
        for entry in self.entries:
            total_size += int(entry['size'])

        print "Total data stored in bucket: " + str(total_size)
        if total_size > self.size:
            print "More data stored in bucket that initial size"
            print "Overflow: " + str(total_size-self.size)


    def save(self, bucket_number, offset):

        # create a file for bucket path entries
        filename = "qsync_" + self.start_time.strftime("%Y%m%d%H%M_bucket") + str(bucket_number) + ".txt"
        bucket_file = open(filename, 'w+')

        for entry in self.entries:
            relative_path = entry['path'][offset:]
            # relative_path = entry['path']
            bucket_file.write(relative_path.encode('utf-8') + '\n')

        bucket_file.close()


#### Classes
class QumuloFilesCommand(object):
    ''' class wrapper for REST API cmd so that we can new them up in tests '''
    def __init__(self, argv=None):

        parser = argparse.ArgumentParser()
        parser.add_argument("--ip", "--host", default="music", dest="host", required=False,  help="specify host for sync source")
        parser.add_argument("-P", "--port", type=int, dest="port", default=8000, required=False, help="specify port on sync source to use for sync")
        parser.add_argument("-u", "--user", default="admin", dest="user", required=False, help="specify user credentials for login")
        parser.add_argument("--pass", default="admin", dest="passwd", required=False, help="specify user pwd for login")
        parser.add_argument("-b", "--buckets", type=int, default=1, dest="buckets", required=False, help="specify number of buckets")
        parser.add_argument("-v", "--verbose", required=False, dest="verbose", help="Echo stuff to ", action="store_true")
        parser.add_argument("-s", "--since", required=False, dest="since", help="Specify comparision datetime in quoted YYYY-MM-DDTHH:MM:SS format to compare (defaults to none / all files)")
        parser.add_argument("start_path", action="store", help="This is the root path on the cluster for syn")


        args = parser.parse_args()
        self.port = args.port
        self.user = args.user
        self.passwd = args.passwd
        self.host = args.host
        self.num_buckets = args.buckets
        self.verbose = args.verbose
        self.start_path = args.start_path
        if args.since is not None:
            self.since = self.since = arrow.get(args.since)
        else:
            self.since = None

        self.connection = None
        self.credentials = None

        self.login()
        # self.start_path = "/music/"
        # self.start_path = "/nfstest/"
        self.total_size = self.get_directory_size(self.start_path)
        self.max_bucket_size = self.total_size / self.num_buckets
        self.start_time = datetime.datetime.now()

        self.create_buckets()
        self.bucket_index = 0



    def login(self):
        try:
            self.connection = qumulo.lib.request.Connection(\
                                self.host, int(self.port))
            login_results, _ = qumulo.rest.auth.login(\
                    self.connection, None, self.user, self.passwd)

            self.credentials = qumulo.lib.auth.Credentials.\
                    from_login_response(login_results)
        except Exception, excpt:
            print "Error connecting to the REST server: %s" % excpt
            print __doc__
            sys.exit(1)

    def create_buckets(self):
        self.buckets = []

        if self.num_buckets == 1:
            self.buckets.append(Bucket(self.max_bucket_size, self.start_time))
        else:
            for i in range(0, self.num_buckets):
                self.buckets.append(Bucket(self.max_bucket_size, self.start_time))

    def current_bucket(self):
        return self.buckets[self.bucket_index]

    def get_next_bucket(self):
        # Only increment to a new bucket if we are not already pointing to the
        # last one
        if self.bucket_index < len(self.buckets) -1:
            self.bucket_index +=1

    def process_buckets(self):
        i = 1

        for bucket in self.buckets:

            if len(bucket.entries) > 0:
                bucket.save(i, len(self.start_path))

                if self.verbose:
                    print "--------Dumping Bucket: " + str(i) + "-------------"
                    bucket.print_contents()

            i += 1

    def get_directory_size(self, path):
        try:
            result = fs.read_dir_aggregates(self.connection, self.credentials,
                                            path=path)
        except qumulo.lib.request.RequestError, excpt:
            print "Error: %s" % excpt
            sys.exit(1)

        return int(result.data['total_capacity'])

    def process_folder(self, path):

        response = fs.read_entire_directory(self.connection, self.credentials,
                                            page_size=5000, path=path)
        nodes = None

        responses = [ r for r in response ]

        for r in responses:

            if self.since is not None:
                # 'change_time' instead of 'max_ctime'
                nodes = [n for n in r['files'] if arrow.get(str(n['change_time'])) >= self.since]
            else:
                nodes = r['files']

            if nodes:
                self.process_folder_contents(nodes, path)

    def process_folder_contents(self, dir_contents, path):

        for entry in dir_contents:
            size = 0
            if entry['type'] == "FS_FILE_TYPE_FILE":
                size = int(entry['size'])
            else:
                size = self.get_directory_size(entry['path'])

            # File or dir fits in the current bucket -> add it
            if size <= self.current_bucket().remaining_capacity():
                self.current_bucket().add(entry, path, size)
            # This item is too large to fit in the bucket.
            # Check if it is a dir and traverse it.
            # We can pick some files within in
            elif (entry['type'] == "FS_FILE_TYPE_DIRECTORY"):
                new_path = path + entry['name'] + "/"
                self.process_folder(new_path)
            # Don't leave an empty bucket.
            elif self.current_bucket().bucket_count() == 0:
               self.current_bucket().add(entry, path, size)
            #Out of space in the current bucket and this is a file
            #Create a new bucket and add the item to it
            else:
                print "Filled bucket " + str(self.bucket_index)
                #self.current_bucket().print_contents()

                self.get_next_bucket()
                self.current_bucket().add(entry, path, size)


### Main subroutine
def main():
    ''' Main entry point '''
    command = QumuloFilesCommand(sys.argv)
    command.process_folder(command.start_path)
    command.process_buckets()

# Main
if __name__ == '__main__':
    main()