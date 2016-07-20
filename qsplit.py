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
./qsplit.py --host ip_address|hostname [options] path

If you are targeting a Windows environment and want to use robocopy as the data mover tool,
specify a -r (or --robocopy) option (note the trailing slash after path):

./qsplit.py r \\servername\path\ --host music /media/ --buckets 4
'''

# Import python libraries
import argparse
import arrow
import datetime
import os
import re
import sys

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

    def add(self, entry, current_path, size, use_robocopy=False, path_prefix=None):
        ''' add an entry to the current bucket.  If there isn't space for the entry
            in the bucket such that we'll exceed max_bucket_size, create a new bucket
            and make it the current one. '''
        if use_robocopy:
            # flip any slashes in the path and prepend the robocopy-needed \\server\path\
            path = current_path + entry['name']
            path = path_prefix + path.replace('/','\\')
            # print "prefix is " + path_prefix + " and path is " + path
        else:
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


    def save(self, bucket_number, offset, use_robocopy):

        # create a file for bucket path entries
        filename = "qsync_" + self.start_time.strftime("%Y%m%d%H%M_bucket") + str(bucket_number) + ".txt"
        bucket_file = open(filename, 'w+')

        for entry in self.entries:
            if use_robocopy:
                bucket_file.write(entry['path'].encode('utf-8') + '\n')
            else:
                relative_path = entry['path'][offset:]
                # relative_path = entry['path']
                bucket_file.write(relative_path.encode('utf-8') + '\n')

        bucket_file.close()


#### Classes
class QumuloFilesCommand(object):
    ''' class wrapper for REST API cmd so that we can new them up in tests '''
    def __init__(self, args=None):

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

        if args.robocopy is not None:  # it's Windows, Jake....
            self.use_robocopy = True
            path_prefix = "\\" + args.robocopy + "\\"
            # path_prefix requires some fixup ... \\server\path becomes \\server\\path, should be \\server\path\
            self.path_prefix = path_prefix
        else:
            self.use_robocopy = False
            self.path_prefix = None

        self.login()
        self.total_size = self.get_directory_size(self.start_path)
        self.max_bucket_size = self.total_size / self.num_buckets

        if self.verbose:
            print "--------Total size: " + str(self.total_size) + " -------------"
            print "--------Max Bucket size: " + str(self.max_bucket_size) + " -------------"

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
        if self.bucket_index < self.num_buckets: 
            print "Filled bucket " + str(self.bucket_index)
            if self.verbose:
                self.current_bucket().print_contents()
            self.bucket_index +=1

    def process_buckets(self):
        i = 1
        for bucket in self.buckets:

            bucket.save(i, len(self.start_path), self.use_robocopy)

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

    def add_node(self, path):
        # API Call #2:  fs.read_dir_aggregates for a single entry
        agg = fs.read_dir_aggregates(self.connection, self.credentials, path, max_entries=1).data
        # return True if max_ctime is with the range of months we care about
        # (i.e. ready to age out)
        change_time = arrow.get(agg['max_change_time'])
        if (change_time >= self.since):
            return True
        else:
            return False

    def process_folder(self, path):

        response = fs.read_entire_directory(self.connection, self.credentials,page_size=15, path=path)

        nodes = []

        for r in response:

            if r.data['type'] == 'FS_FILE_TYPE_DIRECTORY' and self.since is not None:
                if self.add_node(r.data['path']):
                    nodes.append(r.data) 
            else:
                nodes.append(r.data)

        if len(nodes) > 0:
            self.process_folder_contents(nodes, path)

    def process_folder_contents(self, dir_contents, path):

        for entry in dir_contents:
            size = 0
            if entry['type'] == "FS_FILE_TYPE_FILE":
                size = int(entry['size'])
            else:
                size = self.get_directory_size(entry['path'])

            # File or dir fits in the current bucket or 
            # we're on the last bucket already -> add it
            if (size <= self.current_bucket().remaining_capacity()) or (self.bucket_index == (self.num_buckets-1)):
                self.current_bucket().add(entry, path, size, self.use_robocopy, self.path_prefix)
            else:
                # This item is too large to fit in the bucket.
                # Check if it is a dir and traverse it.
                # We can pick files within  
                if (entry['type'] == "FS_FILE_TYPE_DIRECTORY"):
                    new_path = path + entry['name'] + "/"
                    self.process_folder(new_path)
                else:
                    # It is a file that doesn't fit. Start a new bucket.
                    self.get_next_bucket()
                    print "Starting bucket " + str(self.bucket_index)

                if (self.bucket_index == (self.num_buckets-1)):
                    print "Oversized: Adding " + path + " to last bucket..."
 
                self.current_bucket().add(entry, path, size, self.use_robocopy, self.path_prefix)


### Main subroutine
def main():
    ''' Main entry point '''

    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", "--host", default="music", dest="host", required=True,  help="Required: Specify host (cluster) for file lists")
    parser.add_argument("-P", "--port", type=int, dest="port", default=8000, required=False, help="specify port on cluster; defaults to 8000")
    parser.add_argument("-u", "--user", default="admin", dest="user", required=False, help="specify user credentials for login; defaults to admin")
    parser.add_argument("--pass", default="admin", dest="passwd", required=False, help="specify user pwd for login, defaults to admin")
    parser.add_argument("-b", "--buckets", type=int, default=1, dest="buckets", required=False, help="specify number of files; defaults to 1")
    parser.add_argument("-r", "--robocopy", dest="robocopy", required=False, help="specify leading server and path in form '\\\\server\path' (use quotes, no trailing slash)")
    parser.add_argument("-s", "--since", required=False, dest="since", help="Specify comparision datetime in quoted YYYY-MM-DDTHH:MM:SS format to compare (defaults to none / all files)")        
    parser.add_argument("-v", "--verbose", default=False, required=False, dest="verbose", help="Echo values to console; defaults to False ", action="store_true")
    parser.add_argument("start_path", action="store", help="Path on the cluster for file info; Must be the last argument")

    args = parser.parse_args()

    command = QumuloFilesCommand(args)

    command.process_folder(command.start_path)
    command.process_buckets()

# Main
if __name__ == '__main__':
    main()