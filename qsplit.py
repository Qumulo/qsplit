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
- divide a qumulo cluster into N equal partitions based on size. A partition is a list of paths.
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

    def add_without_duplicate(self, entry_to_add):
        ''' 
        add_without_duplicate will add a (directory) entry to bucket entries 
        only if we're not already handling contents for that directory. 
        Example 1:
        last_bucket_entry = "iTunes/TV/Pan Am/._05 One Coin in a Fountain (HD).m4v"
        entry_to_add = "iTunes/TV/Pan Am/"

        In this case we're already splitting the Pan Am directory so we don't add
        the directory itself as an entry (which just creates more work for rsync or robocopy).

        Example 2:
        last_bucket_entry = "iTunes/TV/Pan Am/._05 One Coin in a Fountain (HD).m4v"
        entry_to_add = "iTunes/TV/Pan Am/03 Ich Bin Ein Berliner (HD).m4v

        In this case we add entry_to_add to the current bucket.
        '''
        if len(self.entries) > 0:
            if  not(entry_to_add["path"] in self.entries[-1]["path"]):
                self.entries.append(entry_to_add)
        else:
            self.entries.append(entry_to_add)

        return

    def add(self, entry, current_path, size, robocopy=False):
        ''' add an entry to the current bucket.  If there isn't space for the entry
            in the bucket such that we'll exceed max_bucket_size, create a new bucket
            and make it the current one. '''

        path = current_path + entry['name']

        if robocopy:
            path = path.replace('/','\\')

        bucket_entry = { "path" : path, "size" : size }

        # TODO: Refactor!
        if len([ e for e in self.entries if e['path']==bucket_entry['path']]) == 0:

            # if we're creating robocopy buckets, don't add files just folders
            if robocopy and entry['type'] == "FS_FILE_TYPE_DIRECTORY" :
                # self.entries.append(bucket_entry)
                self.add_without_duplicate(bucket_entry)
            elif not robocopy:
                # self.entries.append(bucket_entry)
                self.add_without_duplicate(bucket_entry)
            # decrement the size, regardless
            self.free_space -= size
        elif len(self.entries) == 0:
            # if we're creating robocopy buckets, don't add files just folders
            if robocopy and entry['type'] == "FS_FILE_TYPE_DIRECTORY" :
                # self.entries.append(bucket_entry)
                self.add_without_duplicate(bucket_entry)
            elif not robocopy:
                # self.entries.append(bucket_entry)
                self.add_without_duplicate(bucket_entry)
            # decrement the size, regardless
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


    def save(self, bucket_number, offset, robocopy):

        # create a file for bucket path entries
        filename = "qsync_" + self.start_time.strftime("%Y%m%d%H%M_bucket") + str(bucket_number) + ".txt"
        bucket_file = open(filename, 'w+')

        for entry in self.entries:
            if robocopy:
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
        self.robocopy = args.robocopy
        self.verbose = args.verbose
        self.start_path = args.start_path

        if args.since is not None:
            self.since = self.since = arrow.get(args.since)
        else:
            self.since = None

        self.connection = None
        self.credentials = None

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

            bucket.save(i, len(self.start_path), self.robocopy)

            if self.verbose:
                print "--------Dumping Bucket: " + str(i) + "-------------"
                bucket.print_contents()

            i += 1

    def get_directory_size(self, path):
        try:
            result = fs.read_dir_aggregates(self.connection, self.credentials,
                                            path=path)
        except qumulo.lib.request.RequestError, excpt:
            sys.exit(1)

        return int(result.data['total_capacity'])

    def process_folder(self, path):

        try:
            response = fs.read_entire_directory(self.connection, self.credentials, page_size=1000, path=path)
        except Exception, excpt:
            print "Error in read_entire_directory: %s" % excpt
            sys.exit(1)

        for r in response:

            # self.process_folder_contents(r.data['files'], path)
            # instead of passing the entire data['files'] collection
            # for r, pass only the ones since change_time
            if self.since:
                nodes = []
                nodes = [ n for n in r.data['files'] if arrow.get(n['change_time']) >= self.since]
                if self.verbose:
                    print("processing " + str(len(nodes)) + " in path " + path)
                self.process_folder_contents(nodes, path)
            else:
                if self.verbose:
                    print("processing " + str(len(r.data['files'])) + " in path " + path)
                self.process_folder_contents(r.data['files'], path)


    def process_folder_contents(self, dir_contents, path):

        for entry in dir_contents:
            size = 0
            if entry['type'] == "FS_FILE_TYPE_FILE" or entry['type'] == "FS_FILE_TYPE_SYMLINK": 
                size = int(entry['size'])
            else:
                size = self.get_directory_size(entry['path'])

            # File or dir fits in the current bucket or 
            # we're on the last bucket already -> add it
            if (size <= self.current_bucket().remaining_capacity()) or (self.bucket_index == (self.num_buckets-1)):
                self.current_bucket().add(entry, path, size, self.robocopy)
            else:
                # This item is too large to fit in the bucket.
                # Check if it is a dir and traverse it.
                # We can pick files within  
                if (entry['type'] == "FS_FILE_TYPE_DIRECTORY"):
                    new_path = path + entry['name'] + "/"
                    if self.verbose:
                        print("Calling process_folder with " + new_path + "... ")
                    self.process_folder(new_path)
                else:
                    # It is a file that doesn't fit. Start a new bucket.
                    self.get_next_bucket()
                    print "Starting bucket " + str(self.bucket_index)

                if (self.bucket_index == (self.num_buckets-1)):
                    print "Oversized: Adding " + path + " to last bucket..."
 
                self.current_bucket().add(entry, path, size, self.robocopy)


def main():
    ''' Main entry point '''

    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", "--host", default="music", dest="host", required=True,  help="Required: Specify host (cluster) for file lists")
    parser.add_argument("-P", "--port", type=int, dest="port", default=8000, required=False, help="specify port on cluster; defaults to 8000")
    parser.add_argument("-u", "--user", default="admin", dest="user", required=False, help="specify user credentials for login; defaults to admin")
    parser.add_argument("--pass", default="admin", dest="passwd", required=False, help="specify user pwd for login, defaults to admin")
    parser.add_argument("-b", "--buckets", type=int, default=1, dest="buckets", required=False, help="specify number of manifest files (aka 'buckets'); defaults to 1")
    parser.add_argument("-s", "--since", required=False, dest="since", help="Specify comparision datetime in quoted YYYY-MM-DDTHH:MM:SS format to compare (defaults to none / all files)")        
    parser.add_argument("-v", "--verbose", default=False, required=False, dest="verbose", help="Echo values to console; defaults to False ", action="store_true")
    parser.add_argument("-r", "--robocopy", default=False, required=False, dest="robocopy", help="Generate Robocopy-friendly buckets", action="store_true")
    parser.add_argument("start_path", action="store", help="Path on the cluster for file info; Must be the last argument")
    args = parser.parse_args()

    command = QumuloFilesCommand(args)

    command.process_folder(command.start_path)
    command.process_buckets()

# Main
if __name__ == '__main__':
    main()
