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

import os
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from get_stats import FileStatsCommand

class CommandTests(unittest.TestCase):
    ''' Test runner for commands '''

    def setUp(self):
        # tweak these to match your local environment
        self.test_argv = ['./get_stats.py',\
                           '--host',\
                           'dev',\
                           '--port',\
                           '20095',\
                           '--user',\
                           'admin',\
                           '--pass',\
                           'admin']

    def test_get_stats(self):
        ''' test getting filesystem stats using the Qumulo REST API '''
        command = FileStatsCommand(argv=self.test_argv)
        command.login()
        result = command.get_stats()
        keys = [u'raw_size_bytes', u'free_size_bytes', \
                u'total_size_bytes', u'block_size_bytes']
        self.assertEqual(result.data.keys(), keys)
