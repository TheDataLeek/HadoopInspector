#!/usr/bin/env python2

"""
    This source code is protected by the BSD license.  See the file "LICENSE"
    in the source code root directory for the full language or refer to it here:
    http://opensource.org/licenses/BSD-3-Clause
    Copyright 2015 Will Farmer and Ken Farmer
"""

# Run PyTest 2!!!!!

import hadinsp_httpserver as server

class TestServer(object):
    def setup_method(self, method):
        self.frontend = server.FrontEnd()
        self.frontend.config = {'db': 'results.sqlite'}

    def test_python_versioning(self):
        assert(self.frontend.python2 == True)
        assert(self.frontend.python3 == False)

    def test_database_file(self):
        assert(self.frontend.get_database() == '../scripts/results.sqlite')
