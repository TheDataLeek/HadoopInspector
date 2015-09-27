#!/usr/bin/env python2

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
