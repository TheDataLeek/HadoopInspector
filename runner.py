#!/usr/bin/env python3

import sys
import os
import argparse
import logging
import collections
import subprocess
import json

def main():
    args = get_args()
    configure_logger(args.logfile)

    tester = TestRunner(args.testdir)
    tester.run_tests()


def configure_logger(logfile):
    logging.basicConfig(filename=logfile,
                        level=logging.DEBUG)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--testdir', type=str, default='./tests',
                        help='Which directory to look for the tests in')
    parser.add_argument('-l', '--logfile', type=str, default='./hadoopqa.log',
                        help='Which logfile to use')
    args = parser.parse_args()
    if not os.path.isdir(args.testdir):
        print('Supplied test directory does not exist. Setting to default')
        args.testdir = './tests'
    return args


class TestRunner(object):
    def __init__(self, testdir, tests=None):
        """
        This is the general test runner class

        :param testdir: string
            This is the directory in which all tests live.
            It must have the structure like the following:
                |-tests
                    |-checks
                    |-rules
        :param tests: collections.namedtuple('tests', ['checks', 'rules'])
            checks -> list of strings containing the checkfiles to run
            rules  -> list of strings containing the rulefiles to run

        """
        self.testdir = testdir
        logging.info('Examining Tests in {}'.format(testdir))
        if tests is None:
            tests = self._get_tests(self.testdir)
            self.rules = tests.rules
            self.checks = tests.rules
        else:
            self.checks = tests.checks
            self.checks = tests.rules

    def run_tests(self):
        """
        Run all tests

        :return: None
        """
        self.run_rules(self.rules)
        self.run_checks(self.checks)

    def run_rules(self, rules):
        count = self._aggregate_results(rules)
        return count

    def run_checks(self, checks):
        count = self._aggregate_results(checks)
        return count

    def _aggregate_results(self, files):
        count = 0
        for f in files:
            raw_output = self._run_file(f)
            json_output = json.loads(raw_output)
        return count

    def _run_file(self, f):
        process = subprocess.Popen(['python', f], stdout=subprocess.PIPE)
        process.wait()
        output = process.stdout.read()
        return output

    def _get_tests(self, testdir):
        """
        Searches testdir for tests to run

        :param testdir: str
            Directory containing tests
        :return: tuple(list, list)
            A tuple containing the rules and the checks
        """
        testdir = './tests'
        testpaths = []
        Tests = collections.namedtuple('Tests', ['rules', 'checks'])
        for dirpath, dirnames, filenames in os.walk(testdir):
            for filename in filenames:
                testpaths.append(os.path.join(dirpath, filename))
        return Tests(rules=filter(lambda t: 'rules' in t, testpaths),
                      checks=filter(lambda t: 'checks' in t, testpaths))


if __name__ == '__main__':
    sys.exit(main())
