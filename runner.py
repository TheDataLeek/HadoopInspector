#!/usr/bin/env python3

import sys
import os
import argparse
import json

def main():
    tests = get_tests()


def get_tests()
    testdir = './tests'
    testpaths = []
    for dirpath, dirnames, filenames in os.walk(testdir):
        for filename in filenames:
            print(os.path.join(dirpath, filename))


if __name__ == '__main__':
    sys.exit(main())
