#!/usr/bin/env python

import os, sys, time
from os.path import dirname, basename, exists, isfile, isdir
import json
import argparse

sys.path.insert(0, dirname(dirname(os.path.abspath(__file__))))
import hadoopinspector.core as core
__version__ = '0.0.2'

def main():
    args = get_args()
    json_results = transform_args(args)
    print(json_results)


def transform_args(args):
    results_dict = {}
    results_dict["rc"] = args.rc
    if args.violation_cnt is not None:
        results_dict["violations"] = args.violation_cnt
    elif args.kv is not None:
        for pair in args.kv:
            key, value = pair.split(':')
            results_dict[key] = value
    return json.dumps(results_dict)



def get_args():
    parser = argparse.ArgumentParser(description='Generates json string to echo to runner')
    parser.add_argument('--rc',
                        default='0',
                        help='Return code.  Default is 0.')
    parser.add_argument('--violation-cnt',
                        help='Rule or profiler violation count.')
    parser.add_argument('--kv',
                        nargs='*',
                        help='key-value pairs - colon between k & v, each k:v in quotes')

    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='displays version number')
    parser.add_argument('--long-help',
                        action='store_true',
                        help='Provides more verbose help')

    args = parser.parse_args()
    if args.long_help:
        print(__doc__)
        sys.exit(0)

    return args


if __name__ == '__main__':
    sys.exit(main())
