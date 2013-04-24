#!/usr/bin/env python

import argparse

parser = argparse.ArgumentParser("split file from input into shards")

parser.add_argument('shards', type=int, action='store', help='Number of shards.')
parser.add_argument('shard_prefix', type=str, action='store', help='Prefix name for shards.')
parser.add_argument('files', type=str, action='store', nargs='*', default=['/dev/stdin'], help='Name of bucket')

def main():
    args = parser.parse_args()
    shard_files = []
    for num in range(args.shards):
        shard = args.shard_prefix + "." + str(num)
        shard_files.append(open(shard, 'w'))
    n = 0
    for path in args.files:
        with open(path, 'r') as f:
            for line in f.readlines():
                shard_files[n % args.shards].write(line)
                n+=1

if __name__ == "__main__":
    main()
