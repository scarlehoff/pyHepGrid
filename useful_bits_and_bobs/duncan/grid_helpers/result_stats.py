#!/usr/bin/env python3
import os
import time
import argparse as ap
rootdir =os.path.expanduser('~/working/RESULTS/')

class RuncardStats():
    def __init__(self,fullpath, directory):
        self.directory = directory
        self.logpath = os.path.join(fullpath,"log/")
        self.logfiles = os.listdir(self.logpath)
        self.nofiles = len(self.logfiles)
        self.modified_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                           time.localtime(os.path.getmtime(self.logpath)))

    def __repr__(self):
        retstr = "Runcard: {0:<40} No. Files: {1:^7} Modified: {2}".format(
            self.directory.replace("results_",""),self.nofiles, self.modified_time )    
        return retstr

if __name__ == "__main__":
    parser = ap.ArgumentParser(description='Analyse results output directory.')
    parser.add_argument('--rev', help='reverse sort.', action='store_false',default = True)
    parser.add_argument('--runcard','-r', help='sort by runcard name.',action='store_true')
    parser.add_argument('--nofiles','-n', help='sort by no files.',action='store_true')
    parser.add_argument('--modtime','-m', help='sort by modification time.',action='store_true')
    args = parser.parse_args()
    stats = []
    for thing in os.listdir(rootdir):
        fullpath = os.path.join(rootdir,thing)
        if os.path.isdir(fullpath):
            try:
                retval = RuncardStats(fullpath,thing)
                stats.append(retval)
            except OSError as e:
                pass
    if args.runcard:
        stats.sort(reverse=args.rev,key=lambda x: x.directory)
    if args.modtime:
        stats.sort(reverse=args.rev,key=lambda x: x.modified_time)
    if args.nofiles:
        stats.sort(reverse=args.rev,key=lambda x: x.nofiles)
    for i in stats:
        print(i)
