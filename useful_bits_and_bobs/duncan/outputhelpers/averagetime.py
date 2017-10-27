from __future__ import division, print_function
import os
import sys
sys.path.insert(0, os.path.expanduser('~/pythonscripts/'))
import lib_functions as lf
import argparse as ap
import numpy as np
import matplotlib as mpl
mpl.use("Agg")
import matplotlib.pyplot as plt


if __name__ == "__main__":
    parser = ap.ArgumentParser(
        description="Generate timing information from a directory of log files.")

    parser.add_argument('directory', metavar='dir',
                        help='Directory to search.')
    parser.add_argument("--histogram", "-hs",
                        help="Include histogram of times.", action="store_true")
    args = parser.parse_args()
    times = []
    for infile in os.listdir(args.directory):
        full_infile = os.path.join(args.directory, infile)
        if infile.endswith(".log"):
            with open(full_infile) as f:
                time_data = lf.tail(f).split()[-2]
                times.append(float(time_data))
    times = np.array(times)
    mean = np.mean(times)
    stdev = np.std(times)
    tot = np.sum(times)
    long_run = np.max(times)
    short_run = np.min(times)
    hr = " hours"
    print("Input directory: " + args.directory)
    print("=============================")
    print("Mean time: " + "{0:10.4f}".format(mean) + hr)
    print("Standard Deviation: " + "{0:10.4f}".format(stdev) + hr)
    print("Total time: " + "{0:10.4f}".format(tot) + hr)
    print("Number of runs: " + str(len(times)))
    print("=============================")
    print("Longest Run: " + "{0:10.4f}".format(long_run) + hr)
    print("Shortest run: " + "{0:10.4f}".format(short_run) + hr)
    if args.histogram:
        n, bins, patches = plt.hist(times, len(
            times) / 30, facecolor='green', normed=1)
        plt.xlabel("Time")
        plt.ylabel("Probability")
        plt.show()
