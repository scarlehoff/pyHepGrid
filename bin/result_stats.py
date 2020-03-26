#!/usr/bin/env python3
import os
import sys
import time
import argparse as ap

# Janky af, but allows script to be called from other directories
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from pyHepGrid.src.header import production_base_dir as rootdir  # noqa: E402


def do_search(args, path):
    if args.search is not None:
        for searchstr in args.search:
            if args.case_insensitive:
                if searchstr.lower() not in path.lower():
                    return False
            else:
                if searchstr not in path:
                    return False
    return True


def do_reject(args, path):
    if args.reject is not None:
        for rejectstr in args.reject:
            if args.case_insensitive:
                if rejectstr.lower() in path.lower():
                    return False
            else:
                if rejectstr in path:
                    return False
    return True


def search_matches_path(args, path):
    if any(not i(args, path) for i in [do_search, do_reject]):
        return False
    return True


def print_output(stats, do_total):
    print(RuncardStats.output_header())
    print(RuncardStats.divider_string())
    no_files = 0
    for i in stats:
        no_files += i.no_files
        print(i)
    if do_total:
        print(RuncardStats.divider_string())
        print(RuncardStats.print_format("Total Files", no_files, ""))


def get_stats(args):
    stats = []
    for thing in os.listdir(rootdir):
        fullpath = os.path.join(rootdir, thing)
        if os.path.isdir(fullpath):
            if search_matches_path(args, fullpath):
                try:
                    retval = RuncardStats(fullpath, thing, args)
                    stats.append(retval)
                except OSError:
                    pass
    return stats


class RuncardStats:

    print_string = "{0:<{runcard}} | {1:<{no_files}} | {2:<{modified_time}}"
    tag_flag = False
    # Identifies attr names with column titles
    column_names = {
        "runcard": "Runcard Name",
        "tag": "Tag",
        # Currently unused
        "no_files": "No. files",
        "modified_time": "Last Modified",
    }
    pads = {"runcard": 5, "tag": 5, "no_files": 9, "modified_time": 20}

    def print_format(*args):
        return RuncardStats.print_string.format(*args, **RuncardStats.pads)

    def divider_string():
        return "=" * len(RuncardStats.print_format(0, 0, 0, 0))

    def adjust_column_widths(self):
        for key in RuncardStats.pads:
            RuncardStats.pads[key] = max(
                RuncardStats.pads[key], len(str(getattr(self, key)))
            )

    def __init__(self, fullpath, directory, parseargs):
        self.directory = directory
        self.logpath = os.path.join(fullpath, "log/")
        self.logfiles = os.listdir(self.logpath)
        self.no_files = len(self.logfiles)
        self.runcard = self.directory.replace("results_", "").split(".run-")[0] + ".run"
        try:
            self.tag = self.directory.split(".run-")[1]
        except IndexError:
            self.tag = " - "
        self.modified_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(self.logpath))
        )
        self.args = parseargs
        if self.args.tag:
            RuncardStats.print_string = (
                "{0:<{runcard}} | {1:<{tag}} | "
                + "{2:<{no_files}} | {3:<{modified_time}}"
            )
            RuncardStats.tag_flag = True
        self.adjust_column_widths()

    def output_header():
        if not RuncardStats.tag_flag:
            retstr = RuncardStats.print_format(
                "Runcard Name", "No. Files", "Last Modified"
            )
        else:
            retstr = RuncardStats.print_format(
                "Runcard Name", "Tag", "No. Files", "Last Modified"
            )
        return retstr

    def __repr__(self):
        if not self.args.tag:
            retstr = RuncardStats.print_format(
                self.runcard, self.no_files, self.modified_time
            )
        else:
            retstr = RuncardStats.print_format(
                self.runcard, self.tag, self.no_files, self.modified_time
            )
        return retstr


def parser_setup():
    parser = ap.ArgumentParser(description="Analyse results output directory.")
    parser.add_argument(
        "--rev", help="reverse sort.", action="store_false", default=True
    )
    parser.add_argument(
        "--runcard", "-rc", help="sort by runcard name.", action="store_true"
    )
    parser.add_argument(
        "--tag", "-t", help="display runcard tags.", action="store_true"
    )
    parser.add_argument(
        "--nofiles", "-n", help="sort by no files.", action="store_true"
    )
    parser.add_argument(
        "--modtime", "-m", help="sort by modification time.", action="store_true"
    )
    parser.add_argument(
        "--case_insensitive",
        "-i",
        help="case insensitive search/reject.",
        action="store_true",
    )
    parser.add_argument(
        "--search",
        "-f",
        "-s",
        help="search for specific string(s) in runcard dir.",
        nargs="+",
    )
    parser.add_argument(
        "--reject", "-r", help="reject specific string(s) in runcard dir.", nargs="+"
    )
    parser.add_argument(
        "--total", "-tot", help="include total # of results", action="store_true"
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parser_setup()
    stats = get_stats(args)
    if args.runcard:
        stats.sort(reverse=args.rev, key=lambda x: x.directory)
    if args.modtime:
        stats.sort(reverse=args.rev, key=lambda x: x.modified_time)
    if args.nofiles:
        stats.sort(reverse=args.rev, key=lambda x: x.nofiles)
    if len(stats) > 0:
        print_output(stats, args.total)
    else:
        print("No results found.")
