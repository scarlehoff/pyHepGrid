from __future__ import print_function
import argparse as ap

def get_args():
    parser = ap.ArgumentParser(
        description="general manager for LFN type stuff")
    parser.add_argument(
        "directories",
        help="lfn directories to look in",
        nargs="*")
    parser.add_argument(
        "--search",
        "-s",
        help="search strings for file name",
        nargs="+")
    parser.add_argument(
        "--reject",
        "-r",
        help="reject strings for file name",
        nargs="+")
    parser.add_argument(
        "--copy",
        "-cp",
        help="Copy to current working directory",
        action="store_true",
        default=False)
    parser.add_argument(
        "--delete", 
        "-d", 
        "-rm", 
        help="Delete selected files",
        action="store_true", default=False)
    parser.add_argument(
        "--case_insensitive",
        "-i",
        help="case insensitive search",
        action="store_true",
        default=False)
    parser.add_argument(
        "--time",
        "-t",
        help="time output",
        action="store_true",
        default=False)
    parser.add_argument(
        "--output_directory",
        "-o",
        help="output directory for copy")
    parser.add_argument(
        "--no_threads",
        "-j",
        help="""no. threads to run in parallel for copying from
                grid/deletions. Default=16""",
        action="store",
        nargs="?",
        default=16,
        type=int)
    parser.add_argument(
        "--unique_runcards",
        "-u",
        help="""only display unique runcards [WIP].
                Doesn't work if numbers are in file name!""",
        action="store_true")
    parser.add_argument(
        "--permissions",
        "-p",
        help="display permissions for files",
        action="store_true")
    parser.add_argument(
        "--verbose",
        "-v",
        help="Gives extra output for lfc-ls calls",
        action="store_true")
    parser.add_argument(
        "--sort",
        "-st",
        help="Sort output",
        action="store_true")
    parser.add_argument(
        "--sortkey",
        "-sk",
        help="key to sort with",
        type=str)
    parser.add_argument(
        "--reverse",
        "-rev",
        help="Reverse sort",
        action="store_true",
        default=False)
    parser.add_argument(
        "--copy_to_grid",
        "-cpg",
        nargs="+",
        help="""copy files specified to grid directory specified
                with -o flag""")
    args = parser.parse_args()

    if args.output_directory is not None:
        args.output_directory = os.path.expanduser(args.output_directory)
        if not os.path.isdir(args.output_directory):
            os.makedirs(args.output_directory)

    return args
