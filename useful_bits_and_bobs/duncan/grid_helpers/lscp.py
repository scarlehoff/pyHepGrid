#!/usr/bin/env python3.4
from __future__ import print_function
import datetime
import itertools
import lscp_args
import multiprocessing as mp
import os
import string
import subprocess as sp

lfn = "lfn:"
file_count_reprint_no = 15


class LFNFile():
    def __init__(self, line):
        self.__line = line
        self.__split = [x.strip() for x in line.split()]
        self.fname = self.__split[-1]
        self.time = self.__split[-2]
        self.month = self.__split[-3]
        self.day = self.__split[-4]
        self.permissions = self.__split[0]

    def return_line_as_str(self, args):
        ln = self.__split
        retstr = "{0:50}".format(self.fname)
        if args.verbose:
            retstr += "{0} {1} {2:15}".format(self.month, self.day,
                                           self.time)
        if args.permissions:
            retstr += "{0:10}".format(self.permissions)
        return retstr

    def __repr__(self):
        ln = self.__split
        return "{3:45} {4} {1} {0} {2:17} No. files: {5:7}  {6}".format(
            ln[-4],ln[-3],ln[-2],ln[-1], self.time, ln[1], ln[0])


def bash_call(*args, **kwargs):
    return [f.decode('utf-8') for f in
            sp.Popen(args, stdout=sp.PIPE, stderr=sp.PIPE,
                     **kwargs).communicate()[0].split(b"\n")
            if f != b""]


def get_usable_threads(no_threads, no_files):
    return max(min(no_threads, no_files), 1)


def copy_file_to_grid(infile, griddir, file_no, no_files):
    infile_loc, infile_name = os.path.split(infile)
    lcgname = "{0}{1}/{2}".format(lfn, griddir, infile_name)
    filename = "file:{0}".format(infile)
    print(
        "Copying {0} to {1} [{2}/{3}]".format(filename, lcgname,
                                              file_no+1, no_files))
    bash_call("lcg-cr", "--vo", "pheno", "-l", lcgname, filename)


def delete_file_from_grid(xfile, lfndirectory, file_no, no_files):
    lcgname = "{0}{1}/{2}".format(lfn, lfndirectory, xfile.fname)
    print(
        "Deleting {0} [{1}/{2}]".format(lcgname, file_no+1, no_files))
    bash_call("lcg-del", lcgname, "-a")
    lfnname = "{0}/{1}".format(lfndirectory, xfile.fname)
    bash_call("lfc-rm", lfnname)


def copy_lfn_file_to_local(lfnfile, localfile):
    bash_call("lcg-cp", lfnfile, localfile)


def copy_to_dir(infile, directory, args, file_no, no_files):
    lcgname = "{0}{1}/{2}".format(lfn, directory, infile.fname)
    if args.output_directory is not None:
        xfile = os.path.join(args.output_directory, infile.fname)
    else:
        xfile = os.path.join(os.getcwd(), infile.fname)
    print("Copying {0} to {1} [{2}/{3}]".format(lcgname, xfile,
                                                file_no+1, no_files))
    copy_lfn_file_to_local(lcgname, xfile)


def _search_match(search_str, fileobj, args):
    if args.case_insensitive:
        return search_str.upper() in fileobj.fname.upper()
    else:
        return search_str in fileobj.fname


def do_search(files, args):
    for search_str in args.search:
        files = [x for x in files if _search_match(search_str, x, args)]
    return files


def do_reject(files, args):
    for rej_str in args.reject:
        files = [x for x in files if not _search_match(rej_str, x, args)]
    return files


def do_copy(lfndirectory, args, files):
    no_files = len(files)
    print("> Copying {0} file{1}...".format(no_files,
                                            ("" if no_files == 1 else "s")))
    pool = mp.Pool(processes=get_usable_threads(args.no_threads, no_files))
    pool.starmap(copy_to_dir, zip(files, itertools.repeat(lfndirectory),
                                  itertools.repeat(args),
                                  range(len(files)),
                                  itertools.repeat(no_files)), chunksize=1)


def do_delete(files, args):
    no_files = len(files)
    query_str = "Do you really want to delete {1} {0} file{2} [y/n]?\n".format(
        no_files,
        ("this" if no_files == 1 else "these"),
        ("" if no_files == 1 else "s"))
    deletion_confirmed = get_yes_no(input(query_str))
    if deletion_confirmed:
        print("> Deleting files...")
        pool = mp.Pool(processes=get_usable_threads(args.no_threads, no_files))
        pool.starmap(delete_file_from_grid, zip(files,
                                                itertools.repeat(lfndirectory),
                                                range(len(files)),
                                                itertools.repeat(no_files)),
                     chunksize=1)


def get_yes_no(string):
    if string.lower().startswith("y"):
        return True
    return False


def get_unique_runcards(files):
    remove_digits = str.maketrans('', '', string.digits)
    return list(set(f.fname.translate(remove_digits).replace("-.", "-SEED.")
                    for f in files))


def lfc_ls_obj_wrapper(*args):
    files = bash_call("lfc-ls", "-l", *args)
    return [LFNFile(x) for x in files]


def print_files(files, args):
    print("\n".join(i.return_line_as_str(args) for i in files))


def sort_files(files, args):
    if not args.sort:
        return files
    else:
        if args.sortkey is not None:
            sortattr = args.sortkey 
        else:
            sortattr = "fname"
        files.sort(key=lambda f : getattr(f,sortattr), 
                   reverse = args.reverse)
        return files


def parse_directory(lfndirectory):

    files = lfc_ls_obj_wrapper(lfndirectory)
    if args.search is not None:
        files = do_search(files, args)
    if args.reject is not None:
        files = do_reject(files, args)

    no_files = len(files)
    files_found = no_files > 0

    punctuation = ":" if files_found else "."
    print("> {0} matching files found in {1}{2} ".format(no_files,
                                                         lfndirectory,
                                                         punctuation))
    files = sort_files(files, args)

    if files_found:
        if not args.unique_runcards:
            print_files(files, args)
        else:
            # Need dummy LFNFile obj here?
            unique_runcards = get_unique_runcards(files)
            print("\n".join(i for i in unique_runcards))
            print("> {0} unique runcards".format(len(unique_runcards)))
        if no_files > file_count_reprint_no:
            print("> {0} files matched".format(no_files))
    else:
        return

    if args.copy:
        do_copy(lfndirectory, args, files)
    if args.delete:
        do_delete(files, args)


def list_dirs(args_obj, *args):
    dirs = lfc_ls_obj_wrapper(*args)
    print("> LFN directories found:")
    print_files(dirs, args_obj)


def do_copy_to_grid(args):
    if args.output_directory is not None:
        output = args.output_directory
    else:
        print("No output directory specified. Exiting...")
        return
    no_files = len(args.copy_to_grid)
    for file_no, xfile in enumerate(args.copy_to_grid):
        copy_file_to_grid(xfile, output, file_no, no_files)


if __name__ == "__main__":
    args = lscp_args.get_args()
    if args.time:
        start_time = datetime.datetime.now()

    if args.copy_to_grid is not None:
        do_copy_to_grid(args)

    elif args.directories == []:
        list_dirs(args)
    else:
        for lfndirectory in args.directories:
            parse_directory(lfndirectory)

    if args.time:
        end_time = datetime.datetime.now()
        total_time = (end_time-start_time).__str__().split(".")[0]
        print("> Time taken {0}".format(total_time))
