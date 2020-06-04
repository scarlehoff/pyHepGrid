#!/usr/bin/env python3
from __future__ import division, print_function
import datetime
import importlib
import itertools as it
import multiprocessing as mp
import os
import re
import pyHepGrid.src.header as config
import subprocess
import tarfile
import sys

logseed_regex = re.compile(r".s([0-9]+)\.[^\.]+$")  # Matches seeds in logfiles
tarfile_regex = re.compile(r"-([0-9]+)\.tar.gz+$")  # Matches tarfiles
# Matches PROGRAM log files
logfile_regex = re.compile(r"\w+\.\w+\.s([0-9]+)\.log")

# CONFIG
no_processes = config.finalise_no_cores
verbose = config.verbose_finalise
DELETE_CORRUPTED = False
MAX_ATTEMPTS = 5
FINALISE_ALL = False
RECURSIVE = config.recursive_finalise
ls_protocol = "dav"
copy_protocol = "xroot"

# Set up environment
os.environ["LFC_HOST"] = config.LFC_HOST
os.environ["LCG_CATALOG_TYPE"] = config.LFC_CATALOG_TYPE

if FINALISE_ALL:
    runcards = os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.append(runcards)
    rc = importlib.import_module(config.finalise_runcards.replace("/", "."))
else:
    sys.path.append(os.path.dirname(os.path.expanduser(sys.argv[2])))
    rc = importlib.import_module(
        os.path.basename(sys.argv[2].replace(".py", "")))


if config.timeout is not None:
    timeoutstr = "-t {0}".format(config.timeout)
else:
    timeoutstr = ""


def mkdir(directory):
    os.makedirs(directory, exist_ok=True)


def get_output_dir_name(runcard, rtag):
    basedir = config.production_base_dir
    subdir = "{0}{1}".format(config.finalise_prefix, runcard)
    if rtag is None:
        return os.path.join(basedir, subdir)
    else:
        return os.path.join(basedir, rtag, subdir)


def get_PROGRAM_logfiles(logdir):
    for i in os.listdir(logdir):
        if logfile_regex.match(i) is not None:
            yield i


def createdirs(currentdir, runcard, rtag):
    targetdir = os.path.join(currentdir, get_output_dir_name(runcard, rtag))
    mkdir(targetdir)
    logdir = os.path.join(targetdir, 'log')
    mkdir(logdir)
    nodedir = os.path.join(logdir, 'node_info')
    mkdir(nodedir)
    copylogdir = os.path.join(logdir, 'copylog')
    mkdir(copylogdir)
    logcheck = set([logseed_regex.search(i).group(1) for i
                    in get_PROGRAM_logfiles(logdir)])
    return logcheck, targetdir


def pullrun(name, seed, run, tmpdir, subfolder, attempts=0):

    if attempts == 0:  # Otherwise already in the tmp dir
        os.chdir(tmpdir)

    if verbose and attempts == 0:
        print("Pulling {0}, seed {1}".format(run, seed))
    elif verbose:
        print("Retrying {0}, seed {1}. Attempt {2}".format(
            run, seed, attempts+1))

    if subfolder is not None:
        __folder = os.path.join(config.grid_output_dir, subfolder)
    else:
        __folder = config.grid_output_dir
    gridname = os.path.join(config.gfaldir, __folder, name)
    gridname = ":".join([copy_protocol, gridname.split(":", 1)[-1]])

    command = 'gfal-copy {0} {1} {2} > /dev/null 2>&1'.format(
        gridname, name, timeoutstr)
    os.system(command)

    corrupted = True
    try:
        with tarfile.open(name, 'r|gz') as tfile:
            for t in tfile:
                if t.name.endswith(".dat"):
                    tfile.extract(t, path="../")
                    corrupted = False
                elif t.name.endswith(".log") and "node_info" not in t.name and \
                        "copies." not in t.name:
                    tfile.extract(t, "../log/")
                    corrupted = False
                elif t.name.endswith(".log") and "copies." in t.name:
                    tfile.extract(t, "../log/copylog/")
                    os.rename(f"../log/copylog/{t.name}",
                              f"../log/copylog/copies_{seed}.log")
                elif t.name.endswith(".log") and "node_info" in t.name:
                    tfile.extract(t, "../log/node_info/")
                    os.rename("../log/node_info/"+t.name,
                              "../log/node_info/node_info_{0}.log".format(seed))
        os.remove(name)
    except (FileNotFoundError, tarfile.ReadError):
        # pull error - corrupted stays True
        pass

    if corrupted:
        if attempts < MAX_ATTEMPTS-1:
            return pullrun(name, seed, run, tmpdir, subfolder,
                           attempts=attempts+1)
        if DELETE_CORRUPTED:
            # Hits if seed not found in any of the output files
            if verbose:
                print(f"\033[91mDeleting {run}, seed {seed}. "
                      f"Corrupted output\033[0m")
            os.system("gfal-rm {0}".format(gridname))
            return 1
        return 1
    return 0


def pull_seed_data(seed, runcard, targetdir, runcardname, subfolder):
    tarname = "{0}{1}.tar.gz".format(runcard, seed)
    tmpdir = os.path.join(targetdir, "log")
    return pullrun(tarname, seed, runcardname, tmpdir, subfolder)


def print_no_files_found(no_files):
    if no_files > 0:
        colour = "\033[92m"
    else:
        colour = "\033[93m"
    print("{1}{0:>5} new output file(s)\033[0m".format(no_files, colour))


def print_run_stats(no_files_found, corrupt_no):
    success_no = no_files_found-corrupt_no
    if success_no > 0:
        suc_col = "\033[92m"
    else:
        suc_col = ""
    if corrupt_no > 0:
        cor_col = "\033[91m"
    else:
        cor_col = ""
    print(
        "    {2}Successful: {0:<5}\033[0m  {3}Corrupted: {1:<5}\033[0m".format(
            success_no, corrupt_no, suc_col, cor_col))


def print_final_stats(start_time, tot_no_new_files, corrupt_no):
    end_time = datetime.datetime.now()
    total_time = (end_time-start_time).__str__().split(".")[0]
    print("\033[92m{0:^80}\033[0m".format("Finalisation finished!"))
    print("Total time: {0} ".format(total_time))
    print("New files found: {0}".format(tot_no_new_files))
    print("Corrupted files: {0}".format(corrupt_no))
    print("Finish time: {0}".format(end_time.strftime('%H:%M:%S')))


def pull_folder(foldername, folders=None, pool=None, rtag=None):
    if folders is None:
        folders = []
    print("\033[94mPulling Folder: {0} \033[0m".format(foldername))
    start_time = datetime.datetime.now()

    dirname = os.path.join(config.gfaldir, foldername)
    dirname = ":".join([ls_protocol, dirname.split(":", 1)[-1]])

    cmd = ['gfal-ls', dirname, "-l", "-t", "9999999"]
    cmd_output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[
        0].decode("utf-8")
    output = []
    for x in str(cmd_output).split("\n"):
        line = x.split()
        if len(line) > 0:
            output.append(line)
    currentdir = os.getcwd()

    output_files = set([x[-1] for x in output if x[0][0] != "d"])
    output_folders = set([x[-1] for x in output if x[0][0] == "d"])

    tot_no_new_files = 0
    tot_no_corrupted_files = 0
    use_list = []
    for runcard in rc.dictCard:
        if type(rc.dictCard[runcard]) == str:
            use_list.append((runcard, rc.dictCard[runcard]))
        if type(rc.dictCard[runcard]) == list:
            for entry in rc.dictCard[runcard]:
                use_list.append((runcard, entry))

    if rtag is not None:
        use_list = [i for i in use_list if i[1] == rtag]

    tot_rc_no = len(use_list)
    print("Finalisation setup complete. Preparing to pull data.")

    for rc_no, (runcard, tag) in enumerate(use_list):
        printstr = "> {0}-{1} ".format(runcard, tag)
        counter = "[{0}/{1}]".format(rc_no+1, tot_rc_no)
        print("{0:<60}{1:<7}".format(printstr, counter), end="")

        dirtag = runcard + "-" + tag
        runcard_name_no_seed = "output{0}-".format(dirtag)

        output_file_names, lfn_seeds = [], []
        for i in output_files:
            if runcard_name_no_seed in i:
                lfn_seeds.append(tarfile_regex.search(i).group(1))
                output_file_names.append(i)

        # Shortcircuit logfile check if nothing in lfn
        if not output_file_names:
            print_no_files_found(0)
            continue

        # Makes the second runcard slightly quicker by removing matched files :)
        output_files = output_files.difference(set(output_file_names))

        logseeds, targetdir = createdirs(currentdir, dirtag, rtag)
        pull_seeds = set(lfn_seeds).difference(logseeds)

        no_files_found = len(pull_seeds)
        print_no_files_found(no_files_found)

        if no_files_found > 0:
            tot_no_new_files += no_files_found
            results = pool.starmap(pull_seed_data,
                                   zip(pull_seeds,
                                       it.repeat(runcard_name_no_seed),
                                       it.repeat(targetdir),
                                       it.repeat(runcard),
                                       it.repeat(rtag)),
                                   chunksize=1)
            corrupt_no = sum(results)
            tot_no_corrupted_files += corrupt_no
            print_run_stats(no_files_found, corrupt_no)

    print_final_stats(start_time, tot_no_new_files, tot_no_corrupted_files)
    if RECURSIVE:
        for output_folder in output_folders:
            if any([tag == output_folder for tag in folders]):
                pull_folder(os.path.join(foldername, output_folder),
                            pool=pool, rtag=output_folder)


def do_finalise(*args, **kwargs):
    tags = set()
    for i in rc.dictCard:
        tags.add(rc.dictCard[i])
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)

    pool = mp.Pool(processes=no_processes)

    pull_folder(config.grid_output_dir, folders=tags, pool=pool)


if __name__ == "__main__":
    do_finalise()
