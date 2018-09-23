#!/usr/bin/env python3
from __future__ import division, print_function
import datetime
import glob
import importlib
import itertools as it
import multiprocessing as mp
import os
import re
import src.header as config
import subprocess
import tarfile

rc = importlib.import_module(config.finalise_runcards.replace("/","."))
logseed_regex = re.compile(r".s([0-9]+)\.[^\.]+$") # Matches seeds in logfiles
tarfile_regex = re.compile(r"-([0-9]+)\.tar.gz+$") # Matches tarfiles
logfile_regex = re.compile(r"\w+\.\w+\.s([0-9]+)\.log") # Matches NNLOJET log files

# CONFIG
no_processes = config.finalise_no_cores
verbose = config.verbose_finalise

# Set up environment
os.environ["LFC_HOST"] = config.LFC_HOST
os.environ["LCG_CATALOG_TYPE"] = config.LFC_CATALOG_TYPE
os.environ["LFC_HOME"] = config.lfndir

if config.timeout is not None:
    timeoutstr = "--sendreceive-timeout {0}".format(config.timeout)
else:
    timeoutstr = ""

def mkdir(directory):
    os.system('mkdir {0} > /dev/null 2>&1'.format(directory))


def get_output_dir_name(runcard):
    basedir = config.production_base_dir
    subdir = "{0}{1}".format(config.finalise_prefix, runcard)
    return os.path.join(basedir, subdir)


def get_NNLOJET_logfiles(logdir):
    for i in os.listdir(logdir):
        if logfile_regex.match(i) is not None:
            yield i


def createdirs(currentdir, runcard):
    targetdir = os.path.join(currentdir, get_output_dir_name(runcard))
    mkdir(targetdir)
    logdir = os.path.join(targetdir, 'log')
    mkdir(logdir)
    logcheck = set([logseed_regex.search(i).group(1) for i 
                    in get_NNLOJET_logfiles(logdir)])
    return logcheck, targetdir


def pullrun(name, seed, run, tmpdir):
    seedstr = ".s{0}.log".format(seed)
    os.chdir(tmpdir)
    status = 0
    if verbose:
        print("Pulling {0}, seed {1}".format(run, seed))
    command = 'lcg-cp lfn:output/{0} {0} 2>/dev/null {1}'.format(name, timeoutstr)
    os.system(command)

    corrupted = True
    try:
        with tarfile.open(name, 'r|gz') as tfile:
            for t in tfile:
                if t.name.endswith(".dat"):
                    tfile.extract(t,path="../")
                    corrupted = False
                elif t.name.endswith(".log") and "node_info" not in t.name:
                    tfile.extract(t,"../log/")
                    corrupted = False
                elif t.name.endswith(".log") and "node_info"in t.name:
                    tfile.extract(t,"../log/node_info_{0}.log".format(seed))
        os.remove(name)
    except (FileNotFoundError, tarfile.ReadError) as e:
        # pull error - corrupted stays True
        pass

    if corrupted:
        # Hits if seed not found in any of the output files
        if verbose:
            print("\033[91mDeleting {0}, seed {1}. Corrupted output\033[0m".format(run, seed))
        os.system('lcg-del -a lfn:output/{0} 2>/dev/null'.format(name))
        os.system('lfc-rm output/{0} -f 2>/dev/null'.format(name))
        os.system('lfc-rm output/{0} -a -f 2>/dev/null'.format(name))
        return 1
    return 0


def pull_seed_data(seed, runcard, targetdir, runcardname):
    tarname = "{0}{1}.tar.gz".format(runcard,seed)
    tmpdir = os.path.join(targetdir, "log")
    return pullrun(tarname, seed, runcardname, tmpdir)


def print_no_files_found(no_files):
    if no_files>0:
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
    print("    {2}Successful: {0:<5}\033[0m  {3}Corrupted: {1:<5}\033[0m".format(success_no,corrupt_no,suc_col,cor_col))


def print_final_stats(start_time, tot_no_new_files, corrupt_no):
    end_time = datetime.datetime.now()
    total_time = (end_time-start_time).__str__().split(".")[0]
    print("\033[92m{0:^80}\033[0m".format("Finalisation finished!"))
    print("Total time: {0} ".format(total_time))
    print("New files found: {0}".format(tot_no_new_files)) 
    print("Corrupted files: {0}".format(corrupt_no)) 
    print("Finish time: {0}".format(end_time.strftime('%H:%M:%S')))


def do_finalise():
    start_time = datetime.datetime.now()

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    cmd = ['lfc-ls', config.lfn_output_dir]
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    currentdir = os.getcwd()
    
    output = set([x for x in str(output).split("\\n")])

    pool = mp.Pool(processes=no_processes)

    tot_no_new_files = 0
    tot_no_corrupted_files = 0
    use_list = []
    for runcard in rc.dictCard:
        if type(rc.dictCard[runcard]) == str:
            use_list.append((runcard,rc.dictCard[runcard]))
        if type(rc.dictCard[runcard]) == list:
            for entry in rc.dictCard[runcard]:
                use_list.append((runcard,entry))
    tot_rc_no = len(use_list)
    print("Finalisation setup complete. Preparing to pull data.")

    for rc_no, (runcard, tag) in enumerate(use_list):
        #printstr = "> {0}-{1} ".format(runcard, tag)
        printstr = "> {0} ".format(runcard, tag)
        counter = "[{0}/{1}]".format(rc_no+1,tot_rc_no)
        print("{0:<60}{1:<7}".format(printstr, counter), end="")

        dirtag = runcard + "-" + tag
        runcard_name_no_seed = "output{0}-".format(dirtag)

        output_file_names,lfn_seeds = [],[]
        for i in output: 
            if runcard_name_no_seed in i:
                lfn_seeds.append(tarfile_regex.search(i).group(1))
                output_file_names.append(i)

        if not output_file_names: # Shortcircuit logfile check if nothing in lfn
            print_no_files_found(0)
            continue

        # Makes the second runcard slightly quicker by removing matched files :)
        output = output.difference(set(output_file_names)) 

        logseeds, targetdir = createdirs(currentdir, dirtag)
        pull_seeds = set(lfn_seeds).difference(logseeds)

        no_files_found = len(pull_seeds)
        print_no_files_found(no_files_found)

        if no_files_found>0:
            tot_no_new_files += no_files_found
            results = pool.starmap(pull_seed_data, zip(pull_seeds, 
                                             it.repeat(runcard_name_no_seed),
                                             it.repeat(targetdir),
                                             it.repeat(runcard)),
                                   chunksize=1)
            corrupt_no = sum(results)
            tot_no_corrupted_files += corrupt_no
            print_run_stats(no_files_found, corrupt_no)

    print_final_stats(start_time,tot_no_new_files,tot_no_corrupted_files)

if __name__ == "__main__":
    do_finalise()
