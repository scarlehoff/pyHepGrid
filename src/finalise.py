#!/usr/bin/env python3
from __future__ import division, print_function
import datetime
import glob
import importlib
import itertools as it
import multiprocessing as mp
import os
import re
import shutil
import src.header as config
import subprocess
import sys
import tarfile

rc = importlib.import_module(config.finalise_runcards.replace("/","."))
logseed_regex = re.compile(r".s([0-9]+)\.[^\.]+$")
tarfile_regex = re.compile(r"-([0-9]+)\.tar.gz+$")

# CONFIG
no_processes = config.finalise_no_cores

# Set up environment
os.environ["LFC_HOST"] = config.LFC_HOST
os.environ["LCG_CATALOG_TYPE"] = config.LFC_CATALOG_TYPE
os.environ["LFC_HOME"] = config.lfndir

def mkdir(directory):
    os.system('mkdir {0} > /dev/null 2>&1'.format(directory))


def get_output_dir_name(runcard):
    basedir = config.production_base_dir
    subdir = "{0}{1}".format(config.finalise_prefix, runcard)
    return os.path.join(basedir, subdir)


def createdirs(currentdir, runcard):
    targetdir = os.path.join(currentdir, get_output_dir_name(runcard))
    mkdir(targetdir)
    logdir = os.path.join(targetdir, 'log')
    mkdir(logdir)
    tmpdir = os.path.join(targetdir, '.tmp')
    mkdir(tmpdir)
    logcheck = set([logseed_regex.search(i).group(1) for i 
                    in glob.glob('{0}/*.log'.format(logdir))])
    return logcheck, targetdir, tmpdir


def seed_present(logcheck, seedstr):
    return any(seedstr in logfile for logfile in logcheck)


def pullrun(name, seed, run, tmpdir):
    seedstr = ".s{0}.log".format(seed)
    os.chdir(tmpdir)
    status = 0
    print("Pulling {0}, seed {1}".format(run, seed))
    command = 'lcg-cp lfn:output/{0} {0} 2>/dev/null'.format(name)
    os.system(command)

    corrupted = True
    try:
        with tarfile.open(name, 'r|gz') as tfile:
            for t in tfile:
                if t.name.endswith(".dat"):
                    tfile.extract(t,path="../")
                    corrupted = False
                elif t.name.endswith(".log"):
                    tfile.extract(t,"../log/")
                    corrupted = False
    except FileNotFoundError as e:
        # pull error - corrupted stays True
        pass
    if corrupted:
        status = 1
        # Hits if seed not found in any of the output files
        print("Deleting {0}, seed {1}. Corrupted output".format(run, seed))
        os.system('lcg-del -a lfn:output/{0} 2>/dev/null'.format(name))
        os.system('lfc-rm output/{0} -f 2>/dev/null'.format(name))
        

def pull_seed_data(seed, runcard, targetdir, runcardname):
    tarname = "{0}{1}.tar.gz".format(runcard,seed)
    tmpdir = os.path.join(targetdir, ".tmp")
    pullrun(tarname, seed, runcardname, tmpdir)


def print_no_files_found(no_files):
    if no_files>0:
        colour = "\033[92m"
    else:
        colour = "\033[93m"
    print("{1}{0:>5} new output file(s)\033[0m".format(no_files, colour))


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
    tot_rc_no = len(rc.dictCard)

    tot_no_new_files = 0
    for rc_no, runcard in enumerate(rc.dictCard):
        printstr = "> {0} [{1}/{2}]".format(runcard, rc_no+1, tot_rc_no)
        print("{0:<60}".format(printstr), end="")

        dirtag = runcard + "-" + rc.dictCard[runcard]
        runcard_name_no_seed = "output{0}-".format(dirtag)
        
        output_file_names,lfn_seeds = [],[]
        for i in output: 
            if i in runcard_name_no_seed:
                lfn_seeds.append(tarfile_regex.search(i).group(1))
                output_file_names.append(i)

        if not output_file_names: # Shortcircuit logfile check if nothing in lfn
            print_no_files_found(0)
            continue

        # Makes the second runcard slightly quicker by removing matched files:)
        output = output.difference(set(output_file_names)) 

        logseeds, targetdir, tmpdir = createdirs(currentdir, dirtag)
        pull_seeds = set(lfn_seeds).difference(logseeds)
        
        no_files_found = len(pull_seeds)
        print_no_files_found(no_files_found)
        tot_no_new_files += no_files_found

        if no_files_found>0:
            pool.starmap(pull_seed_data, zip(pull_seeds, 
                                             it.repeat(runcard_name_no_seed),
                                             it.repeat(targetdir),
                                             it.repeat(runcard)))
        shutil.rmtree(tmpdir)
    
    end_time = datetime.datetime.now()
    total_time = (end_time-start_time).__str__().split(".")[0]
    print("\033[92m{0:^80}\033[0m".format("Finalisation finished!"))
    print("Total time: {0} ".format(total_time))
    print("New files found: {0}".format(tot_no_new_files)) 

if __name__ == "__main__":
    do_finalise()
