#!/usr/bin/env python3
from __future__ import division, print_function
import os
import sys
import subprocess
import glob
import shutil
import multiprocessing as mp
import itertools as it
import importlib
import src.header as config
import tarfile

rc = importlib.import_module(config.finalise_runcards.replace("/","."))

# TODO
# Remove as many mkdir/rmtree calls as possible. 
# These take a lot of time/system resources, and probably 
# can be removed by keeping better track of tmp files

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
    logcheck = glob.glob('{0}/*.log'.format(logdir))
    return logcheck, targetdir, tmpdir


def seed_present(logcheck, seedstr):
    return any(seedstr in logfile for logfile in logcheck)


def pullrun(name, seed, run, output, logcheck, tmpdir):
    seedstr = ".s{0}.log".format(seed)
    if name in output and not seed_present(logcheck, seedstr):
        os.chdir(tmpdir)
        status = 0
        print("Pulling {0}, seed {1}".format(run, seed))
        command = 'lcg-cp lfn:output/{0} {0} 2>/dev/null'.format(name)
        os.system(command)
        # Use python tar extraction here
        corrupted = True
        with tarfile.open(name, 'r|gz') as tfile:
            for t in tfile:
                if t.name.endswith(".dat"):
                    tfile.extract(t,path="../")
                    corrupted = False
                elif t.name.endswith(".log"):
                    tfile.extract(t,"../log/")
                    corrupted = False
        if corrupted:
            status = 1
            # Hits if seed not found in any of the output files
            print("Deleting {0}, seed {1}. Corrupted output".format(run, seed))
            os.system('lcg-del -a lfn:output/{0}'.format(name))
        

def pull_seed_data(rc_tar, runcard, output, logcheck, targetdir):
    tmpdir = os.path.join(targetdir, ".tmp")
    seed = rc_tar.split(".")[-3].split("-")[-1]
    pullrun(rc_tar, seed, runcard, output, logcheck, tmpdir)


def do_finalise():
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    cmd = ['lfc-ls', config.lfn_output_dir]
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    currentdir = os.getcwd()
    output = set([x for x in str(output).split("\\n")])

    pool = mp.Pool(processes=no_processes)
    tot_rc_no = len(rc.dictCard)
    for rc_no, runcard in enumerate(rc.dictCard):
        print("> Checking output for {0} [{1}/{2}]".format(runcard, rc_no+1, tot_rc_no))
        dirtag = runcard + "-" + rc.dictCard[runcard]
        runcard_name_no_seed = "output{0}-".format(dirtag)
        runcard_output = [i for i in output if runcard_name_no_seed in i]
        logcheck, targetdir, tmpdir = createdirs(currentdir, dirtag)
        pool.starmap(pull_seed_data, zip(runcard_output, it.repeat(runcard), 
                                         it.repeat(output), it.repeat(logcheck),
                                         it.repeat(targetdir)))
        shutil.rmtree(tmpdir)

if __name__ == "__main__":
    do_finalise()
