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

rc = importlib.import_module(config.finalise_runcards)

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
    newdir = os.path.join(targetdir, 'log')
    mkdir(newdir)

    logdir = os.path.join(targetdir, 'log')
    os.chdir(logdir)
    logcheck = glob.glob('*.log')

    return logcheck, targetdir


def seed_present(logcheck, seedstr):
    return any(seedstr in logfile for logfile in logcheck)


def move_logfile_to_log_dir(tmpfiles):
    logfile = next(x for x in tmpfiles if x.endswith(".log"))
    direct = os.path.join('../log/', logfile)
    os.rename(logfile, direct)


def move_dat_files_to_root_dir(tmpfiles):
    for f in tmpfiles:
        if f.endswith('.dat'):
            os.rename(f, '../' + f)


def pullrun(name, seed, run, output, logcheck, tmpdir):
    seedstr = ".s{0}.log".format(seed)
    if name in output and not seed_present(logcheck, seedstr):
        os.mkdir(tmpdir)
        os.chdir(tmpdir)
        status = 0
        print("Pulling {0}, seed {1}".format(run, seed))
        command = 'lcg-cp lfn:output/{0} {0} 2>/dev/null'.format(name)
        os.system(command)
        out = os.system('tar -xf ' + name + ' -C .')
        tmpfiles = os.listdir('.')
        if seed_present(tmpfiles, seedstr):
            move_logfile_to_log_dir(tmpfiles)
            move_dat_files_to_root_dir(tmpfiles)
        else:
            status = 1
            # Hits if seed not found in any of the output files
            print("Deleting {0}, seed {1}. Corrupted output".format(run, seed))
            os.system('lcg-del -a lfn:output/{0}'.format(name))
        shutil.rmtree(tmpdir)
        

def pull_seed_data(rc_tar, runcard, output, logcheck, targetdir):
    pid = mp.current_process().pid
    # Separate tmpdir for each process
    tmpdir = os.path.join(targetdir, '.process_{0}_tmp'.format(pid))
    seed = rc_tar.split(".")[-3].split("-")[-1]
    pullrun(rc_tar, seed, runcard, output, logcheck, tmpdir)


def do_finalise():
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    cmd = ['lfc-ls', 'output']
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
    currentdir = os.getcwd()
    output = [x for x in str(output).split("\\n")]

    pool = mp.Pool(processes=no_processes)
    tot_rc_no = len(rc.dictCard)
    for rc_no, runcard in enumerate(rc.dictCard):
        print("> Checking output for {0} [{1}/{2}]".format(runcard, rc_no+1, tot_rc_no))
        dirtag = runcard + "-" + rc.dictCard[runcard]
        runcard_name_no_seed = "output{0}-".format(dirtag)
        runcard_output = [i for i in output if runcard_name_no_seed in i]
        logcheck, targetdir = createdirs(currentdir, dirtag)
        pool.starmap(pull_seed_data, zip(runcard_output, it.repeat(runcard), 
                                         it.repeat(output), it.repeat(logcheck),
                                         it.repeat(targetdir)))


if __name__ == "__main__":
    do_finalise()
