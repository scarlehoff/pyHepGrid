#!/usr/bin/env python

import header
import proxyUtil
import initialise
import runArcJob
import runDiracJob

from argparse import ArgumentParser
from sys import exit
from glob import glob

parser = ArgumentParser()

parser.add_argument("mode", help = "Mode running [initialize/run/manage/proxy] + runcard or [list/")
parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

parser.add_argument("-L", "--updateLibraries", help = "When running initialize mode: upgrades local.tar.gz", action = "store_true")

# ARC Options
parser.add_argument("-A", "--runArc", help = "run/manage an Arc job (warmup)", action = "store_true")
parser.add_argument("-g", "--getData", help = "getdata from an ARC job", action = "store_true")
parser.add_argument("-k", "--killJob", help = "kill a given job", action = "store_true")
parser.add_argument("-i", "--info", help = "retrieve arcstat for a given job", action = "store_true")
parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
parser.add_argument("-j", "--idjob", help = "id of the job to act upon")

# DIRAC Option
parser.add_argument("-D", "--runDirac", help = "Run a dirac job (production)", action = "store_true")

#### for debugging
parser.add_argument("-n", "--noProxy", help = "Bypasses proxy creation", action = "store_true")

args = parser.parse_args()

rcard = args.runcard
rmode = args.mode
# Various checks
if len(rmode) < 3:
    print("Mode not valid")
    exit(1)
if rmode[:3] == "run" or rmode[:3] == "man":
    if args.runDirac and args.runArc:
        print("Please, choose only Dirac (-D) or Arc (-A)")
        exit(1)
    elif not args.runDirac and not args.runArc:
        print("Please, choose either Dirac (-D) or Arc (-A)")
        exit(1)
###### 


####### MANAGEMENT WRAPPER
def importMe(isDirac):
    if isDirac:
        import runDiracJob as rt
    else:
        import runArcJob as rt
    return rt

if args.runDirac:
    print("Sourcing dirac...")
    cmd = ["bash", "-c", "source $sourcedirac && env"]
    import os, subprocess
    out = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    for line in out.stdout:
        if len(line) < 3: continue
        (key, _, value) = line.partition("=")
        value = value.rstrip()
        os.environ[key] = value


# Initialise: Send stuff to the Grid Storage
if rmode[:3] == "ini":
    print("Running initialize")
    # Right now, only ARC allowed
    if args.updateLibraries:
        initialise.updateLibraries()
        exit(0)
    elif args.runDirac:
        print("You might need to source dirac srcfile...")
        raw_input("Trying to do so on your behalf... press key to continue")
        if not args.noProxy: proxyUtil.diracProxy()
        initialise.initialiseNNLOJET(rcard, "production")
    else:
        if not args.noProxy: proxyUtil.arcProxyWiz()
        initialise.initialiseNNLOJET(rcard)
# Run: run an ARC or DIRAC job for the given runcard
elif rmode[:3] == "run":
    if args.runArc:
        if not args.noProxy: proxyUtil.arcProxyWiz()
        runArcJob.runArc(rcard)
    if args.runDirac: 
        if not args.noProxy: proxyUtil.diracProxy()
        runDiracJob.runDirac(rcard)
# Management
elif rmode[:3] == "man":
    backend = importMe(args.runDirac)
    backend.listRuns()
    if args.printme and args.idjob:
        id = args.idjob
    else:
        id = raw_input("> Select id to act upon: ")
    jobid = backend.getId(id) # A string for ARC, a list for DIRAC
    if args.getData:
        backend.getData(id)
    elif args.killJob:
        print("Killing the job")
        backend.killJob(jobid)
        backend.desactivateJob(id)
    elif args.info:
        print("Retrieving information . . . ")
        backend.statusJob(jobid)
    elif args.printme:
        print("Printing information . . .")
        backend.catJob(jobid)
    elif args.clean:
        print("Cleaning given job...")
        try:
            runArcJob.cleanJob(jobid)
            runArcJob.desactivateJob(id)
        except:
            raise Exception("Could not clean job")
    else:
        print("Printing jobid field")
        print(jobid)
elif rmode[:3] == "lis": 
    print("Listing runcards at %s" % header.runcardDir)
    for i in glob(header.runcardDir + "/*.run"):
        rc = i.split("/")[-1]
        print(" > %s " % rc)
    exit(0)
# PROXY
elif rmode[:3] == "pro":
    print("Creating ARC proxy")
    proxyUtil.arcProxyWiz()
else:
    print("ERROR")
    print("Mode: " + rmode + " not recognised")
    exit(1)


