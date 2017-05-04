#!/usr/bin/env python

from argparse import ArgumentParser
from sys import exit, version_info

##### Compatibility
try:
    if version_info.major == 2: 
        py_input = raw_input
    else:
        py_input = input
except:
    # *sigh*
    py_input = raw_input
#############################

parser = ArgumentParser()

parser.add_argument("mode", help = "Mode running [initialize/run/manage/proxy] + runcard or [list/")
parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

# Backend selection
parser.add_argument("-A", "--runArc",   help = "run/manage an Arc job (warmup)", action = "store_true")
parser.add_argument("-D", "--runDirac", help = "Run a dirac job (production)", action = "store_true")

# Initialisation options
parser.add_argument("-L", "--lhapdf",    help = "Send LHAPDF to Grid", action = "store_true")

parser.add_argument("-n", "--noProxy", help = "Bypasses proxy creation", action = "store_true")

# Global management
parser.add_argument("-g", "--getData", help = "getdata from an ARC job", action = "store_true")
parser.add_argument("-k", "--killJob", help = "kill a given job", action = "store_true")
parser.add_argument("-i", "--info", help = "retrieve arcstat for a given job", action = "store_true")
parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
parser.add_argument("-j", "--idjob", help = "id of the job to act upon")
# Arc only
parser.add_argument("-u", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
parser.add_argument("-r", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
parser.add_argument("-w", "--provWarm", help = "Provide warmup files for an DIRAC run (only with ini)")
parser.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
parser.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes)", action = "store_true")

args  = parser.parse_args()

rcard = args.runcard
rmode = args.mode

##### Checks go here
if len(rmode) < 3:
    raise Exception("Mode ", rmode, " not valid")
if rmode[:3] == "run" or rmode[:3] == "man":
    if args.runDirac and args.runArc:
        raise Exception("Please, choose only Dirac (-D) or Arc (-A)")
    if not args.runDirac and not args.runArc:
        raise Exception("Please , choose either Dirac (-D) or Arc (-A)")
########################################

#### Step0, if Dirac, source dirac
if  args.runDirac:
    print("Sourcing dirac...")
    cmd = ["bash", "-c", "source $sourcedirac && env"]
    import os, subprocess
    out = subprocess.Popen(cmd, stdout = subprocess.PIPE)
    for line in out.stdout:
        if len(line) < 3: continue
        (key, _, value) = line.partition("=")
        value = value.rstrip()
        os.environ[key] = value
########################################

#### Step1, invoke proxy
if not args.noProxy:
    import proxyUtil
    if args.runArc:   proxyUtil.arcProxyWiz()
    if args.runDirac: proxyUtil.diracProxy()
    if rmode[:4] == "prox": exit(0)
########################################

#### Step2, generate database and tables
from header import arctable, diractable, dbname, dbfields
from dbapi  import database
db = database(dbname)
if not db.isThisTableHere(arctable):   db.createTable(arctable,   dbfields)
if not db.isThisTableHere(diractable): db.createTable(diractable, dbfields)
########################################

#### Step3, run command (initialisation, run or management)

#### Initialisation: send stuff to Grid Storage
if rmode[:3] == "ini":
    if args.runArc:
        from runArcjob import iniWrapper
        iniWrapper(rcard)
    elif args.runDirac:
        from runDiracjob import iniWrapper
        if args.provWarm:
            iniWrapper(rcard, args.provWarm)
        else:
            iniWrapper(rcard)
    elif args.lhapdf:
        from utilities import lhapdfIni
        lhapdfIni()
    else:
        raise Exception("Choose what do you want to initialise -(A/D/L)")
        
#### Run: run an ARC or DIRAC job for the given runcard
elif rmode[:3] == "run":
    if args.runArc:
        from runArcjob import runWrapper
    if args.runDirac:
        from runDiracjob import runWrapper
    if args.test and args.runArc:
        runWrapper(rcard, True)
    else:
        runWrapper(rcard)
#### Management: 
elif rmode[:3] == "man":
    if args.runArc:
        from backendManagement import Arc as backend_class
    if args.runDirac: 
        from backendManagement import Dirac as backend_class
    backend = backend_class()
    if args.updateArc:
        if not args.runArc: raise Exception("Update ARC can only be used with ARC")
        backend.updateStdOut()
        exit(0)
    if args.idjob:
        id = args.idjob
    else:
        backend.listRuns()
        id = py_input("> Select id to act upon: ")
    jobid = backend.getId(id) # A string for ARC, a string (list = string.split(" ")) for Dirac
    #
    if args.getData:
        print("Retrieving job data")
        backend.getData(id)
        backend.desactivateJob(id)
    elif args.killJob:
        print("Killing the job")
        backend.killJob(jobid)
        backend.desactivateJob(id)
    elif args.info:
        print("Retrieving information . . . ")
        backend.statusJob(jobid)
    elif args.renewArc:
        print("Renewing proxy for the job . . . ")
        backend.renewProxy(jobid)
    elif args.printme:
        print("Printing information . . . ")
        backend.catJob(jobid)
    elif args.clean:
        print("Cleaning job . . . ")
        backend.cleanJob(jobid)
        backend.desactivateJob(id)
    elif args.enableme:
        backend.reactivateJob(id)
    else:
        print(jobid)

