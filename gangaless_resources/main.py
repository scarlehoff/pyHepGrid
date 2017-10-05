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

parser.add_argument("mode", help = "Mode [initialize/run/manage/proxy] ")
parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

# Backend selection
parser.add_argument("-A", "--runArc",   help = "Run/manage an Arc job (warmup)", action = "store_true")
parser.add_argument("-D", "--runDirac", help = "Run/manage a dirac job (production)", action = "store_true")

# Initialisation options
parser.add_argument("-L", "--lhapdf",    help = "Send LHAPDF to Grid", action = "store_true")

parser.add_argument("-n", "--noProxy", help = "Bypasses proxy creation", action = "store_true")

# Global management
parser.add_argument("-g", "--getData", help = "getdata from an ARC job", action = "store_true")
parser.add_argument("-k", "--killJob", help = "kill a given job", action = "store_true")
parser.add_argument("-i", "--info", help = "retrieve arcstat/diracstat for a given job", action = "store_true")
parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
parser.add_argument("-j", "--idjob", help = "id of the job to act upon")
# Arc only
parser.add_argument("-u", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
parser.add_argument("-r", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
parser.add_argument("-w", "--provWarm", help = "Provide warmup files for an DIRAC run (only with ini)")
parser.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
parser.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes)", action = "store_true")

# Dirac Only
parser.add_argument("-s", "--stats", help = "output statistics for all subjobs in a dirac job", action = "store_true")

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
    elif args.runDirac:
        from runDiracjob import iniWrapper
    elif args.lhapdf:
        from utilities import lhapdfIni
        lhapdfIni()
        exit(0)
    else:
        raise Exception("Choose what do you want to initialise -(A/D/L)")
    if args.provWarm:
        iniWrapper(rcard, args.provWarm)
    else:
        iniWrapper(rcard)
        
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
        if not args.runArc: 
            raise Exception("Update ARC can only be used with ARC")
        backend.updateStdOut()
        exit(0)
    if args.idjob:
        id_str = args.idjob
    else:
        backend.listRuns()
        id_str = py_input("> Select id to act upon: ")
    if "-" in id_str:
        id_limits = id_str.split("-")
        id_list = range(int(id_limits[0]), int(id_limits[1])+1)
    else:
        id_list = [id_str]
    
    for id_int in id_list:
        db_id = str(id_int)
        jobid = backend.getId(db_id) # A string for ARC, a string (list = string.split(" ")) for Dirac
        # Options that keep the database entry
        if args.stats:
            if not args.runDirac:
                raise Exception("Statistics currently only implemented for Dirac")
            backend.statsJob(jobid)
        elif args.info:
            print("Retrieving information . . . ")
            backend.statusJob(jobid)
        elif args.renewArc:
            print("Renewing proxy for the job . . . ")
            backend.renewProxy(jobid)
        elif args.printme:
            print("Printing information . . . ")
            backend.catJob(jobid)
        # Options that deactivate the database entry once they're done
        elif args.getData:
            print("Retrieving job data")
            backend.getData(db_id)
            backend.desactivateJob(db_id)
        elif args.killJob:
            print("Killing the job")
            backend.killJob(jobid)
            backend.desactivateJob(db_id)
        elif args.clean:
            print("Cleaning job . . . ")
            backend.cleanJob(jobid)
            backend.desactivateJob(db_id)
        # Enable back any database entry
        elif args.enableme:
            backend.reactivateJob(db_id)
        else:
            print(jobid)

