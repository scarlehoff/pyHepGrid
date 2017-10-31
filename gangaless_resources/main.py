#!/usr/bin/env python3.4

from argparse import ArgumentParser
from sys import exit

# ##### Compatibility
# try:
#     if version_info.major == 2: 
#         input = raw_input
#     else:
#         input = input
# except:
#     # *sigh*
#     input = raw_input
#############################

parser = ArgumentParser()

parser.add_argument("mode", help = "Mode [initialize/run/manage/proxy] ")
parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

# Backend selection
parser.add_argument("-A", "--runArc",   help = "Run/manage an Arc job (warmup)", action = "store_true")
parser.add_argument("-B", "--runArcProduction",   help = "Run/manage an Arc job (production)", action = "store_true")
parser.add_argument("-D", "--runDirac", help = "Run/manage a dirac job (production)", action = "store_true")

# Initialisation options
parser.add_argument("-L", "--lhapdf",    help = "Send LHAPDF to Grid", action = "store_true")

parser.add_argument("-n", "--noProxy", help = "Bypasses proxy creation", action = "store_true")

# Global management
parser.add_argument("-g", "--getData", help = "getdata from an ARC job", action = "store_true")
parser.add_argument("-k", "--killJob", help = "kill a given job", action = "store_true")
parser.add_argument("-i", "--info", help = "retrieve arcstat/diracstat for a given job", action = "store_true")
parser.add_argument("-I", "--infoVerbose", help = "retrieve arcstat/diracstat for a given job (more verbose, only ARC)", action = "store_true")
parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
parser.add_argument("-P", "--printmelog", help = "do arccat to the *.log files of a given job (only ARC)", action = "store_true")
parser.add_argument("-j", "--idjob", help = "id of the job to act upon")
parser.add_argument("-w", "--provWarm", help = "Provide warmup files for an DIRAC run (only with ini)")
parser.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
parser.add_argument("-f", "--find", help = "Only database entries in which a certain string is found are shown")
# Warmup only
parser.add_argument("-u", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
parser.add_argument("-r", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
parser.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes)", action = "store_true")

# Production Only
parser.add_argument("-s", "--stats", help = "output statistics for all subjobs in a dirac job", action = "store_true")

args  = parser.parse_args()

rcard = args.runcard
rmode = args.mode

##### Checks go here
if len(rmode) < 3:
    raise Exception("Mode ", rmode, " not valid")
if rmode[:3] == "run" or rmode[:3] == "man":
    if args.runDirac and args.runArc:
        raise Exception("Please, choose only Dirac (-D) or Arc (-A) or Arc Production Mode (-B)")
    if not args.runDirac and not args.runArc and not args.runArcProduction:
        raise Exception("Please , choose either Dirac (-D) or Arc (-A) or Arc Production Mode (-B)")
########################################

# Disabled dirac sourcing, pending tests...

# #### Step0, if Dirac, source dirac
# if  args.runDirac:
#     print("Sourcing dirac...")
#     cmd = ["bash", "-c", "source $sourcedirac && env"]
#     import os, subprocess
#     out = subprocess.Popen(cmd, stdout = subprocess.PIPE)
#     for lineRaw in out.stdout:
#         if len(lineRaw) < 3:
#             continue
#         line = lineRaw.decode()
#         (key, _, value) = line.partition("=")
#         value = value.rstrip()
#         os.environ[key] = value
# ########################################

#### Step1, invoke proxy
if not args.noProxy:
    import proxyUtil
    if args.runArc or args.runArcProduction:   proxyUtil.arcProxyWiz()
    if args.runDirac: proxyUtil.diracProxy()
    if rmode[:4] == "prox": exit(0)
########################################

#### Step2, generate database and tables
from header import arctable, diractable, dbname, dbfields
from dbapi  import database
db = database(dbname, tables = [arctable, diractable], fields = dbfields)
########################################

#### Step3, run command (initialisation, run or management)

#### Initialisation: send stuff to Grid Storage
if rmode[:3] == "ini":
    if args.runArc:
        from runArcjob import iniWrapper
    elif args.runArcProduction:
        from runArcjob import iniWrapperProduction as iniWrapper
    elif args.runDirac:
        from runDiracjob import iniWrapper
    elif args.lhapdf:
        from utilities import lhapdfIni
        lhapdfIni()
        exit(0)
    else:
        raise Exception("Choose what do you want to initialise -(A/B/D/L)")
    if args.provWarm:
        iniWrapper(rcard, args.provWarm)
    else:
        iniWrapper(rcard)
        
#### Run: run an ARC or DIRAC job for the given runcard
elif rmode[:3] == "run":
    if args.runArc:
        from runArcjob import runWrapper
    elif args.runArcProduction:
        from runArcjob import runWrapperProduction as runWrapper
    elif args.runDirac:
        from runDiracjob import runWrapper
    else:
        raise Exception("Choose what do you want to run -(A/B/D/L)")
    runWrapper(rcard, args.test)
#### Management: 
elif rmode[:3] == "man":
    if args.runArc or args.runArcProduction:
        from backendManagement import Arc as backend_class
    if args.runDirac: 
        from backendManagement import Dirac as backend_class
    backend = backend_class()

    from header import finalise_runcards
    if args.getData and finalise_runcards:
        backend.getData(0)
        exit(0)

    if args.updateArc:
        if not args.runArc: 
            raise Exception("Update ARC can only be used with ARC")
        backend.updateStdOut()
        exit(0)
    if args.idjob:
        id_str = args.idjob
    else:
        backend.listRuns(args.find)
        id_str = input("> Select id to act upon: ")

    id_list_raw = str(id_str).split(",")
    id_list = []
    for id_selected in id_list_raw:
        if "-" in id_selected:
            id_limits = id_selected.split("-")
            for id_int in range(int(id_limits[0]), int(id_limits[1]) + 1):
                id_list.append(str(id_int))
        else:
            id_list.append(id_selected)

    for db_id in id_list:
        jobid = backend.getId(db_id) # A string for ARC, a string (list = string.split(" ")) for Dirac
        # Options that keep the database entry
        if args.stats:
            backend.statsJob(jobid)
        elif args.info or args.infoVerbose:
            print("Retrieving information . . . ")
            backend.statusJob(jobid, args.infoVerbose)
        elif args.renewArc:
            print("Renewing proxy for the job . . . ")
            backend.renewProxy(jobid)
        elif args.printme:
            print("Printing information . . . ")
            backend.catJob(jobid)
        elif args.printmelog:
            print("Printing information . . . ")
            backend.catLogJob(jobid)

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

