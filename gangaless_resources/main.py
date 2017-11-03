#!/usr/bin/env python3.4

##### Call argument parser
from argument_parser import arguments as args
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

    from header import finalisation_script
    if args.get_data and finalisation_script:
        backend.get_data(0, custom_get = finalisation_script)
        exit(0)

    if args.updateArc:
        if not args.runArc: 
            raise Exception("Update ARC can only be used with ARC")
        backend.update_stdout()
        exit(0)
    if args.idjob:
        id_str = args.idjob
    else:
        backend.list_runs(args.find)
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
        jobid = backend.get_id(db_id) # a list
        # Options that keep the database entry after they are done
        if args.stats:
            backend.stats_job(jobid)
        elif args.statsCheat:
            date = backend.get_date(db_id)
            backend.stats_job_cheat(jobid, date)
        elif args.info or args.infoVerbose:
            print("Retrieving information . . . ")
            backend.status_job(jobid, args.infoVerbose)
        elif args.renewArc:
            print("Renewing proxy for the job . . . ")
            backend.renew_proxy(jobid)
        elif args.printme:
            print("Printing information . . . ")
            backend.cat_job(jobid)
        elif args.printmelog:
            print("Printing information . . . ")
            backend.cat_log_job(jobid)

        # Options that deactivate the database entry once they're done
        elif args.get_data:
            print("Retrieving job data")
            backend.get_data(db_id)
            backend.disable_db_entry(db_id)
        elif args.kill_job:
            print("Killing the job")
            backend.kill_job(jobid)
            backend.disable_db_entry(db_id)
        elif args.clean:
            print("Cleaning job . . . ")
            backend.clean_job(jobid)
            backend.disable_db_entry(db_id)
        # Enable back any database entry
        elif args.enableme:
            backend.enable_db_entry(db_id)
        else:
            print(jobid)

