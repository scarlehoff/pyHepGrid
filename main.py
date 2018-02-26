#!/usr/bin/env python3.4

##### Call argument parser
from src.argument_parser import arguments as args
rcard = args.runcard
rmode = args.mode

from src.header import arctable, diractable, dbname, dbfields
from src.dbapi  import database

##### Checks go here
if len(rmode) < 3:
    raise Exception("Mode ", rmode, " not valid")
if rmode[:3] == "run" or rmode[:3] == "man":
    if args.runDirac and args.runArc:
        if not args.idjob == "all":
            raise Exception("Please choose only Dirac (-D) or Arc (-A) or Arc Production Mode (-B) (unless using -j all)")
    if not args.runDirac and not args.runArc and not args.runArcProduction:
        raise Exception("Please choose either Dirac (-D) or Arc (-A) or Arc Production Mode (-B)")

########################################

#### Step1, invoke proxy
if args.genProxy:
    import src.proxyUtil as proxyUtil
    if args.runArc or args.runArcProduction:   proxyUtil.arcProxyWiz()
    if args.runDirac: proxyUtil.diracProxy()
    if rmode[:4] == "prox": exit(0)
########################################

#### Step2, generate database and tables
db = database(dbname, tables = [arctable, diractable], fields = dbfields)
########################################

#### Step3, run command (initialisation, run or management)

#### Initialisation: send stuff to Grid Storage
if rmode[:3] == "ini":
    mode_Warmup = args.runArc
    mode_Production = (args.runDirac or args.runArcProduction)

    from src.Backend import generic_initialise

    if mode_Warmup:
        generic_initialise(rcard, warmup=True, grid=args.provWarm, overwrite_grid=args.continue_warmup)
    elif mode_Production:
        generic_initialise(rcard, production=True, grid=args.provWarm)
    else:
        raise Exception("Choose what do you want to initialise -(A/B/D/L)")

        
#### Run: run an ARC or DIRAC job for the given runcard
elif rmode[:3] == "run":
    if args.runArc:
        from src.runArcjob import runWrapper
    elif args.runArcProduction:
        from src.runArcjob import runWrapperProduction as runWrapper
    elif args.runDirac:
        from src.runDiracjob import runWrapper
    else:
        raise Exception("Choose what do you want to run -(A/B/D)")
    runWrapper(rcard, args.test)


#### Management: 
elif rmode[:3] == "man":
    import src.main_routines as mr
    backends = []
    if (args.runArc or args.runArcProduction) and args.runDirac:
        from src.backendManagement import Arc as backend_Arc
        backends.append(backend_Arc(act_only_on_done = args.done))
        from src.backendManagement import Dirac as backend_Dirac
        backends.append(backend_Dirac(act_only_on_done = args.done))
    else:
        if args.runArc or args.runArcProduction:
            from src.backendManagement import Arc as backend_class
        if args.runDirac: 
            from src.backendManagement import Dirac as backend_class
        backends.append(backend_class(act_only_on_done = args.done))

    for backend in backends:
        mr.management_routine(backend, args)

elif rmode[:3] == "tes":
    from src.test_nnlojob import run_test
    run_test(args, rcard)

    
else:
    print("Invalid mode selected. Exiting")
