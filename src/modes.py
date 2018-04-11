from src.header import logger


def do_management(args,rcard):
#### Management of running/finished jobs
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


def do_test(args,rcard):
#### Test an initialised runcard
    from src.test_nnlojob import run_test
    run_test(args, rcard)


def do_initialise(args,rcard):
#### Initialisation: send stuff to Grid Storage
    mode_Warmup = args.runArc
    mode_Production = (args.runDirac or args.runArcProduction)
    from src.Backend import generic_initialise
    if mode_Warmup:
        generic_initialise(rcard, warmup=True, grid=args.provWarm, overwrite_grid=args.continue_warmup)
    elif mode_Production:
        generic_initialise(rcard, production=True, grid=args.provWarm)
    else:
        logger.critical("Choose what do you want to initialise -(A/B/D/L)")


def do_run(args,rcard):
#### Run: run an ARC or DIRAC job for the given runcard
    if args.runArc:
        from src.runArcjob import runWrapper
    elif args.runArcProduction:
        from src.runArcjob import runWrapperProduction as runWrapper
    elif args.runDirac:
        from src.runDiracjob import runWrapper
    else:
        raise logger.critical("Choose what do you want to run -(A/B/D)")
    runWrapper(rcard, test=args.test)


def do_proxy(args,rcard):
#### Proxy management
    import src.proxyUtil as proxyUtil
    if args.runArc or args.runArcProduction:   proxyUtil.arcProxyWiz()
    if args.runDirac: proxyUtil.diracProxy()
