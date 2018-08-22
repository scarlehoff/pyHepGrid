from src.header import logger


def do_management(args,rcard):
#### Management of running/finished jobs
    import src.main_routines as mr
    backends = []
    if args.runArc:
        from src.backendManagement import Arc as backend_Arc
        backends.append(backend_Arc(act_only_on_done = args.done))
    if args.runArcProduction:
        from src.backendManagement import Arc as backend_ArcProd
        backends.append(backend_ArcProd(act_only_on_done = args.done, 
                                        production=True))
    if args.runDirac:
        from src.backendManagement import Dirac as backend_Dirac
        backends.append(backend_Dirac(act_only_on_done = args.done))
    if args.runSlurm:
        from src.backendManagement import Slurm as backend_Slurm
        backends.append(backend_Slurm(act_only_on_done = args.done))
    if args.runSlurmProduction:
        from src.backendManagement import Slurm as backend_SlurmProd
        backends.append(backend_SlurmProd(act_only_on_done = args.done))

    for backend in backends:
        mr.management_routine(backend, args)


def do_test(args,rcard):
#### Test an initialised runcard
    from src.test_nnlojob import run_test
    run_test(args, rcard)


def do_initialise(args,rcard):
#### Initialisation: send stuff to Grid Storage
    mode_Warmup = (args.runArc or args.runSlurm)
    mode_Production = (args.runDirac or args.runArcProduction or args.runSlurmProduction)
    from src.Backend import generic_initialise
    local = False
    if args.runSlurm or args.runSlurmProduction:
        local = True
    if mode_Warmup:
        generic_initialise(rcard, warmup=True, grid=args.provWarm, 
                           overwrite_grid=args.continue_warmup, local=local)
    elif mode_Production:
        generic_initialise(rcard, production=True, grid=args.provWarm, local=local)
    else:
        logger.critical("Choose what do you want to initialise -(A/B/D/E/F/L)")


def do_run(args,rcard):
#### Run: run an ARC or DIRAC job for the given runcard
    if args.runArc:
        from src.runArcjob import runWrapper
    elif args.runArcProduction:
        from src.runArcjob import runWrapperProduction as runWrapper
    elif args.runDirac:
        from src.runDiracjob import runWrapper
    elif args.runSlurm:
        from src.runSlurmjob import runWrapper
    elif args.runSlurmProduction:
        from src.runSlurmjob import runWrapperProduction as runWrapper
    else:
        raise logger.critical("Choose what do you want to run -(A/B/D/E/F)")
    runWrapper(rcard, test=args.test)


def do_proxy(args,rcard):
#### Proxy management
    import src.proxyUtil as proxyUtil
    if args.runArc or args.runArcProduction:   proxyUtil.arcProxyWiz()
    if args.runDirac: proxyUtil.diracProxy()
