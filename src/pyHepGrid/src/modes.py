from pyHepGrid.src.header import logger
import pyHepGrid.src.proxyUtil as proxyUtil
import pyHepGrid.src.main_routines as mr
import pyHepGrid.src.runArcjob as runArcJob
import pyHepGrid.src.runDiracjob as runDiracJob
import pyHepGrid.src.runSlurmjob as runSlurmJob
import pyHepGrid.src.backendManagement as bm
import pyHepGrid.src.test_nnlojob
from pyHepGrid.src.Backend import generic_initialise


def do_management(args, rcard):
    # Management of running/finished jobs
    backend_setups = {
        "runArc": {"backend": bm.Arc,
                   "kwargs": {"production": False}},
        "runArcProduction": {"backend": bm.Arc,
                             "kwargs": {"production": True}},
        "runDirac": {"backend": bm.Dirac,
                     "kwargs": {}},
        "runSlurm": {"backend": bm.Slurm,
                     "kwargs": {"production": False}},
        "runSlurmProduction": {"backend": bm.Slurm,
                               "kwargs": {"production": True}}
    }

    for _backend in backend_setups:
        if getattr(args, _backend):  # If mode is selected
            backend_opt = backend_setups[_backend]
            kwargs = backend_opt["kwargs"]
            backend = backend_opt["backend"](
                act_only_on_done=args.done, **kwargs)
            logger.info("{0}".format(backend))
            mr.management_routine(backend, args)


def do_test(args, rcard):
    # Test an initialised runcard
    pyHepGrid.src.test_nnlojob.run_test(args, rcard)


def do_initialise(args, rcard):
    # Initialisation: send stuff to Grid Storage
    mode_Warmup = (args.runArc or args.runSlurm)
    mode_Production = (
        args.runDirac or args.runArcProduction or args.runSlurmProduction)
    local = False
    if args.runSlurm or args.runSlurmProduction:
        local = True
    if mode_Warmup:
        generic_initialise(rcard, warmup=True, grid=args.provWarm,
                           overwrite_grid=args.continue_warmup, local=local)
    elif mode_Production:
        generic_initialise(rcard, production=True,
                           grid=args.provWarm, local=local)
    else:
        logger.critical("Choose which mode you want to initialise -(A/B/D/E/F/L)")


def do_run(args, rcard):
    # Run: run an ARC or DIRAC job for the given runcard
    runfuncs = {
        "runArc": runArcJob.runWrapper,
        "runArcProduction": runArcJob.runWrapperProduction,
        "runDirac": runDiracJob.runWrapper,
        "runSlurm": runSlurmJob.runWrapper,
        "runSlurmProduction": runSlurmJob.runWrapperProduction
    }

    func_selected = False
    for run_function in runfuncs:
        if getattr(args, run_function):  # If mode is selected
            runWrapper = runfuncs[run_function]
            runWrapper(rcard, test=args.test)
            func_selected = True

    if not func_selected:
        logger.critical("Choose which mode you want to run -(A/B/D/E/F)")


def do_proxy(args, rcard):
    # Proxy management
    if args.runArc or args.runArcProduction:
        proxyUtil.arcProxyWiz()
    if args.runDirac:
        proxyUtil.diracProxy()
