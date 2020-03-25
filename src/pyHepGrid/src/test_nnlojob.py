import os
import sys
import pyHepGrid.src.utilities as util
import shutil
import pyHepGrid.src.header as header
import subprocess


def setup():
    oldpath = os.getcwd()
    try:
        shutil.rmtree(header.sandbox_dir)
    except FileNotFoundError as e:
        pass
    os.mkdir(header.sandbox_dir)
    shutil.copyfile(header.runfile, os.path.join(
        header.sandbox_dir, os.path.basename(header.runfile)))
    os.chdir(header.sandbox_dir)
    path_to_orig = os.path.relpath(oldpath, os.getcwd())
    header.dbname = os.path.join(path_to_orig, header.dbname)


def run_test(args, runcard):
    # header.debug_level = 99999

    if args.runArc:
        from pyHepGrid.src.runArcjob import testWrapper
    elif args.runArcProduction:
        from pyHepGrid.src.runArcjob import testWrapperProduction as testWrapper
    elif args.runDirac:
        from pyHepGrid.src.runDiracjob import testWrapper
    elif args.runSlurm:
        from pyHepGrid.src.runSlurmjob import testWrapper
    elif args.runSlurmProduction:
        from pyHepGrid.src.runSlurmjob import testWrapperProduction \
            as testWrapper
    else:
        raise Exception("Choose what you want to test -(A/B/D/E/F)")

    rncards, dCards = util.expandCard(runcard)

    # if args.runSlurm:
    #     header.runfile = header.SLURMSCRIPTDEFAULT
    # if args.runSlurmProduction:
    #     header.runfile = header.SLURMSCRIPTDEFAULT_PRODUCTION

    setup()

    for r in rncards:
        nnlojob_args = testWrapper(r, dCards).replace("\"", "").split()
        runfile = os.path.basename(header.runfile)
        util.spCall(["chmod", "+x", runfile])
        util.spCall(["./{0}".format(runfile)] + nnlojob_args)
