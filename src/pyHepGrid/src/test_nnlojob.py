import os
import pyHepGrid.src.utilities as util
import shutil
import pyHepGrid.src.header as header


def setup_runfiles():
    inputs = next(arg for arg in header.DIRACSCRIPTDEFAULT
                  if arg.startswith('InputSandbox')
                  ).split("=")[-1].strip(' {};')
    inputs = [i.strip('" ') for i in inputs.split(',')]
    if header.runfile not in inputs or header.grid_helper not in inputs:
        raise Exception('"runfile" or "grid_helper" missing in Dirac arguments')

    for file in inputs:
        shutil.copyfile(
            file, os.path.join(header.sandbox_dir, os.path.basename(file)))


def setup():
    oldpath = os.getcwd()
    try:
        shutil.rmtree(header.sandbox_dir)
    except FileNotFoundError:
        pass
    os.mkdir(header.sandbox_dir)
    setup_runfiles()
    os.chdir(header.sandbox_dir)
    path_to_orig = os.path.relpath(oldpath, os.getcwd())
    header.dbname = os.path.join(path_to_orig, header.dbname)


def run_test(args, runcard):
    # header.debug_level = 99999

    if args.runArc:
        from pyHepGrid.src.ArcRunBackend import testWrapper
    elif args.runArcProduction:
        from pyHepGrid.src.ArcRunBackend import testWrapperProduction as testWrapper
    elif args.runDirac:
        from pyHepGrid.src.DiracRunBackend import testWrapper
    elif args.runSlurm:
        from pyHepGrid.src.SlurmRunBackend import testWrapper
    elif args.runSlurmProduction:
        from pyHepGrid.src.SlurmRunBackend import testWrapperProduction \
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
