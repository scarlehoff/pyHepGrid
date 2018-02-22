import os
import sys
import src.utilities as util
import shutil
import src.header as header
import subprocess 

def setup():
    oldpath = os.getcwd()
    try:
        shutil.rmtree(header.sandbox_dir)
    except FileNotFoundError as e:
        pass
    os.mkdir(header.sandbox_dir)
    shutil.copyfile(header.runfile, os.path.join(header.sandbox_dir,header.runfile))
    os.chdir(header.sandbox_dir)
    path_to_orig = os.path.relpath(oldpath,os.getcwd())
    header.dbname = os.path.join(path_to_orig,header.dbname)

def run_test(args, runcard):
    header.debug_level = 99999
    
    if args.runArc:
        from src.runArcjob import testWrapper
    elif args.runArcProduction:
        from src.runArcjob import testWrapperProduction as testWrapper
    elif args.runDirac:
        from src.runDiracjob import testWrapper
    else:
        raise Exception("Choose what you want to test -(A/B/D)")

    rncards, dCards = util.expandCard(runcard)

    setup()

    for r in rncards:
        nnlojob_args = testWrapper(r, dCards).replace("\"","").split()
        util.spCall(["chmod","+x",header.runfile])
        util.spCall(["./{0}".format(header.runfile)] + nnlojob_args)
#        print(len(nnlojob_args))
