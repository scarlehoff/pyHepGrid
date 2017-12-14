arguments = None
runcard = None
import __main__
import os

try:
    caller_script = os.path.basename(os.path.realpath(__main__.__file__)) 
except:
    caller_script = "None"

if caller_script == "main.py":

    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("mode", help = "Mode [initialize/run/manage/proxy] ")
    parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

    # Backend selection
    parser.add_argument("-A", "--runArc",   help = "Run/manage an Arc job (warmup)", action = "store_true")
    parser.add_argument("-B", "--runArcProduction",   help = "Run/manage an Arc job (production)", action = "store_true")
    parser.add_argument("-D", "--runDirac", help = "Run/manage a dirac job (production)", action = "store_true")

    # LHAPDF initialisation
    class LHAPDF_initAction(argparse.Action):
        def __init__(self, nargs=0, **kw):
            super().__init__(nargs=nargs, **kw)
        def __call__(self, parser, namespace, values, option_string=None):
            from utilities import lhapdfIni
            lhapdfIni()
            parser.exit(0)
    parser.add_argument("-L", "--lhapdf", help = "Send LHAPDF to Grid", action = LHAPDF_initAction)

    parser.add_argument("-n", "--noProxy", help = "Bypasses proxy creation", action = "store_true")
    parser.add_argument("--yes", help = "Assume yes to all questions in management (use with care!)", action = "store_true")

    # Global management
    parser.add_argument("-g", "--get_data", help = "getdata from an ARC job", action = "store_true")
    parser.add_argument("-k", "--kill_job", help = "kill a given job", action = "store_true")
    parser.add_argument("-i", "--info", help = "retrieve arcstat/diracstat for a given job", action = "store_true")
    parser.add_argument("-I", "--infoVerbose", help = "retrieve arcstat/diracstat for a given job (more verbose, only ARC)", action = "store_true")
    parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
    parser.add_argument("-P", "--printmelog", help = "do arccat to the *.log files of a given job (only ARC)", action = "store_true")
    parser.add_argument("-j", "--idjob", help = "id of the job to act upon")
    parser.add_argument("-w", "--provWarm", help = "Provide warmup files for an DIRAC run (only with ini)")
    parser.add_argument("-f", "--find", help = "Only database entries in which a certain string is found are shown")
    parser.add_argument("-s", "--stats", help = "output statistics for all subjobs in a dirac job", action = "store_true")
    parser.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
    parser.add_argument("-d", "--disableme", help = "disable database entry", action = "store_true")

    # Options that modify the jobs we can act onto
    parser.add_argument("--done", help = "For multiruns, only act on jobs which have the done status store in the database", action = "store_true")


    # Arc only
    parser.add_argument("-u", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
    parser.add_argument("-r", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
    parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
    parser.add_argument("-G", "--getmewarmup", help = "Force the retrieval of the warmup file from an unfinished job", action = "store_true")
    parser.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes)", action = "store_true")
    parser.add_argument("--error", help = "When doing arccat, print the standard error instead of std output", action = "store_true")
    parser.add_argument("-mf","--most_free_cores",  help = "Override ce_base with ce with most free cores", action = "store_true")

    # Dirac only
    parser.add_argument("-S", "--statsCheat", help = "Dirac only, use a modified version of dirac to speed up the information retrieval process", action = "store_true")

    arguments = parser.parse_args()
    runcard = arguments.runcard
    override_ce_base = arguments.most_free_cores
