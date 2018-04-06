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
    import src.logger
    parser = argparse.ArgumentParser()

    parser.add_argument("mode", help = "Mode [initialize/run/manage/test] ")
    parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

    # src.Backend selection
    parser.add_argument("-A", "--runArc",   help = "Run/manage/test an Arc job (warmup)", action = "store_true")
    parser.add_argument("-B", "--runArcProduction",   help = "Run/manage/test an Arc job (production)", action = "store_true")
    parser.add_argument("-D", "--runDirac", help = "Run/manage/test a dirac job (production)", action = "store_true")

    # LHAPDF initialisation
    class LHAPDF_initAction(argparse.Action):
        def __init__(self, nargs=0, **kw):
            super().__init__(nargs=nargs, **kw)
        def __call__(self, parser, namespace, values, option_string=None):
            from src.utilities import lhapdfIni
            lhapdfIni()
            parser.exit(0)
    parser.add_argument("-L", "--lhapdf", help = "Send LHAPDF to Grid", action = LHAPDF_initAction)

    parser.add_argument("-gp", "--genProxy", help = "Generates proxy for the chosen backend", action = "store_true")
    parser.add_argument("-n", "--noProxy", help = "[DEPRECATED] Bypasses proxy creation", action = "store_true")
    parser.add_argument("--yes", help = "Assume answer yes to all questions in management (use with care!)", action = "store_true")

    # Global management
    parser.add_argument("-cw", "--continue_warmup", help = "Continue a previous warmup", action = "store_true")
    parser.add_argument("-g", "--get_data", help = "getdata from an ARC job", action = "store_true")
    parser.add_argument("-k", "--kill_job", help = "kill a given job", action = "store_true")
    parser.add_argument("-i", "--info", help = "retrieve arcstat/diracstat for a given job", action = "store_true")
    parser.add_argument("-I", "--infoVerbose", help = "retrieve arcstat/diracstat for a given job (more verbose, only ARC)", action = "store_true")
    parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
    parser.add_argument("-P", "--printmelog", help = "do arccat to the *.log files of a given job (only ARC)", action = "store_true")
    parser.add_argument("-j", "--idjob", help = "id of the job to act upon")
    parser.add_argument("-w", "--provWarm", help = "Provide warmup files for an production run (only with ini)")
    parser.add_argument("-f", "--find", help = "Only database entries in which a certain string is found are shown")
    parser.add_argument("-s", "--stats", help = "output statistics for all subjobs in a dirac job", action = "store_true")
    parser.add_argument("--simple_string", help = "To be used with -s/-S, prints one liners for done/total", action = "store_true")
    parser.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
    parser.add_argument("-d", "--disableme", help = "disable database entry", action = "store_true")
    parser.add_argument("-dbg", "--debuglevel", help = "set debug level", type=str, default="VALUES")
    parser.add_argument("--list_disabled", help = "List also disabled entries", action = "store_true")

    # Options that modify the jobs we can act onto
    parser.add_argument("--done", help = "For multiruns, only act on jobs which have the done status stored in the database", action = "store_true")


    # Arc only
    parser.add_argument("-u", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
    parser.add_argument("-r", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
    parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
    parser.add_argument("-G", "--getmewarmup", help = "Force the retrieval of the warmup file from an unfinished job", action = "store_true")
    parser.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes). NB this is different to the test mode, which runs nnlojob with the intended submission arguments locally for testing before submission. Also, the test queue was broken as of 22/2/18, so this option is a little bit broken.", action = "store_true")
    parser.add_argument("--error", help = "When doing arccat, print the standard error instead of std output", action = "store_true")
    parser.add_argument("-mf","--most_free_cores",  help = "Override ce_base with ce with most free cores", action = "store_true")

    # Dirac only
    parser.add_argument("-S", "--statsCheat", help = "Dirac only, use a modified version of dirac to speed up the information retrieval process", action = "store_true")


    # further overrides
    parser.add_argument("-a", "--args", help = "Extra arguments that override those in BOTH the header and the runcard. Syntax:> var_name_1 val_1 var_name_2 val_2 var_name_3 val_3 ...",nargs="+")

    arguments = parser.parse_args()

    runcard = arguments.runcard
    override_ce_base = arguments.most_free_cores
    additional_arguments = {}

    # Save to logger as header not loaded yet. 
    # Reference copied to header.logger at the top of header when loaded
    src.logger.logger = src.logger.setup_logger(arguments.debuglevel.upper()) 

    if arguments.args is not None:
        if len(arguments.args)%2!=0:
            src.logger.logger.error("Not all additional arguments specified at prompt have values.")
            import sys
            sys.exit(-1)
        else:
            for i in range(0,len(arguments.args),2):
                additional_arguments[arguments.args[i]]=arguments.args[i+1]
        
