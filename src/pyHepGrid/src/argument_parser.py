arguments = None
runcard = None
import __main__
import os

def check_mode(rmode,args,logger):
    if len(rmode) < 3:
        logger.critical("Mode {0} not valid".format(rmode))

    if (rmode[:3] == "run" and not "runcard" in rmode) or rmode[:3] == "man" :
        if args.runDirac and args.runArc:
            if not args.idjob == "all":
                logger.critical("Please choose only Dirac (-D), Arc (-A), Arc Production Mode (-B), Slurm Warmup mode (-E) or Slurm production mode (-F) (unless using -j all)")
        if not args.runDirac and not args.runArc and not args.runArcProduction and \
           not args.runSlurm and not args.runSlurmProduction:
            logger.critical("Please choose Dirac (-D), Arc (-A), Arc Production Mode (-B), Slurm Warmup mode (-E) or Slurm production mode (-F)")

try:
    caller_script = os.path.basename(os.path.realpath(__main__.__file__))
except:
    caller_script = "None"

if caller_script in ("main.py", "pyHepGrid"):
    import argparse
    import pyHepGrid.src.logger
    parser = argparse.ArgumentParser()

    parser.add_argument("mode", help = "Mode [initialize/run/manage/test] ")
    parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

    # pyHepGrid.src.Backend selection
    parser_back = parser.add_argument_group("backend selection")
    parser_back.add_argument("-A", "--runArc",   help = "Run/manage/test an Arc job (warmup)", action = "store_true")
    parser_back.add_argument("-B", "--runArcProduction",   help = "Run/manage/test an Arc job (production)", action = "store_true")
    parser_back.add_argument("-D", "--runDirac", help = "Run/manage/test a dirac job (production)", action = "store_true")
    parser_back.add_argument("-E", "--runSlurm", help = "Run/manage/test a SLURM job (warmup)", action = "store_true")
    parser_back.add_argument("-F", "--runSlurmProduction", help = "Run/manage/test a SLURM job (production)", action = "store_true")


    # Proxy initialisation
    parser_proxy = parser.add_argument_group("proxy initialisation options")
    parser_proxy.add_argument("-gp", "--genProxy", help = "Generates proxy for the chosen backend", action = "store_true")
    parser_proxy.add_argument("-n", "--noProxy", help = "[DEPRECATED] Bypasses proxy creation", action = "store_true")

    # Global options
    parser_global = parser.add_argument_group("global options", "Modify the runtime behaviour of main.py")
    parser_global.add_argument("-a", "--args", help = "Extra arguments that override those in BOTH the header and the runcard. Syntax:> var_name_1 val_1 var_name_2 val_2 var_name_3 val_3 ...",nargs="+")
    parser_global.add_argument("-dbg", "--debuglevel", help = "set logger debug level: [DEBUG/VALUES/INFO/WARNING/ERROR/CRITICAL] default=VALUES", type=str, default="VALUES")
    # Option override
    parser_global.add_argument("--yes", help = "Assume answer yes to all questions in management (use with care!)", action = "store_true")




    ######## Initialisation options
    parser_ini = parser.add_argument_group("initialisation options", "To be used with mode=initialisation")
    parser_ini.add_argument("-w", "--provWarm", help = "Provide warmup files for an production run")
    parser_ini.add_argument("-cw", "--continue_warmup", help = "Continue a previous warmup", action = "store_true")
   # LHAPDF initialisation
    class LHAPDF_initAction(argparse.Action):
        def __init__(self, nargs=0, **kw):
            super().__init__(nargs=nargs, **kw)
        def __call__(self, parser, namespace, values, option_string=None):
            from pyHepGrid.src.utilities import lhapdfIni
            lhapdfIni()
            parser.exit(0)
    parser_ini.add_argument("-L", "--lhapdf", help = "Send LHAPDF to Grid", action = LHAPDF_initAction)

    ######## Run options
    parser_run = parser.add_argument_group("running options", "To be used with mode=run")
    parser_run.add_argument("-mf","--most_free_cores",  help = "Override ce_base with ce with most free cores", action = "store_true")
    parser_run.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes). NB this is different to the test mode, which runs nnlojob with the intended submission arguments locally for testing before submission. Also, the test queue was broken as of 22/2/18, so this option is a little bit broken.", action = "store_true")


    ######## Management options
    # Information about jobs
    parser_info = parser.add_argument_group("info options", "Display information about jobs, to be used with mode=man")
    parser_info.add_argument("-s","-S", "--stats", help = "output status statistics for all subjobs in a job", action = "store_true")
    parser_info.add_argument("--simple_string", help = "To be used with -s/-S, prints one liners for done/total", action = "store_true")
    parser_info.add_argument("-C", "--checkwarmup", help = "Check completed warmup to see if a warmup file is present", action = "store_true")
    parser_info.add_argument("--resubmit", help = "Resubmit if warmup not present. For use with --checkwarmup only", action = "store_true")
    parser_info.add_argument("-c","--completion", help = "Show current iteration completion of running jobs", action = "store_true")
    parser_info.add_argument("--gnuplot", help = "Add gnuplot output where possible", action = "store_true")

    parser_info.add_argument("-i", "--info", help = "retrieve arcstat/diracstat for a given job", action = "store_true")
    parser_info.add_argument("-I", "--infoVerbose", help = "retrieve arcstat/diracstat for a given job (more verbose, only ARC)", action = "store_true")

    parser_info.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
    parser_info.add_argument("-P", "--printmelog", help = "do arccat to the *.log files of a given job (only ARC)", action = "store_true")
    parser_info.add_argument("--error", help = "When doing arccat, print the standard error instead of std output", action = "store_true")

    # Getting Data
    parser_fin = parser.add_argument_group("finalisation options", "Retrieve or kill jobs, to be used with mode=man")
    parser_fin.add_argument("-g", "--get_data", help = "Retrieve all data for a database entry", action = "store_true")
    parser_fin.add_argument("-G", "--getmewarmup", help = "Force the retrieval of the warmup file from an unfinished job", action = "store_true")
    parser_fin.add_argument("-gg", "--get_grid_stdout", help = "Retrieve the warmup grid from stdout for a given job.", action = "store_true")

    # Killing jobs
    parser_fin.add_argument("-k", "--kill_job", help = "kill a given job", action = "store_true")

    # Options that act directly on the database
    parser_db = parser.add_argument_group("database options", "These options act directly on the database selecting specific entries or subjobs within said entries")
    parser_db.add_argument("-j", "--idjob", help = "id of the job(s) to act on. -jall Will act on all jobs, and multiple jobs can be selected with a comma separated list and ranges sspecified by hyphens. e.g. -j1,4-6 selects jobs 1,4,5,6")
    parser_db.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
    parser_db.add_argument("-d", "--disableme", help = "disable database entry", action = "store_true")
    parser_db.add_argument("-f", "--find", help = "Only database entries in which a certain string is found are shown")
    parser_db.add_argument("--done", help = "For multiruns, only act on jobs which have the done status stored in the database", action = "store_true")
    parser_db.add_argument("--list_disabled", help = "List also disabled entries", action = "store_true")

    # Some Arc-only utilities
    parser_arc = parser.add_argument_group("arc-only options")
    parser_arc.add_argument("-uArc", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
    parser_arc.add_argument("-rArc", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
    parser_arc.add_argument("-cArc", "--clean", help = "clean given job from the remote cluster", action = "store_true")


    arguments = parser.parse_args()
    arguments.runcard = os.path.relpath(arguments.runcard, os.getcwd())
    runcard = arguments.runcard
    override_ce_base = arguments.most_free_cores
    additional_arguments = {}

    # Save to logger as header not loaded yet.
    # Reference copied to header.logger at the top of header when loaded
    pyHepGrid.src.logger.logger = pyHepGrid.src.logger.setup_logger(arguments.debuglevel.upper())
    check_mode(arguments.mode, arguments,pyHepGrid.src.logger.logger)

    if arguments.args is not None:
        if len(arguments.args)%2!=0:
            pyHepGrid.src.logger.logger.critical("Not all additional arguments specified at prompt have values.")
        else:
            for i in range(0,len(arguments.args),2):
                additional_arguments[arguments.args[i]]=arguments.args[i+1]
