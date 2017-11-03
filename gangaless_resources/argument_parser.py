arguments = None
runcard = None
import __main__
import os

if os.path.basename(os.path.realpath(__main__.__file__)) == "main.py":
    from argparse import ArgumentParser
    parser = ArgumentParser()

    parser.add_argument("mode", help = "Mode [initialize/run/manage/proxy] ")
    parser.add_argument("runcard", nargs = "?", help = "Runcard to act upon")

    # Backend selection
    parser.add_argument("-A", "--runArc",   help = "Run/manage an Arc job (warmup)", action = "store_true")
    parser.add_argument("-B", "--runArcProduction",   help = "Run/manage an Arc job (production)", action = "store_true")
    parser.add_argument("-D", "--runDirac", help = "Run/manage a dirac job (production)", action = "store_true")

    # Initialisation options
    parser.add_argument("-L", "--lhapdf",    help = "Send LHAPDF to Grid", action = "store_true")

    parser.add_argument("-n", "--noProxy", help = "Bypasses proxy creation", action = "store_true")

    # Global management
    parser.add_argument("-g", "--get_data", help = "getdata from an ARC job", action = "store_true")
    parser.add_argument("-k", "--killJob", help = "kill a given job", action = "store_true")
    parser.add_argument("-i", "--info", help = "retrieve arcstat/diracstat for a given job", action = "store_true")
    parser.add_argument("-I", "--infoVerbose", help = "retrieve arcstat/diracstat for a given job (more verbose, only ARC)", action = "store_true")
    parser.add_argument("-p", "--printme", help = "do arccat to a given job", action = "store_true")
    parser.add_argument("-P", "--printmelog", help = "do arccat to the *.log files of a given job (only ARC)", action = "store_true")
    parser.add_argument("-j", "--idjob", help = "id of the job to act upon")
    parser.add_argument("-w", "--provWarm", help = "Provide warmup files for an DIRAC run (only with ini)")
    parser.add_argument("-e", "--enableme", help = "enable database entry", action = "store_true")
    parser.add_argument("-f", "--find", help = "Only database entries in which a certain string is found are shown")
    parser.add_argument("-s", "--stats", help = "output statistics for all subjobs in a dirac job", action = "store_true")

    # Arc only
    parser.add_argument("-u", "--updateArc", help = "fetch and save all stdout of all ARC active runs", action = "store_true")
    parser.add_argument("-r", "--renewArc", help = "renew the proxy of one given job", action = "store_true")
    parser.add_argument("-c", "--clean", help = "clean given job from the remote cluster", action = "store_true")
    parser.add_argument("-test", "--test", help = "Use test queue (only runs for 20 minutes)", action = "store_true")

    # Dirac only
    parser.add_argument("-S", "--statsCheat", help = "Dirac only, use a modified version of dirac to speed up the information retrieval process", action = "store_true")

    arguments = parser.parse_args()
    runcard = arguments.runcard
