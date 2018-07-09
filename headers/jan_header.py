import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use src.utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

#
# Global Variables (default values)
# 
runcardDir = "/mt/home/jniehues/NNLOJET/driver/grid/"
NNLOJETdir = "/mt/home/jniehues/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 100
baseSeed   = 100
jobName    = "gridjob"
debug_level = 0

#
# Grid config 
#
lfndir   = "/grid/pheno/jniehues"
# The following options are not fully functional yet 
# in particular they need to be propagated to DIRAC/ARC.py via ?cmd line args?
# I may also have missed some hardcodings
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

# TMUX config
tmux_location= "tmux"

#lhapdf config
lhapdf_grid_loc = "input/" # util/ for Juan
lhapdf_loc = "lhapdf" # lhapdf for Juan
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to

# Use installed version of LHAPDF by default
lhapdf = get_cmd_output("lhapdf-config","--prefix")
 
#
# ARC parameters
#
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"
arcbase  = "/mt/home/jniehues/.arc/jobs.dat" # arc database

# DIRAC parameters
dirac_name = "jan.niehues"
DIRAC_BANNED_SITES = ["VAC.UKI-SCOTGRID-GLASGOW.uk"]

#
# NNLOJET Database Parameters
#
dbname     = "NNLOJET_november.dat"     
provided_warmup_dir = None

# Finalisation and storage options
finalise_no_cores = 15
timeout = 60

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_src.finalise()
finalisation_script = None
verbose_finalise = True
# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

warmup_base_dir = "/WarmupsRunGrids"
production_base_dir = "/ResultsRunGrids"

# src.finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None
wait_time = 3600 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)

#SLURM parameters
local_run_directory = "/ddn/data/qpsv27/run_directories/"
warmup_queue = "openmp7.q"
production_queue = "par7.q"
test_queue = "test.q"
production_threads = 24
