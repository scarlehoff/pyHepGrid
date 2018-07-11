import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

# Global Variables (default values, can be changed by runcard.py)
runcardDir = "/mt/home/dwalker/NNLOJET/driver/grid/" # Directory for grid runcard storage
NNLOJETdir = "/mt/home/dwalker/NNLOJET/"             # Directory for NNLOJET
NNLOJETexe = "NNLOJET"                               # Exectuable name
warmupthr  = 8
producRun  = 100
baseSeed   = 100
jobName    = "testjob"
debug_level = 0
stacksize = 50 #MB

# Grid config 
lfndir         = "/grid/pheno/dwalker"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

# TMUX config
tmux_location= "tmux"

# Lhapdf config
lhapdf_grid_loc    = "util/lhapdf.tar.gz"
lhapdf_loc         = "lhapdf"
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets
lhapdf             = get_cmd_output("lhapdf-config","--prefix")
 
# NNLOJET Database Parameters
dbname     = "NNLOJET_database.dat"     
provided_warmup_dir = None

# Finalisation and storage options
finalise_no_cores = 15
timeout = 60

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = None
verbose_finalise = True
# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

warmup_base_dir = "/WarmupsRunGrids"
production_base_dir = "/ResultsRunGrids"

short_stats = False

# ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"
arcbase  = "/mt/home/dwalker/.arc/jobs.dat" # arc database

# DIRAC parameters
dirac_name = "duncan.walker"
DIRAC_BANNED_SITES = ["VAC.UKI-SCOTGRID-GLASGOW.uk"]

# finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None

# socket default parameters
server_host = "gridui1.dur.scotgrid.ac.uk"
port = 9999
wait_time = 3600 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)

#SLURM parameters
local_run_directory = "/ddn/data/qpsv27/run_directories/"
warmup_queue = "openmp7.q"
test_queue = "test.q"
production_queue = "par7.q"
production_threads = 24
slurm_exclusive = True
