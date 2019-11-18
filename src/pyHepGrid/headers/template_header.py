import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

# Global Variables
# Global Variables (default values, can be changed by runcard.py)
runcardDir = "/path/to/runcard/directory/" # Directory for grid runcard storage
executable_src_dir = "/path/to/nnlojet/directory"             # Directory for NNLOJET
executable_exe = "NNLOJET"                               # Executable name
runmode    = "NNLOJET"
warmupthr  = 8
producRun  = 100
baseSeed   = 100
events     = 100
jobName    = "testjob"
debug_level = 0
stacksize = 50 #MB

# Grid folder config
grid_input_dir  = "input"
grid_output_dir = "output"
grid_warmup_dir = "warmup"
runfile    = "nnlorun.py"

use_gfal = False
gfaldir = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/dwalker/"
cvmfs_gfal_location = "/cvmfs/dirac.egi.eu/dirac/v6r22p6/Linux_x86_64_glibc-2.17/bin/" # set to None for environment gfal

# TMUX config
tmux_location= "tmux"

# Lhapdf config
lhapdf_grid_loc    = "location/relative/to/gfaldir/lhapdf.tar.gz"
lhapdf_loc         = "lhapdf"
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets
lhapdf             = "/path/to/lhapdf"
use_cvmfs_lhapdf = False
cvmfs_lhapdf_location = "/cvmfs/pheno.egi.eu/lhapdf/6.1.6"

## Rivet config
use_custom_rivet = False
grid_rivet_dir = None

# SQLite Database Parameters
dbname     = "/path/to/sqlite/database/for/storage.dat"
provided_warmup_dir = None

# Finalisation and storage options
finalise_no_cores = 15
timeout = 60

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = None
verbose_finalise = True
recursive_finalise = True

# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

warmup_base_dir = "/WarmupsRunGrids"
production_base_dir = "/ResultsRunGrids"

short_stats = False

# ARC parameters
ce_base = "base_computing_element.ac.uk"
ce_test = "test_computing_element.ac.uk"
ce_listfile = "computing_elements.txt"
arcbase = "/path/to/ARC/base/.arc/jobs.dat" # arc database
arc_submit_threads = 10 # Threads for use in arc submission. Raising this too high /may/ result
                        # in some job loss due to filesystem locks on arc jobs database.

# DIRAC parameters
dirac_name = "user_name_for_dirac"
DIRAC_BANNED_SITES = []
dirac_platform = "EL7"

# finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None

# socket default parameters
server_host = "url.of.the.socket.server"
port = 9999
wait_time = 3600 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)

#SLURM parameters
local_run_directory = "/path/to/local/working/dir/"
warmup_queue = "nameOfWarmupQueue"
test_queue = "nameOfTestQueue"
production_queue = "nameOfProductionQueue"
production_threads = 24
slurm_exclusive = True
slurm_exclude = []

# LOCAL
desktop_list = []
