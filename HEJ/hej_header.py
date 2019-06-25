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
# for initialisation
runcardDir = "" # ONLY FOR NNNLOjets
executable_src_dir = "/mt/home/mheil/HEJ/reversed_hej/installed/HEJ/bin"             # Directory for exe
executable_exe = "HEJ"                               # Exectuable name
warmupthr  = 0
producRun  = 10
baseSeed   = 1234
jobName    = "testjob"
debug_level = 15
stacksize = 5000 #MB # RAM per job smaller->higher priority

# Grid config for LFN
lfndir         = "/grid/group/user/folder/"
# these are also used by gfal
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup" # could be useful for Sherpa

use_gfal = True
gfaldir = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/mheil/"
cvmfs_gfal_location = "/cvmfs/dirac.egi.eu/dirac/v6r20p16/Linux_x86_64_glibc-2.17/bin/" # set to None for environmentt gfal

# TMUX config
tmux_location= "tmux"

# Lhapdf config
lhapdf_grid_loc    = "/cvmfs/pheno.egi.eu/HEJ/LHAPDF" # not needed with cvmfs ?
lhapdf_loc         = "/cvmfs/pheno.egi.eu/HEJ/LHAPDF/bin/lhapdf"
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets
lhapdf             = lhapdf_grid_loc

# NNLOJET Database Parameters
dbname     = "nice_test"
provided_warmup_dir = None
runfile = "hejrun.py"

# Finalisation and storage options
finalise_no_cores = 16
timeout = 60

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = None
verbose_finalise = True
# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

warmup_base_dir = ""
production_base_dir = "/ResultsRunGrids"

short_stats = False

# ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"
arcbase  = "/mt/home/mheil/tst_grid/.arc/jobs.dat" # arc database

# DIRAC parameters
dirac_name = "user_name_for_dirac"
DIRAC_BANNED_SITES = []

# finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None

# socket default parameters
server_host = "url.of.the.socket.server"
port = 9999
wait_time = 3600

# SLURM parameters
local_run_directory = "/mt/home/mheil/grid_tst/"
warmup_queue = None
test_queue = None
production_queue = None
production_threads = 1
slurm_exclusive = True
slurm_exclude = []

# LOCAL (this doesn't work)
desktop_list = ["d76"]
