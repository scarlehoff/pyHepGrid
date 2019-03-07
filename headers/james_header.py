import subprocess as sp
import os

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

# Global Variables (default values, can be changed by runcard.py)
runcardDir = "/mt/home/jwhitehead/runcards/"
NNLOJETdir = "/mt/home/jwhitehead/NNLOJET/"
NNLOJETexe = "NNLOJET"
#don't set warmupthr > 16 (not permitted by arc)
warmupthr  = 16
#no. separate production jobs submitted concurrently
producRun  = 100
#first seed (runs from baseseed to baseseed+producRun-1)
baseSeed   = 100
#how arc/dirac identifies the job
jobName    = "testjob"
debug_level = 0
stacksize = 50 #MB

# Grid config 
lfndir         = "/grid/pheno/jwhitehead"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

use_gfal = False
protocol = "srm" # "dav" "gsiftp"
gfaldir = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/jwhitehead/"
cvmfs_gfal_location = "/cvmfs/dirac.egi.eu/dirac/v6r20p16/Linux_x86_64_glibc-2.17/bin/" # set to None for environmentt gfal

# TMUX config
tmux_location= "tmux"

# Lhapdf config
lhapdf_grid_loc    = "util/lhapdf.tar.gz"
lhapdf_loc         = "lhapdf"
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets
lhapdf             = get_cmd_output("lhapdf-config","--prefix")
use_cvmfs_lhapdf = False
cvmfs_lhapdf_location = "/cvmfs/pheno.egi.eu/lhapdf/6.1.6"

# NNLOJET Database Parameters
dbname     = "NNLOJET_database.dat"     
arcbase    = os.path.expanduser("~/.arc/jobs.dat")
provided_warmup_dir = None

# Finalisation and storage options
timeout = 60
finalise_no_cores = 15

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = "src/finalise"
#finalisation_script = None
verbose_finalise = True
# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

warmup_base_dir = os.path.expanduser("~/warmups")
#warmup_base_dir = "warmups"
#production_base_dir = os.path.expanduser("~/results")
production_base_dir = "/scratch/jwhitehead/results"

short_stats = True

# ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"

# DIRAC parameters
dirac_name = "james.whitehead"
DIRAC_BANNED_SITES = ["VAC.UKI-SCOTGRID-GLASGOW.uk"]

# finalise.py-only parameters
finalise_runcards = "runcards/finalise_runcards"
finalise_prefix = ""

# socket default parameters
server_host = "gridui1.dur.scotgrid.ac.uk"
port = 8080
wait_time = 14400 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)
#sockets_active = 10
#warmupthr = 8

#SLURM parameters
local_run_directory = "/ddn/data/qpsv27/run_directories/"
warmup_queue = "openmp7.q"
production_queue = "par7.q"
test_queue = "test.q"
production_threads = 24
slurm_exclusive = True
slurm_exclude = []

# LOCAL
desktop_list = ["ws1"]
