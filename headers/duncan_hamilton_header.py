import subprocess as sp
import os
import random
######################################################
#                Helper Functions                    #
# Can't use src.utilities due to circular imports :( #
######################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

# Global Variables (default values)
runcardDir = os.path.expanduser("~/runcards/")
NNLOJETdir ="/ddn/data/qpsv27/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 24
producRun  = 1000
baseSeed   = 1
jobName    = "gridjob"
debug_level = 0

# Grid config 
lfndir   = "/grid/pheno/dwalker"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

# TMUX config
tmux_location= "/ddn/home/qpsv27/local/bin/tmux"

#Lhapdf config
lhapdf_grid_loc = "input/lhapdf.tar.gz" 
lhapdf_loc = "lhapdf"
lhapdf_ignore_dirs = ["doc", "examples", "config", "LHAPDF-6.2.1/share", 
                      "tests", "python2.6", "wrappers", "bin", "include", 
                      "src"]
lhapdf = get_cmd_output("lhapdf-config","--prefix")
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets

# NNLOJET Database Parameters
dbname     = "/ddn/home/qpsv27/grid/databases/batchdb.dat"     
arcbase    = "/dev/null/"
provided_warmup_dir = None

# Finalisation and storage options
timeout = 60
finalise_no_cores = 25

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_src.finalise()
finalisation_script = None
verbose_finalise = False

# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = os.path.expanduser("~/warmups/")

warmup_base_dir = os.path.expanduser("~/warmups")
production_base_dir = os.path.expanduser("/ddn/data/qpsv27/results/")

short_stats = True

# ARC parameters
ce_base = random.choice(["ce1.dur.scotgrid.ac.uk","ce2.dur.scotgrid.ac.uk"])
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"


# DIRAC parameters
dirac_name = "duncan.walker"
DIRAC_BANNED_SITES = ["VAC.UKI-SCOTGRID-GLASGOW.uk"]

# finalise.py-only parameters
finalise_runcards = "runcards/finalise_runcards"
finalise_prefix = "results_"

# socket parameters
server_host = "hamilton2.hpc.dur.ac.uk"
port = 9000
wait_time = 3600 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)

#SLURM parameters
local_run_directory = "/ddn/data/qpsv27/run_directories/"
#warmup_queue = "openmp7.q"
warmup_queue = "par7.q"
production_queue = "par7.q"
test_queue = "test.q"
production_threads = 24
