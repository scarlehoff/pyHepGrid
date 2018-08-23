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
runcardDir = os.path.expanduser("/mt/home/dwalker/pyhepgrid/runcards/")
NNLOJETdir ="/mt/home/dwalker/nnlojet/"
NNLOJETexe = "NNLOJET"
warmupthr  = 1
producRun  = 1000
baseSeed   = 1
jobName    = "gridjob"
debug_level = 0
stacksize = 50 #MB

# Grid config 
lfndir   = "/grid/pheno/dwalker"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

# TMUX config
tmux_location= "tmux"

#Lhapdf config
lhapdf_grid_loc = "input/lhapdf.tar.gz" 
lhapdf_loc = "lhapdf"
lhapdf_ignore_dirs = ["doc", "examples", "config", "LHAPDF-6.2.1/share", 
                      "tests", "python2.6", "wrappers", "bin", "include", 
                      "src"]
lhapdf = get_cmd_output("lhapdf-config","--prefix")
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets

# NNLOJET Database Parameters
dbname     = "/mt/home/dwalker/pyhepgrid/grid/databases/batchdb.dat"     
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
default_runfolder = os.path.expanduser("~/pyhepgrid/warmups/")

warmup_base_dir = os.path.expanduser("~/pyhepgrid/warmups")
production_base_dir = os.path.expanduser("/mt/home/dwalker/pyhepgrid/results/")

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
server_host = "d76.phyip3.dur.ac.uk"
port = 9050
wait_time = 3600 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)

#SLURM parameters
local_run_directory = "/mt/home/dwalker/pyhepgrid/run_directories/"
warmup_queue = None
production_queue = None
test_queue = None
production_threads = 1
slurm_exclusive = True
slurm_exclude = ["d76"]
slurmprodtable = "slurmjobs_prod"

#LOCAL runs
desktop_list = ["d76", "d77", "d78", "d79", "d75", "d74"]
