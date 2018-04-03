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

# Grid config 
lfndir         = "/grid/pheno/jwhitehead"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

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
timeout = 60
finalise_no_cores = 15

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = "src/finalise_james"
# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

warmup_base_dir = os.path.expanduser("~/warmups")
production_base_dir = os.path.expanduser("~/results")

short_stats = True

# ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"

# DIRAC parameters
dirac_name = "james.whitehead"

# finalise.py-only parameters
finalise_runcards = "runcards/finalise_runcards"
finalise_prefix = ""

# socket default parameters
server_host = "gridui1.dur.scotgrid.ac.uk"
port = 8080
wait_time = 3600 # default waiting time for the socket server (time between the first job activates and nnlojet starting to run)
