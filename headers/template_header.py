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
warmupthr  = 16
producRun  = 100
baseSeed   = 100
jobName    = "testjob"
debug_level = 0

# Grid config 
lfndir         = "/grid/pheno/dwalker"
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

# Finalisation and storage options
finalise_no_cores = 15

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

# DIRAC parameters
dirac_name = "duncan.walker"

# finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None

# socket default parameters
server_host = "gridui1.dur.scotgrid.ac.uk"
port = 8080
