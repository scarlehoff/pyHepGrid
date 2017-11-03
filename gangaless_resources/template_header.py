import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

# Global Variables (default values, can be changed by runcard.py)
runcardDir = "/mt/home/dwalker/NNLOJET/driver/grid/"
NNLOJETdir = "/mt/home/dwalker/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 100
baseSeed   = 100
jobName    = "testjob"

# Grid config 
lfndir   = "/grid/pheno/dwalker"

# Lhapdf config
lhapdf_grid_loc = "input/" 
lhapdf_loc = "lhapdf/"
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to
lhapdf = get_cmd_output("lhapdf-config","--prefix")
 
# NNLOJET Database Parameters
dbname     = "NNLOJET_november.dat"     

# Finalisation and storage options
finalise_no_cores = 15

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = None

warmup_base_dir = "/WarmupsRunGrids"
production_base_dir = "/ResultsRunGrids"

# ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"

# finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None
