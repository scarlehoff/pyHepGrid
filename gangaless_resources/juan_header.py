import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

# Global Variables (default values)
runcardDir = "/mt/home/jmartinez/Runcards"
NNLOJETdir = "/mt/home/jmartinez/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 500
baseSeed   = 100
jobName    = "testjob"
debug_level = 0

# Grid config 
lfndir   = "/grid/pheno/jmartinez"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

# Lhapdf config
lhapdf_grid_loc = "util/lhapdf.tar.gz"  
lhapdf_loc = "lhapdf" 
lhapdf_ignore_dirs = []# ["doc", "examples", "config"]
lhapdf = get_cmd_output("lhapdf-config","--prefix")
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets
 
# NNLOJET Database Parameters
dbname     = "NNLOJET_december.dat"     

# Finalisation and storage options
finalise_no_cores = 15

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = None

warmup_base_dir = "/WarmupsRunGrids"
production_base_dir = "/ResultsRunGrids"

short_stats = True

# ARC parameters
ce_base = "ce1.dur.scotgrid.ac.uk"
# ce_base = "ce02.tier2.hep.manchester.ac.uk"
# ce_base = "svr009.gla.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"

# DIRAC parameters
dirac_name = "juan.m.cruzmartinez"

# finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None

# socket parameters
server_host = "gridui1.dur.scotgrid.ac.uk"
port = 8888
