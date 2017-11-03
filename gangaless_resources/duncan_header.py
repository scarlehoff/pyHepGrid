import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")


# Global Variables (default values)
runcardDir = "/mt/home/dwalker/NNLOJET/driver/grid/"
NNLOJETdir = "/mt/home/dwalker/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 1000
baseSeed   = 4000
jobName    = "gridjob"

# Grid config 
lfndir   = "/grid/pheno/dwalker"

#Lhapdf config
lhapdf_grid_loc = "input/" 
lhapdf_loc = "lhapdf"
lhapdf_ignore_dirs = ["doc", "examples", "config", "LHAPDF-6.2.1/share"]
lhapdf = get_cmd_output("lhapdf-config","--prefix")

# NNLOJET Database Parameters
dbname     = "/mt/home/dwalker/submit/gangaless_resources/alljobs.dat"     

# Finalisation and storage options
finalise_no_cores = 15

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = "finalise"

warmup_base_dir = None
production_base_dir = "../../working/RESULTS/"

# ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"

# DIRAC parameters
dirac_name = "duncan.walker"

# finalise.py-only parameters
finalise_runcards = "finalise_runcards"
finalise_prefix = "results_"
