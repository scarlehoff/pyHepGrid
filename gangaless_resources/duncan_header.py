import subprocess as sp
import os
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
baseSeed   = 1
jobName    = "gridjob"
debug_level = 0

# Grid config 
lfndir   = "/grid/pheno/dwalker"
lfn_input_dir  = "input"
lfn_output_dir = "output"
lfn_warmup_dir = "warmup"

#Lhapdf config
lhapdf_grid_loc = "input/lhapdf.tar.gz" 
lhapdf_loc = "lhapdf"
lhapdf_ignore_dirs = ["doc", "examples", "config", "LHAPDF-6.2.1/share", 
                      "tests", "python2.6", "wrappers", "bin", "include", 
                      "src"]
lhapdf = get_cmd_output("lhapdf-config","--prefix")
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets

# NNLOJET Database Parameters
dbname     = "/mt/home/dwalker/submit/gangaless_resources/wpt.dat"     
#dbname     = "/mt/home/dwalker/submit/gangaless_resources/test.dat"     

# Finalisation and storage options
finalise_no_cores = 15

# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = "finalise"

warmup_base_dir = None
production_base_dir = os.path.expanduser("~/working/RESULTS/")

short_stats = True

# ARC parameters
ce_base = "ce1.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"


# DIRAC parameters
dirac_name = "duncan.walker"

# finalise.py-only parameters
finalise_runcards = "finalise_runcards"
finalise_prefix = "results_"

# socket parameters
server_host = "gridui2.dur.scotgrid.ac.uk"
port = 8010
