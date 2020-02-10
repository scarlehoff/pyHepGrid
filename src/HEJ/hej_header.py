import subprocess as sp
from getpass import getuser
import os

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

def base_dir(folder):
  return "/mt/home/{0}/tst_grid/{1}".format(getuser(), folder)

def scratch_dir(folder):
  return "/scratch/{0}/tst_grid/{1}".format(getuser(), folder)

## Global Variables (default values, can be changed by runcard.py)
## for initialisation
runcardDir = base_dir("Setup/")
provided_warmup_dir = runcardDir
## local directory for exe (TODO not used yet for automatic upload)
executable_src_dir = "/mt/home/{0}/HEJ/reversed_hej/installed/".format(getuser())
## Executable name
executable_exe = "HEJ/bin/HEJ"

## General run setting
warmupthr  = 0
producRun  = 2
baseSeed   = 1
events     = 123
jobName    = "HEJ"
debug_level = 16
copy_log   = True
stacksize = 5000 # MB RAM per job smaller->higher priority (slurm only)
runmode = "HEJ"

## actual file to run
runfile    = os.path.dirname(os.path.realpath(__file__+"/../.."))+"/hejrun.py"

## grid storage (gfal) options
grid_input_dir  = "tst_grid/input"
grid_output_dir = "tst_grid/output"
# path of the HEJ tar on gfal (ignore the variable name)
grid_warmup_dir = "HEJ/HEJ.tar.gz"

warmup_base_dir = ""
production_base_dir = "/ResultsRunGrids"

use_gfal = True
gfaldir = "xroot://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/{0}/".format(getuser())
# set to None for environment gfal
# relies on "pro" symlink
cvmfs_gfal_location = "/cvmfs/dirac.egi.eu/dirac/pro/Linux_x86_64_glibc-2.17/bin/"
# or explicit name (might change in the future)
# cvmfs_gfal_location = "/cvmfs/dirac.egi.eu/dirac/v6r22p6/Linux_x86_64_glibc-2.17/bin/"

## Lhapdf config
cvmfs_lhapdf_location = "/cvmfs/sft.cern.ch/lcg/external/lhapdfsets/current"
use_cvmfs_lhapdf = True
lhapdf_grid_loc    = cvmfs_lhapdf_location # not needed with cvmfs ?
lhapdf_loc         = "/cvmfs/pheno.egi.eu/HEJ/LHAPDF/bin/lhapdf"
lhapdf_ignore_dirs = [] # Don't tar up all of LHAPDF if you don't want to
lhapdf_central_scale_only = True # Only tar up central [0000.dat] PDF sets
lhapdf             = lhapdf_grid_loc

## Rivet config
# TODO parse "use_custom_rivet" from config.yml
use_custom_rivet = False
grid_rivet_dir = "Wjets/Rivet/Rivet.tgz"

## Database name (database should be stored on a non-network disk)
dbname     = scratch_dir("hej_database")

## Finalisation and storage options
finalise_no_cores = 16
timeout = 60
# finalisation script, if "None" use native ./main.py man -[DA] -g
# if using a script, ./main.py will call script.do_finalise()
finalisation_script = "src/hej_finalise"
verbose_finalise = True
# Default folder for use only if finalisation script != None
# Gives a default destination for warmup files pulled whilst run is in progress
default_runfolder = None

## finalise.py-only parameters
finalise_runcards = None
finalise_prefix = None
recursive_finalise = None

## terminal output option
short_stats = True

## ARC parameters
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"
ce_listfile = "computing_elements.txt"
arcbase  = scratch_dir("arc_jobs.dat") # arc database
arc_submit_threads = 10 # Threads for use in arc submission. Raising this too high /may/ result
                        # in some job loss due to filesystem locks on arc jobs database.

## DIRAC parameters
dirac_name = "marian.heil"
DIRAC_BANNED_SITES = []
dirac_platform = "EL7"

## socket default parameters (for NNNLO Warmups)
server_host = "url.of.the.socket.server"
port = 9999
wait_time = 3600

## SLURM parameters
local_run_directory = base_dir("Wjets/")
warmup_queue = None
test_queue = None
production_queue = None
production_threads = 1
slurm_exclusive = True
slurm_exclude = []

## TMUX config
tmux_location= "tmux"

# LOCAL (this doesn't work)
desktop_list = ["d76"]
