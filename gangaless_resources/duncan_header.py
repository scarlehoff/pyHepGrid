import subprocess as sp

##################################################
#                Helper Functions                #
# Can't use utilities due to circular imports :( #
##################################################
def get_cmd_output(*args,**kwargs):
    outbyt = sp.Popen(args, stdout=sp.PIPE,**kwargs).communicate()[0]
    return outbyt.decode("utf-8")

#
# Global Variables (default values)
# 
arcbase    = "/mt/home/dwalker/.arc/jobs.dat" # arc database
runcardDir = "/mt/home/dwalker/NNLOJET/driver/grid/"
NNLOJETdir = "/mt/home/dwalker/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 500
baseSeed   = 100
jobName    = "testjob"

#
# Grid config 
#
username = "dwalker"
lfndir   = "/grid/pheno/dwalker"
gsiftp   = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/generated/"
lhapdf_grid_loc = "input/" # util/ for Juan
lhapdf_loc = "lhapdf/LHAPDF-6.2.1" # lhapdf for Juan
lhapdf_ignore_dirs = ["doc", "examples", "config", "LHAPDF-6.2.1/share"]
#
# Grid and libraries. GCC requires version > 5
# Can be enabled by sourcing on login to gridui
# As per login message
#
gccdir = "/mt/home/dwalker/gcc-5.2.0"
# Use installed version of LHAPDF by default
lhapdf = get_cmd_output("lhapdf-config","--prefix")
# lhapdf     = "/mt/home/dwalker/LHAPDF"
 
#
# ARC parameters
#
ce_base = "ce2.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"


#
# NNLOJET Database Parameters
#
dbname     = "alljobs.dat"     
arctable   = "arcjobs"
diractable = "diracjobs"
dbfields   = ['jobid', 'date', 'runcard', 'runfolder', 'pathfolder', 'status', 'jobtype']

#
# Templates
# 

# # If a job is expected to run for long, use the following property (in minutes)
# "(wallTime  =    \"3 days\")" 
# it is also possible to specifiy the maximum cpu time instead (or 'as well')
# "(cpuTime = \"3 days\")"
# if nothing is used, the end system will decide what the maximum is
#

ARCSCRIPTDEFAULT = ["&",
        "(executable   = \"ARC.py\")",
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
        ]

ARCSCRIPTDEFAULTPRODUCTION = ["&",
        "(executable   = \"DIRAC.py\")",
        "(outputFiles  = (\"outfile.out\" \"\") )",
        "(stdout       = \"stdout\")",
        "(stderr       = \"stderr\")",
        "(gmlog        = \"testjob.log\")",
        "(memory       = \"100\")",
        ]

DIRACSCRIPTDEFAULT = [
        "JobName    = \"gridjob1\";",
        "Executable = \"DIRAC.py\";",
        "StdOutput  = \"StdOut\";",
        "StdError   = \"StdErr\";",
        "InputSandbox  = {\"DIRAC.py\"};",
        "OutputSandbox = {\"StdOut\",\"StdErr\"};",
        ]
