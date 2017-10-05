#
# Global Variables (default values)
# 

arcbase    = "/mt/home/jmartinez/.arc/jobs.dat" # arc database
NNLOJETdir = "/mt/home/jmartinez/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 400
baseSeed   = 400
jobName    = "gridjob"

#
# Grid config 
#
username = "jmartinez"
lfndir = "/grid/pheno/jmartinez"
gsiftp = "gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/generated/"

#
# Grid and libraries 
#
gccdir     = "/mt/home/jmartinez/LIBRARIES/gcc-5.2.0"
lhapdf     = "/mt/home/jmartinez/LHAPDF"

#
# ARC parameters
#
ce_base = "ce3.dur.scotgrid.ac.uk"
ce_test = "ce-test.dur.scotgrid.ac.uk"


#
# NNLOJET Database Parameters
#
dbname     = "NNLOJET_october.dat"     
arctable   = "arcjobs"
diractable = "diracjobs"
dbfields   = ['jobid', 'date', 'runcard', 'runfolder', 'pathfolder', 'status']

#
# Templates
# 
ARCSCRIPTDEFAULT = ["&",
        "(executable   = \"ARC.py\")",
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

