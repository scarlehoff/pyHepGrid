#
# Global Variables (default values)
# 

arcbase    = "/mt/home/jmartinez/.arc/jobs.dat" # arc database
NNLOJETdir = "/mt/home/jmartinez/NNLOJET/"
NNLOJETexe = "NNLOJET"
warmupthr  = 16
producRun  = 500
baseSeed   = 400
jobName    = "gridjob"

#
# Grid and libraries 
#
lfndir     = "/grid/pheno/jmartinez"
gccdir     = "/mt/home/jmartinez/LIBRARIES/gcc-5.2.0"
lhapdf     = "/mt/home/jmartinez/LHAPDF"

#
# NNLOJET Database Parameters
#
dbname     = "NNLOJET_VFH.dat"     
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

