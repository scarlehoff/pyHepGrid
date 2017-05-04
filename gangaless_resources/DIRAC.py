#!/usr/bin/env python

import os, sys

#lcg_cp = "gfal-cp"
#lcg_cr = "gfal-cp"
#lfn = "lfn://grid/pheno/jmartinez/"

# Define some utilites
def outputName(runcard, rname, seed):
    # This function must always be the same as the one in Backend.py
    out = "output" + runcard + "-" + rname + "-" + seed + ".tar.gz"
    return out

# Runscript for ARC (modified from Tom's ganga.py)

#
#  Read input arguments
#

LFNDIR  = "/grid/pheno/jmartinez"
RUNCARD = sys.argv[1]
RUNNAME = sys.argv[2]
SEED    = sys.argv[3] 

#
# Set environment
#

#os.environ["LFC_HOST"]         = "lfc.grid.sara.nl"
os.environ["LFC_HOST"]         = "lfc01.dur.scotgrid.ac.uk"
os.environ["LCG_CATALOG_TYPE"] = "lfc"
os.environ["LFC_HOME"]         = LFNDIR
os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"
os.environ['OMP_STACKSIZE']    = "999999"
os.environ['OMP_NUM_THREADS']  = "1"

lhapdf_path                    = os.path.join(os.getcwd(), "lhapdf", "lib")
lhapdf_sharepath               = os.path.join(os.getcwd() ,"lhapdf", "share", "LHAPDF")
os.environ['LHAPATH']          = lhapdf_sharepath
os.environ['LHA_DATA_PATH']    = lhapdf_sharepath

# Check for gcc in cvmfs
cvmfs_gcc_dir = '/cvmfs/pheno.egi.eu/compilers/GCC/5.2.0/'
if os.path.isdir(cvmfs_gcc_dir):
    gcclocal      = False
    gcc_libpath   = os.path.join(cvmfs_gcc_dir, "lib")
    gcc_lib64path = os.path.join(cvmfs_gcc_dir, "lib64")
    gcc_PATH      = os.path.join(cvmfs_gcc_dir, "bin")
else:
    gcclocal      = True
    gcc_libpath   = os.path.join(os.getcwd(), "gcc", "lib")
    gcc_lib64path = os.path.join(os.getcwd(), "gcc", "lib64")
    gcc_PATH      = os.path.join(os.getcwd(), "gcc", "bin")
    

# Populate LD_LIBRARY_PATH
old_ldpath                    = os.environ["LD_LIBRARY_PATH"]
os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s" % (gcc_libpath, gcc_lib64path, old_ldpath, lhapdf_path)

# Populate PATH
old_PATH           = os.environ["PATH"]
os.environ["PATH"] = "%s:%s" % (gcc_PATH,old_PATH)

gcc_libpath   = os.path.join(os.getcwd(), "gcc", "lib")
gcc_lib64path = os.path.join(os.getcwd(), "gcc", "lib64")

#
# Bringing files from Grid Storage
#

# Bring LHAPDF from Grid Storage
os.system("lcg-cp lfn:util/lhapdf.tar.gz lhapdf.tar.gz")
os.system("tar -zxf lhapdf.tar.gz")
# Bring gcc if needed
if gcclocal:
    os.system("lcg-cp lfn:util/gcc.tar.gz")
    os.system("tar zxf gcc.tar.gz")
# Bring NNLOJET and runcards
os.system("lcg-cp lfn:input/"+RUNCARD+RUNNAME+".tar.gz NNLOJET.tar.gz")
os.system("tar zxf NNLOJET.tar.gz")

#
# Run NNLOJET
#

# Prepare for running
os.system("ls")
os.system("chmod +x NNLOJET")
command = "./NNLOJET -run " + RUNCARD  + " -iseed " + SEED

# For debugging
command +=" 2>&1 outfile.out;echo $LD_LIBRARY_PATH"
print " > Executed command: ", command
print " > Sys.argv: ", sys.argv

# Run command
status = os.system(command)
if status == 0:
    print "Command successfully executed"
else:
    print "Something went wrong"
    os.system("cat outfile.out")

#
# Is cleanup necessary at all? Is this not done by ARC itself
# once we "timeout"?
#
os.system("voms-proxy-info --all")
os.system("rm -rf lhapdf/")
os.system("rm -rf runcards/")
if gcclocal: 
    os.system("rm -rf gcc/")
    os.system("rm -rf gcc.tar.gz")
os.system("rm lhapdf.tar.gz")
os.system('rm *.RRa *.RRb *.vBa *.vRa NNLOJET')
os.system("rm NNLOJET.tar.gz")
os.system("rm TOT.*")
os.system("rm fort*")
# Create warmup name
directory = "output"
output    = outputName(RUNCARD, RUNNAME, SEED)
os.system("tar -czf "+output+" *") 
# Copy to grid storage
cmd = "lcg-cr --vo pheno -l lfn:"+directory+"/"+output+" file:$PWD/"+output
print(cmd)
os.system(cmd)
os.system('ls')

# Bring cross section parser
try:
    os.system("lcg-cp lfn:util/pyCross.py pyCross.py")
    dir = os.listdir('.')
    print dir
    for i in dir:
        if "cross" in i:
            cmd = "python pyCross.py " + i + " " + RUNCARD
            os.system(cmd)
except:
    print("Some problem doing pycross")
