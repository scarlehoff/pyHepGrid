#!/usr/bin/env python

import os, sys

# Runscript for ARC (modified from Tom's ganga.py)

LFNDIR     = "/grid/pheno/jmartinez"
RUNCARD    = sys.argv[1]
NNLOJET    = sys.argv[2]
NUMTHREADS = sys.argv[3]

# Set environment
os.environ["LFC_HOST"]         = "lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"] = "lfc"
os.environ["LFC_HOME"]         = LFNDIR
os.environ["LCG_GFAL_INFOSYS"] = "lcgbdii.gridpp.rl.ac.uk:2170"

lhapdf_path   = os.path.join(os.getcwd(), "LHAPDF", "lib")
gcc_libpath   = os.path.join(os.getcwd(), "gcc", "lib")
gcc_lib64path = os.path.join(os.getcwd(), "gcc", "lib64")

if "LD_LIBRARY_PATH" in os.environ:
  old_ldpath = os.environ["LD_LIBRARY_PATH"]
  os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s" % (gcc_libpath,gcc_lib64path,old_ldpath, lhapdf_path)
else:
  os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s" % (gcc_libpath,gcc_lib64path,lhapdf_path)

gcc_PATH = os.path.join(os.getcwd(),"gcc","bin")
old_PATH = os.environ["PATH"]
os.environ["PATH"] = "%s:%s" % (gcc_PATH,old_PATH)
lhapdf_sharepath   = os.path.join(os.getcwd(),"LHAPDF","share","LHAPDF")
os.environ['LHAPATH']         = lhapdf_sharepath
os.environ['LHA_DATA_PATH']   = lhapdf_sharepath
os.environ['OMP_STACKSIZE']   = "999999"
os.environ['OMP_NUM_THREADS'] = NUMTHREADS
os.environ['CC']              = "gcc"
os.environ['CXX']             = "g++"

LFN='lfn:'+LFNDIR
##################

# Bring files we are working with from Grid storage
os.system('lcg-cp lfn:input/local.tar.gz local.tar.gz')
os.system('lcg-cp lfn:input/'+NNLOJET+'.tar.gz NNLOJET.tar.gz')
os.system('tar -zxf local.tar.gz')
os.system('tar -zxf NNLOJET.tar.gz')
runcardtar = RUNCARD +'.tar.gz'
#status = os.system('lcg-cp lfn:runcards/'+runcardtar+' run.tar.gz')
#if status == 0:
#    print "Successfully extracted runcard from: "+runcardtar
#    os.system('tar -zxf run.tar.gz')
#    os.system('rm run.tar.gz')
#else:
#    print "ERROR: Failed to extract runcard from: "+runcardtar
#####################################################

# Prepare for running
os.system('chmod +x NNLOJET')
command = "./NNLOJET -run " + RUNCARD 

# For debugging
command +=' 2>&1 outfile.out;echo $LD_LIBRARY_PATH'
print "executed command: ", command
print "sys.argv: ", sys.argv
####################


# Run command
os.system(command)
print "Command successfully executed"
#############

# Organise everything into grid storage and cleanup after me
os.system('voms-proxy-info --all')
os.system('rm -rf LHAPDF/')
os.system('rm -rf runcards/')
os.system('rm -rf gcc/')
os.system('rm local.tar.gz')
os.system('rm NNLOJET.tar.gz')
os.system('rm TOT.*')
os.system('rm fort*')
config = RUNCARD+'-warm-'+NNLOJET
directory = 'warmup'
output = 'output'+config+'.tar.gz'
os.system('tar -czf '+output+' *') 
#os.system('lcg-cp $PWD/'+output+' '+SRM+directory+'/'+output)
os.system('ls')
cmd = 'lcg-cr --vo pheno -l lfn:'+directory+'/output'+config+'.tar.gz file:$PWD/output'+config+'.tar.gz'
os.system(cmd)
print(cmd)
