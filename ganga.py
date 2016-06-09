#!/usr/bin/env python 

"""A grid submission script using ganga"""

import sys,os
import subprocess

# Set environment variables

def writetofile(filename,string):
    f = open(filename,'w')
    f.write(string)
    f.close()

def readpdfset(runcard):
    f = open(runcard,'r')
    lines = f.readlines()
    pdf=lines[8].split('!')[0].strip()
    return pdf

LFNDIR=sys.argv[8]
NNLOJET=sys.argv[9]
NUMTHREADS=sys.argv[10]

if sys.argv[7] == 'True':
    warmup = True
elif sys.argv[7] == 'False':
    warmup = False
else:
    print "Arguments: ", sys.argv
    raise Exception('Invalid arguments passed to job')
cvmfs_gcc_dir = '/cvmfs/pheno.egi.eu/compilers/GCC/5.2.0/'
os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
os.environ["LFC_HOME"]=LFNDIR
os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"
debug = []
if os.path.isdir(cvmfs_gcc_dir):
    cvmfs_str = "Found pheno cvmfs gcc version, attempting to load this compiler..." 
    cvmfs = True
    gcc_libpath = os.path.join(cvmfs_gcc_dir, "lib")
    gcc_lib64path = os.path.join(cvmfs_gcc_dir, "lib64")
    gcc_PATH = os.path.join(cvmfs_gcc_dir,"bin")
else:
    cvmfs_str = "No pheno cvmfs found, falling back to grid storage gcc version"
    cvmfs = False
    gcc_libpath = os.path.join(os.getcwd(), "gcc", "lib")
    gcc_lib64path = os.path.join(os.getcwd(), "gcc", "lib64")
    gcc_PATH = os.path.join(os.getcwd(),"gcc","bin")
print cvmfs_str
debug.append(cvmfs_str)

lhapdf_path = os.path.join(os.getcwd(), "LHAPDF", "lib")
if "LD_LIBRARY_PATH" in os.environ:
  old_ldpath = os.environ["LD_LIBRARY_PATH"]
  os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s:%s" % (gcc_libpath,gcc_lib64path,old_ldpath, lhapdf_path)
else:
  os.environ["LD_LIBRARY_PATH"] = "%s:%s:%s" % (gcc_libpath,gcc_lib64path,lhapdf_path)

# PATH must always exist
old_PATH = os.environ["PATH"]
os.environ["PATH"] = "%s:%s" % (gcc_PATH,old_PATH)
lhapdf_sharepath = os.path.join(os.getcwd(),"LHAPDF","share","LHAPDF")
os.environ['LHAPATH']=lhapdf_sharepath
os.environ['LHA_DATA_PATH']=lhapdf_sharepath
os.environ['OMP_STACKSIZE']="999999"
if warmup:
    os.environ['OMP_NUM_THREADS']=NUMTHREADS
else:
    os.environ['OMP_NUM_THREADS']="1"
os.environ['CC']="gcc"
os.environ['CXX']="g++"

LFN='lfn:'+LFNDIR
SRM='srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir/'

# SRM
#os.system('lcg-cp '+SRM+'input/local.tar.gz $PWD/local.tar.gz' )
# LFN

if not cvmfs: # no cvmfs => pull compiler from grid storage
    os.system('lcg-cp lfn:input/gcc.tar.gz gcc.tar.gz')
    status = os.system('tar -zxf gcc.tar.gz')
    if status == 0:
        gcc_str = "Successfully extracted gcc from the grid storage" 
    else:
        gcc_str = "Failed to extract gcc from the grid storage"
    print gcc_str
    debug.append(gcc_str)
os.system('lcg-cp lfn:input/LHAPDF.tar.gz LHAPDF.tar.gz')
status = os.system('tar -zxf LHAPDF.tar.gz')
if status == 0:
    lhapdf_str = "Successfully extracted LHAPDF" 
else:
    lhapdf_str = "ERROR: Failed to extract LHAPDF from grid storage"
print lhapdf_str
debug.append(lhapdf_str)
os.system('lcg-cp lfn:input/'+NNLOJET+'.tar.gz NNLOJET.tar.gz')
status = os.system('tar -zxf NNLOJET.tar.gz')
if status == 0:
    nnlojet_str = "Successfully extracted NNLOJET"
else:
    nnlojet_str = "ERROR: Failed to extract NNLOJET"
print nnlojet_str
debug.append(nnlojet_str)
runcard = sys.argv[2]
runcardtar = runcard+'.tar.gz'
status = os.system('lcg-cp lfn:runcards/'+runcardtar+' run.tar.gz')
if status == 0:
    runcard_str = "Successfully extracted runcard from: "+runcardtar
    os.system('tar -zxf run.tar.gz')
    os.system('rm run.tar.gz')
    pdfset = readpdfset(runcard)
    pdfsettar = pdfset+'.tar.gz'
    home = os.getcwd()
    os.chdir(lhapdf_sharepath)
    os.system('lcg-cp lfn:input/'+pdfsettar+' pdf.tar.gz')
    status = os.system('tar -zxf pdf.tar.gz')
    os.system('rm pdf.tar.gz')
    os.chdir(home)
    if status == 0:
        pdf_str = "Successfully extracted pdf set from: "+pdfsettar
    else:
        pdf_str = "ERROR: Failed to extra pdf set from: "+pdfsettar
    print pdf_str
    debug.append(pdf_str)
else:
    runcard_str =  "ERROR: Failed to extract runcard from: "+runcardtar
print runcard_str
debug.append(runcard_str)
if not warmup: # attempt to find the grid files automatically
    warmuptar = 'output'+sys.argv[2]+'-w'+'.tar.gz'
    os.system('mkdir warmup')
    status = os.system('lcg-cp lfn:warmup/'+warmuptar+' warmup.tar.gz')
    if status == 0:
        os.system('tar -xf warmup.tar.gz -C warmup/')
        for gfile in ['RRa','RRb','vRa','vRb','vBa','vBb']:
            os.system('cp warmup/*.'+gfile+' .')
        warmup_str = "Successfully extracted warmup grids from: "+warmuptar
    else:
        warmup_str = "ERROR: Failed to extract warmup grids from: "+warmuptar
    print warmup_str
    debug.append(warmup_str)

os.system('chmod +x NNLOJET')

# COMMAND GOES HERE
#command = ''

#command += './NNLOJET'
#for var in sys.argv[1:5]:
#    command += ' '+var
#os.system('cp runcards/'+sys.argv[2]+' .') # copy runcard to working dir

# For debugging
#command +=';echo $LD_LIBRARY_PATH'
command = ['./NNLOJET']+sys.argv[1:5]
print "executed command: ", command
print "sys.argv: ", sys.argv
result = subprocess.Popen(command, stdout=subprocess.PIPE,stderr=subprocess.PIPE ).communicate()[0]
output = ''
for d in debug:
    output += d+'\n'
output += result
writetofile('outfile.out',output)
os.system('voms-proxy-info --all')
os.system('lfc-mkdir output')
# clear all unnecessary files for taring

if not warmup:
    os.system('rm *.RRa *.RRb *.vRa NNLOJET')
os.system('rm -rf LHAPDF/')
os.system('rm -rf runcards/')
os.system('rm -rf gcc/')
os.system('rm local.tar.gz')
os.system('rm NNLOJET.tar.gz')
os.system('rm TOT.*')
os.system('rm fort*')


# tar and send to grid storage

if warmup:
    config = sys.argv[2]+'-w'
    directory = 'warmup'
else:
    config = sys.argv[2]+'-'+sys.argv[4]
    directory = 'output'

output = 'output'+config+'.tar.gz'

os.system('tar -czf '+output+' *') 
os.system('lcg-cp $PWD/'+output+' '+SRM+directory+'/'+output)

#SRM 
#print 'lcg-cp $PWD/'+output+' '+SRM+directory+'/'+output
#os.system('lcg-rf '+SRM+directory+'/'+output+' -l '+LFN+'/'+directory+'/'+output) 
#print 'lcg-rf '+SRM+directory+'/'+output+' -l '+LFN+'/'+directory+'/'+output

#LFN

os.system('ls')
os.system('lcg-cr --vo pheno -l lfn:'+directory+'/output'+config+'.tar.gz file:$PWD/output'+config+'.tar.gz')
print 'lcg-cr --vo pheno -l lfn:'+directory+'/output'+config+'.tar.gz file:$PWD/output'+config+'.tar.gz'
