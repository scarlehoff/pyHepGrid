import os,sys,shutil
import config as c

os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
os.environ["LFC_HOME"]= c.LFNDIR
os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"

LFN='lfn:'+ c.LFNDIR
SRM='srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir'

LHAPDF_website = 'http://www.hepforge.org/archive/lhapdf/pdfsets/6.1/'
HOME = os.getcwd()

try:
    extraFlag = sys.argv[2]
except IndexError:
    extraFlag = 'None'

def readpdfset(runcard):
    fullpath = os.path.join(c.RUNCARDS,runcard)
    f = open(fullpath,'r')
    lines = f.readlines()
    pdf=lines[8].split('!')[0].strip()
    return pdf

shutil.copy(os.path.join(c.NNLOJETDIR,'driver','NNLOJET'),HOME)
shutil.rmtree(os.path.join(HOME,'runcards'))
shutil.copytree(c.RUNCARDS,os.path.join(HOME,'runcards'))
if extraFlag == 'mcfm':
    shutil.copy(os.path.join(c.MCFMDIR,'Bin','mcfm_omp'),HOME)
    shutil.copy(os.path.join(c.MCFMDIR,'Bin','process.DAT'),HOME)
    shutil.rmtree(os.path.join(HOME,'Pdfdata'))
    shutil.copytree(os.path.join(c.MCFMDIR,'Bin','Pdfdata'),os.path.join(HOME,'Pdfdata'))


try:
    runcard = sys.argv[1]
except IndexError:
    raise Exception('Please provide a runcard name, e.g. python initialise.py TEST.run')

#rundir = os.listdir(c.RUNCARDS)
# Skipping hidden files
rundir = [rcard for rcard in os.listdir(c.RUNCARDS) if not rcard.startswith('.')]

if runcard not in rundir:
    raise Exception('Error: specified runcard not found in runcard directory')

pdf = readpdfset(runcard)
sharepath = os.path.join(c.LHAPDFDIR,'share','LHAPDF',pdf)
try:
    shutil.rmtree(os.path.join(HOME,pdf))
except OSError:
    pass
try:
    shutil.copytree(sharepath,os.path.join(HOME,pdf))
    os.system('tar -cpzf '+pdf+'.tar.gz '+pdf)
except OSError:  # pdf set does not exist locally, try to pull it straight from the LHAPDF website...
    os.system('wget '+LHAPDF_website+pdf+'.tar.gz')

print 'copying '+pdf+' to grid storage'
os.system('lcg-del -a lfn:input/'+pdf+'.tar.gz --force')
os.system('lcg-cr --vo pheno -l lfn:input/'+pdf+'.tar.gz file:$PWD/'+pdf+'.tar.gz')

if extraFlag == 'all':
    print "Initialising full local directory"
    try:
        shutil.rmtree(os.path.join(HOME,'LHAPDF'))
        shutil.rmtree(os.path.join(HOME,'gcc'))
    except OSError:
        pass
    shutil.copytree(c.LHAPDFDIR,os.path.join(HOME,'LHAPDF'))
    shutil.copytree(c.GCCDIR,os.path.join(HOME,'gcc'))
    sharepath = os.path.join(HOME,'LHAPDF','share','LHAPDF')
    # clean out pdf sets, note rm -rf is dangerous!! There are two files in this directory LHAPDF needs to run and helpfully gives no error if they're missing -_-
    os.chdir(sharepath)
    dirList = [f for f in os.listdir('.') if os.path.isdir(f)]
    for d in dirList:
        shutil.rmtree(d)
    os.chdir(HOME)
    os.system('tar -czf gcc.tar.gz gcc')
    os.system('tar -czf LHAPDF.tar.gz LHAPDF')    
    os.system('lcg-del -a lfn:input/gcc.tar.gz --force')
    os.system('lcg-del -a lfn:input/LHAPDF.tar.gz --force')
    os.system('lcg-cr --vo pheno -l lfn:input/gcc.tar.gz  file:$PWD/gcc.tar.gz')
    os.system('lcg-cr --vo pheno -l lfn:input/LHAPDF.tar.gz  file:$PWD/LHAPDF.tar.gz')
    

if extraFlag == 'mcfm':
    print "Initialising MCFM"
    tarfile = c.RUNS[runcard] + ".tar.gz"
    os.system("lcg-del -a lfn:input/" + tarfile + ' --force')
    os.system("tar -czf " + tarfile + " mcfm_omp process.DAT Pdfdata")
    
else:
    print "Initialising NNLOJET"
    tarfile = c.RUNS[runcard] + ".tar.gz"
    os.system("lcg-del -a lfn:input/" + tarfile + ' --force')
    os.system("tar -czf " + tarfile  + " NNLOJET runcards")

os.system("lcg-cr --vo pheno -l lfn:input/" + tarfile + " file:$PWD/" + tarfile)
print "lcg-cr --vo pheno -l lfn:input/" + tarfile + " file:$PWD/" + tarfile

    # TODO: add support for SRM when LFN is down
    #SRM
    #lcg-del $SRM/input/local.tar.gz
    #lcg-cp $PWD/local.tar.gz $SRM/input/local.tar.gz
    #GRID_FILE=$(lcg-rf $SRM/input/local.tar.gz -l $LFN/input/local.tar.gz)
