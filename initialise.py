import os,sys,shutil

os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
os.environ["LFC_HOME"]="/grid/pheno/morgan"
os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"

NNLOJETDIR='/mt/home/morgan/NNLOJET'
LHAPDFDIR='/mt/home/morgan/NNLOJET/driver/LHAPDF'
RUNCARDS='/mt/home/morgan/NNLOJET/driver/grid' # changeme
GCCDIR='/mt/home/morgan/gcc-5.2.0/'

LFN='lfn:/grid/pheno/morgan/'
SRM='srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir'

HOME = os.getcwd()

shutil.copy(os.path.join(NNLOJETDIR,'driver','NNLOJET'),HOME)
shutil.rmtree(os.path.join(HOME,'runcards'))
shutil.copytree(RUNCARDS,os.path.join(HOME,'runcards'))

try:
    allFlag = sys.argv[1]
except IndexError:
    allFlag = 'None'

if allFlag == 'all':
    print "Initialising full local directory"
    shutil.rmtree(os.path.join(HOME,'LHAPDF'))
    shutil.rmtree(os.path.join(HOME,'gcc'))
    shutil.copytree(LHAPDFDIR,os.path.join(HOME,'LHAPDF'))
    shutil.copytree(GCCDIR,os.path.join(HOME,'gcc'))
    os.system('tar -czf local.tar.gz LHAPDF gcc')
    os.system('lcg-del -a lfn:input/local.tar.gz --force')
    os.system('lcg-cr --vo pheno -l lfn:input/local.tar.gz  file:$PWD/local.tar.gz')

    # TODO: add support for SRM when LFN is down
    #SRM
    #lcg-del $SRM/input/local.tar.gz
    #lcg-cp $PWD/local.tar.gz $SRM/input/local.tar.gz
    #GRID_FILE=$(lcg-rf $SRM/input/local.tar.gz -l $LFN/input/local.tar.gz)


print "Initialising NNLOJET"
os.system('lcg-del -a lfn:input/NNLOJET.tar.gz --force')
os.system('tar -czf NNLOJET.tar.gz NNLOJET *.RRa *.RRb *.vRa *.vRb *.vBa *.vBb runcards')
os.system('lcg-cr --vo pheno -l lfn:input/NNLOJET.tar.gz  file:$PWD/NNLOJET.tar.gz')
