import os,sys,shutil
import config as c # dangerous but I'm a rebel

os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
os.environ["LFC_HOME"]= c.LFNDIR
os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"

LFN='lfn:'+ c.LFNDIR
SRM='srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir'

HOME = os.getcwd()

shutil.copy(os.path.join(c.NNLOJETDIR,'driver','NNLOJET'),HOME)
shutil.rmtree(os.path.join(HOME,'runcards'))
shutil.copytree(c.RUNCARDS,os.path.join(HOME,'runcards'))

try:
    runcard = sys.argv[1]
except IndexError:
    raise Exception('Please provide a runcard name, e.g. python initialise.py TEST.run')

rundir = os.listdir(c.RUNCARDS)

if runcard not in rundir:
    raise Exception('Error: specified runcard not found in runcard directory')

try:
    allFlag = sys.argv[2]
except IndexError:
    allFlag = 'None'

if allFlag == 'all':
    print "Initialising full local directory"
    shutil.rmtree(os.path.join(HOME,'LHAPDF'))
    shutil.rmtree(os.path.join(HOME,'gcc'))
    shutil.copytree(c.LHAPDFDIR,os.path.join(HOME,'LHAPDF'))
    shutil.copytree(c.GCCDIR,os.path.join(HOME,'gcc'))
    os.system('tar -czf local.tar.gz LHAPDF gcc')
    os.system('lcg-del -a lfn:input/local.tar.gz --force')
    os.system('lcg-cr --vo pheno -l lfn:input/local.tar.gz  file:$PWD/local.tar.gz')

    # TODO: add support for SRM when LFN is down
    #SRM
    #lcg-del $SRM/input/local.tar.gz
    #lcg-cp $PWD/local.tar.gz $SRM/input/local.tar.gz
    #GRID_FILE=$(lcg-rf $SRM/input/local.tar.gz -l $LFN/input/local.tar.gz)


print "Initialising NNLOJET"
tarfile = c.RUNS[runcard] + ".tar.gz"
os.system("lcg-del -a lfn:input/" + tarfile + ' --force')
os.system("tar -czf " + tarfile  + " NNLOJET *.RRa *.RRb *.vRa *.vRb *.vBa *.vBb runcards")
os.system("lcg-cr --vo pheno -l lfn:input/" + tarfile + " file:$PWD/" + tarfile)
