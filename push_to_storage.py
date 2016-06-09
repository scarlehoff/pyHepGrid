import os,sys,shutil
import config as c # dangerous but I'm a rebel

os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
os.environ["LFC_HOME"]= c.LFNDIR
os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"

try:
    fileid = sys.argv[1]
    newname = sys.argv[2]
except IndexError:
    raise Exception('Please provide a file name and a target name e.g. python push_to_storage.py grid.vBa Runcard.run')

newnameFull = 'output'+newname+'-w.tar.gz'
print 'uploading '+newnameFull+' to grid storage'
if fileid.endswith('tar.gz'):
    os.system('mv '+fileid+' '+newnameFull)
else:
    os.system('tar -czf '+newnameFull+' '+fileid)
os.system('lcg-cr --vo pheno -l lfn:warmup/'+newnameFull+' file:$PWD/'+newnameFull)
