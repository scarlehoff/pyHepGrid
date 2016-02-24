#!/usr/bin/env python 

import os,sys
import subprocess
import glob
import shutil
import config as c

os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
#os.environ["LFC_HOME"]="/grid/pheno/morgan"
os.environ["LFC_HOME"] = c.LFNDIR
#os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"
#os.environ["LD_LIBRARY_PATH"]="./LHAPDF/Lib"
os.environ["LD_LIBRARY_PATH"] = c.LHAPDFDIR + "/lib"
SRM='srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir/'

#runcarddir = '/mt/home/morgan/NNLOJET/driver/grid'
runcarddir = c.RUNCARDS

runcards = os.listdir(runcarddir)

warm = ''
try:
    warm = sys.argv[1]
except IndexError:
    pass

if warm == 'warmup':
    warmup=True
else:
    warmup=False


if warmup:
    cmd = ['lfc-ls','warmup']
    seedList = ['w']
    processList = []
else:
    cmd = ['lfc-ls','output']
    seedList = [str(i) for i in range(1,10000)]
    processList = [] # no longer separate processes for modules

output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]

currentdir = os.getcwd()

def getid(run):
    direct = os.path.join(runcarddir,run)
    f = open(direct,'r')
    text = f.readlines()
    id = text[1].strip()
    id = id[:3]
    sproc = text[2].split('!')
    sproc = sproc[0].strip()
    f.close()
    return id,sproc

for run in runcards:
    targetdir = os.path.join(currentdir,'results_'+run)
    os.system('mkdir '+targetdir)
    if not warmup:
        for subdir in ['tmp','log']+processList:
            newdir = os.path.join(targetdir,subdir)
            os.system('mkdir '+newdir)
    else:
        newdir = os.path.join(targetdir,'tmp')
        os.system('mkdir '+newdir)         

    if not warmup:
        newdir = os.path.join(targetdir,'log')
    else:
        newdir = targetdir
    os.chdir(newdir)
    logcheck = glob.glob('*.log')
    tmpdir = os.path.join(targetdir,'tmp')
    os.chdir(tmpdir)

    for seed in seedList:
        name = 'output'+run+'-'+seed+'.tar.gz'
        runid,sproc = getid(run)
        # for now ZJ is a hack
        if warmup:
            checkname = sproc+'.'+runid+'.'+'s1.log'
        else:
            checkname = sproc+'.'+runid+'.s'+seed+'.log'
#        output = [name] # HACK for now
        if name in output and checkname not in logcheck:
            status = 0
            print run,seed
            if warmup:
                command = 'lcg-cp lfn:warmup/'+name+' '+name
            else:
                command = 'lcg-cp lfn:output/'+name+' '+name
            os.system(command)
            os.system('tar -xf '+name+' -C .')
            tmpfiles = os.listdir('.')
            
            if checkname in tmpfiles:
                if warmup:
                    direct = os.path.join('../',checkname)
                else:
                    direct = os.path.join('../log/',checkname)
                os.rename(checkname,direct)
                for f in tmpfiles:
                    splitname = f.split('.')
                    if splitname[-1] == 'dat':
                        os.rename(f,'../'+f)
                    #if splitname[0] in ['v5b','v5a','RRa','RRb','vRa','vBa']:
                    #    if len(splitname) == 5:
                    #        os.rename(f,'../all/'+f)
                    #    elif len(splitname) == 6 and splitname[-1] in processList:
                    #        direct = os.path.join('../',splitname[-1])
                    #        direct = os.path.join(direct,f)
                    #        os.rename(f,direct)
                    elif splitname[-1] in ['RRa','RRb','vRa','vRb','txt'] and warmup:
                        os.rename(f,'../'+f)
            else:
                status = 1

            if status != 0:
                print "deleting: ", run,seed
                os.system('lcg-del -a lfn:output/'+name) 
            shutil.rmtree(tmpdir)
            os.mkdir(tmpdir)
            os.chdir(tmpdir)




