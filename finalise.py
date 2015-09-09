#!/usr/bin/env python 

import os,sys
import subprocess
import glob
import shutil

os.environ["LFC_HOST"]="lfc.grid.sara.nl"
os.environ["LCG_CATALOG_TYPE"]="lfc"
os.environ["LFC_HOME"]="/grid/pheno/morgan"
#os.environ["LCG_GFAL_INFOSYS"]="lcgbdii.gridpp.rl.ac.uk:2170"
os.environ["LD_LIBRARY_PATH"]="./LHAPDF/lib"
SRM='srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir/'

runcarddir = '/mt/home/morgan/NNLOJET/driver/grid'

runcards = os.listdir(runcarddir)


seedList = [str(i) for i in range(1,1000)]

cmd = ['lfc-ls','output']

output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]

processList = ['qq','qg','gq','qqb','qbq','gg','qbg','gqb','qbqb']

os.system('mkdir results/')
os.system('mkdir results/tmp/')
os.system('mkdir results/log/')
os.system('mkdir results/all')
for p in processList:
    newDir = 'results/'+p
    os.system('mkdir '+newDir)


os.chdir('results/log')
logcheck = glob.glob('*.log')

os.chdir('../tmp/')

currentdir = os.getcwd()

def getid(run):
    direct = os.path.join(runcarddir,run)
    f = open(direct,'r')
    text = f.readlines()
    id = text[1].strip()
    id = id[:3]
    f.close()
    return id

for run in runcards:
    for seed in seedList:
        name = 'output'+run+'-'+seed+'.tar.gz'
        runid = getid(run)
        checkname = runid+'-'+seed+'.log'
        output = [name] # HACK for now
        if name in output and checkname not in logcheck:
            status = 0
            print run,seed
            #command = 'lcg-cp lfn:output/'+name+' '+name
            command = 'lcg-cp '+SRM+'output/'+name+' '+name
            os.system(command)
            os.system('tar -xf '+name+' -C .')
            tmpfiles = os.listdir('.')
            if checkname in tmpfiles:
                direct = os.path.join('../log/',checkname)
                os.rename(checkname,direct)
                for f in tmpfiles:
                    splitname = f.split('.')
                    if splitname[0] in ['v5b','v5a','RRa','RRb','vRa']:
                        if len(splitname) == 5:
                            os.rename(f,'../all/'+f)
                        elif len(splitname) == 6 and splitname[-1] in processList:
                            direct = os.path.join('../',splitname[-1])
                            direct = os.path.join(direct,f)
                            os.rename(f,direct)
            else:
                status = 1

            if status != 0:
                print "deleting: ", run,seed
                os.system('lcg-del -a lfn:output/'+name) 
               
            shutil.rmtree(currentdir)
            os.mkdir(currentdir)
            os.chdir(currentdir)




