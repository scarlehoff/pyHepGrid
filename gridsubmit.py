#!/usr/bin/env ganga
import os,sys
import subprocess
import config as c

try:
    prodwarm = sys.argv[1]
except IndexError:
    prodwarm = 'production'

# warmup,production
prodwarm = 'production'
#prodwarm = 'warmup'
# Dirac,ARC,Local
#mode = 'ARC'
mode = 'Dirac'
#mode = 'Local'

#### WIZARD MODE/PRODWARM

mem = '100' # memory allocation per thread in Mb for ARC submissions

if prodwarm == 'warmup':
    multithread=True
    print "SETTING TO MULTITHREADED RUNNING"
    nruns = 1
else:
    multithread=False
    print "ASSUMING THIS IS A PRODUCTION RUN"
    print "SETTING TO SINGLE THREADED RUNNING"
    nruns = c.NRUNS

if multithread and mode != 'ARC':
    print "Error: multithreading is not supported for backends other than ARC"
    exit()


seedList = [str(i) for i in range(1,nruns+1)]


argList = []

runcards = os.listdir(c.RUNCARDS)

cmd = ['lfc-ls','output']

output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]


for seed in seedList:
    for r in runcards:
        if '~' not in r:
            arg = ' -run '+r+' -iseed '+seed
            checkarg = r+'-'+seed
            if checkarg not in output  or mode == 'Local':
                argList.append([arg,r,seed,multithread,c.LFNDIR,c.NNLOJETNAME])


print "Number of jobs: ", len(argList)

argSplit = ArgSplitter(args = argList)

HOME = os.getcwd()

j0 = Job()
if mode == 'Local': # slightly different syntax and can be used for debugging, does not generate data either
    j0.application = Executable(exe=File(os.path.join(HOME,'ganga_local.py')))
else:
    j0.application = Executable(exe=File(os.path.join(HOME,'ganga.py')))

if mode == 'ARC':
    j0.backend=ARC()
    j0.backend.CE='ce2.dur.scotgrid.ac.uk'
elif mode == 'Dirac' or mode == 'DIRAC':
    j0.backend=Dirac()
    j0.backend.settings['BannedSites']=["LCG.UKI-NORTHGRID-MAN-HEP.uk","LCG.EFDA-JET.xx"]#,"LCG.UKI-LT2-IC-HEP.uk"]
elif mode == 'Local':
    j0.backend=Local()
else:
    print "Invalid backend: ", mode
    exit()


if mode == 'ARC':
    j0.backend.requirements.other = ['(memory='+mem+')']
    if multithread:
        j0.backend.requirements.other += ['(count=16)','(countpernode=16)']
    

j0.splitter=argSplit
j0.submit()
