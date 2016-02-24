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
mode = 'ARC'
#mode = 'Dirac'
#mode = 'Local'

mcfmFlag=False # use MCFM on the grid
#### WIZARD MODE/PRODWARM


if prodwarm == 'warmup':
    multithread=True
    print "SETTING TO MULTITHREADED RUNNING"
    nruns = 1
    mem = '100' # memory allocation per thread in Mb for ARC submissions

else:
    multithread=False
    print "ASSUMING THIS IS A PRODUCTION RUN"
    print "SETTING TO SINGLE THREADED RUNNING"
    nruns = c.NUMRUNS
    mem = '1000' # memory allocation per thread in Mb for ARC submissions

if multithread and mode != 'ARC':
    print "Error: multithreading is not supported for backends other than ARC"
    exit()

if mcfmFlag:
    print "Submitting MCFM job(s)"
else:
    print "Submitting NNLOJET job(s)"

seedList = [str(i) for i in range(500,nruns+500)]

NUMTHREADS = str(c.NUMTHREADS)

argList = []

runcards = [r for r in os.listdir(c.RUNCARDS) if '~' not in r and ".swp" not in r and not r.startswith('.')]



for r in runcards:
    if r not in c.RUNS.keys():
        raise Exception('Runcard '+r+' not found in config.py')
    else:
        fullr = os.path.join(c.RUNCARDS,r)
        os.system('cp '+fullr+' .')
        tarfile = r+'.tar.gz'
        print "Writing "+r+" to grid storage"
        os.system("tar -czf "+tarfile+" "+r)
        os.system("lcg-del -a lfn:runcards/" + tarfile + ' --force')
        os.system("lcg-cr --vo pheno -l lfn:runcards/" + tarfile + " file:$PWD/" + tarfile)

if prodwarm != 'warmup':
    cmd = ['lfc-ls','output']
else:
    cmd = ['lfc-ls','warmup']

output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]

cmd = ['lfc-ls','input']

input  = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]

for r in runcards:
    if c.RUNS[r] not in input:
        raise Exception('Error: '+c.RUNS[r]+' not found in input lfs directory')

for seed in seedList:
    for r in runcards:
        if '~' not in r:
            if not mcfmFlag:
                arg = ' -run '+r+' -iseed '+seed
                if prodwarm != 'warmup':
                    checkarg = r+'-'+seed
                else:
                    checkarg = r+'-'+'w'
                if checkarg not in output  or mode == 'Local':
                    argList.append([arg,r,seed,multithread,c.LFNDIR,c.RUNS[r],NUMTHREADS])
            else:
                arg = r
                argList.append([arg,multithread,c.LFNDIR,c.RUNS[r],NUMTHREADS])


print "Number of jobs: ", len(argList)

argSplit = ArgSplitter(args = argList)

HOME = os.getcwd()

j0 = Job()
if mode == 'Local': # slightly different syntax and can be used for debugging, does not generate data either
    j0.application = Executable(exe=File(os.path.join(HOME,'ganga_local.py')))
elif mcfmFlag: # modified ganga.py for mcfm
    j0.application = Executable(exe=File(os.path.join(HOME,'ganga_mcfm.py')))
else:
    j0.application = Executable(exe=File(os.path.join(HOME,'ganga.py')))

if mode == 'ARC':
    j0.backend=ARC()
    j0.backend.CE='ce2.dur.scotgrid.ac.uk'
elif mode == 'Dirac' or mode == 'DIRAC':
    j0.backend=Dirac()
 #   j0.backend.settings['BannedSites']=["LCG.UKI-NORTHGRID-MAN-HEP.uk","LCG.EFDA-JET.xx"]#,"LCG.UKI-LT2-IC-HEP.uk"]
    j0.backend.settings['BannedSites']=["LCG.RAL-LCG2.uk"]
elif mode == 'Local':
    j0.backend=Local()
else:
    print "Invalid backend: ", mode
    exit()


if mode == 'ARC':
    j0.backend.requirements.other = ['(memory='+mem+')']
    if multithread:
        j0.backend.requirements.other += ['(count='+NUMTHREADS+')','(countpernode='+NUMTHREADS+')']

j0.inputfiles = [] 
#for r in runcards:  # send all the runcards as an input file now
#    fullr = os.path.join(c.RUNCARDS,r)
#    j0.inputfiles += [LocalFile(fullr)]

j0.splitter=argSplit
j0.submit()
