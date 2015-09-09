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

runs = sys.argv[1:]

if runs == []:
    raise Exception('Please provide run names to delete')

seedList = [str(i) for i in range(1,1000)]

cmd = ['lfc-ls','output']

output = subprocess.Popen( cmd, stdout=subprocess.PIPE ).communicate()[0]



for run in runs:
    for seed in seedList:
        name = 'output'+run+'-'+seed+'.tar.gz'
        if name in output:
            print "deleting: ", run,seed
            os.system('lcg-del -a lfn:output/'+name) 
               
            


