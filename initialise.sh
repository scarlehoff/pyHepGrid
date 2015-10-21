#!/bin/bash

export LFC_HOST=lfc.grid.sara.nl
export LCG_CATALOG_TYPE=lfc
export LFC_HOME=/grid/pheno/morgan
export LCG_GFAL_INFOSYS=lcgbdii.gridpp.rl.ac.uk:2170

NNLOJETDIR=/mt/home/morgan/NNLOJET
LHAPDFDIR=/mt/home/morgan/NNLOJET/driver/LHAPDF
RUNCARDS=/mt/home/morgan/NNLOJET/driver/grid # changeme

LFN=lfn:/grid/pheno/morgan/
SRM=srm://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/morgan_dir



cp $NNLOJETDIR/driver/NNLOJET .
cp -r $LHAPDFDIR .
cp -r $RUNCARDS .

lcg-del -a lfn:input/local.tar.gz
lcg-del $SRM/input/local.tar.gz
tar -czf local.tar.gz NNLOJET *.RRa *.RRb *.vRa *.vRb *.vBa *.vBb LHAPDF grid

#LFN
lcg-cr --vo pheno -l lfn:input/local.tar.gz  file:$PWD/local.tar.gz

#SRM
#lcg-cp $PWD/local.tar.gz $SRM/input/local.tar.gz
#GRID_FILE=$(lcg-rf $SRM/input/local.tar.gz -l $LFN/input/local.tar.gz)