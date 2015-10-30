#!/bin/bash

#
# Usage: ./wizard.sh ARC/DIRAC/ARCPROD
#					 clean
#


configfile=config.py
firstrun=false

echo "Welcome to the Grid Wizard for NNLOJET"

########### FIRST RUN

if [ ! -f $configfile ]; then
######## CONFIG.PY
#
# This assumes you already have installed:
#	NNLOJET
#   LHAPDF
#   gcc
#

	echo "Let us create the config file for ganga"
	read -rsp "Press any key to continue... " -n1 adsgasf
	echo ""
	echo "#Config file for NNLOJET-ganga \n" > $configfile
	# NNLOJET and Runcards
	echo "Please, write the full path for the NNLOJET installation"
	read -p " > " nnlodir
	echo "NNLOJETDIR = \"$nnlodir\"" >> $configfile
	
	echo "Please write the directory for the NNLOJET runcards"
	read -p " > " runcarddir
	echo "RUNCARDS = "\"$runcarddir\" >> $configfile

	# LHAPDF
	lhadir=$(lhapdf-config --prefix) || echo "Couldn't find a LHAPDF installation"
	if [[ $lhadir == *"/"* ]] ; then
		echo "LHAPDF found at "$lhadir
	else
		echo "Please, write the full path for the LHAPDF installation"
		read -p " > " lhadir
	fi
	echo "LHAPDFDIR = \"$lhadir\"" >> $configfile

	# GCC
	echo "Please write the path to your gcc compiler"
	read -p " > " gccdir
	echo "GCCDIR = \"$gccdir\"" >> $configfile

	# lfc mkdirs
	echo "We are goint to create the following folder:"
	echo "/grid/pheno/"$USER
	read -p "Is that ok with you? " yn
	case $yn in
		[Yy]*) lfnname=$USER ;;
		*) read -p "Write the name of the folder: /grid/pheno/"lfnname ;;
	esac
	source bash_nnlojet
	lfc-mkdir /grid/pheno/$lfnname        
	lfc-mkdir /grid/pheno/$lfnname/input  
	lfc-mkdir /grid/pheno/$lfnname/output 
	lfc-mkdir /grid/pheno/$lfnname/warmup 
	lfndir=/grid/pheno/$lfnname
	echo "LFNDIR = "\"$lfndir\" >> $configfile

	echo "Congratulations, your config.py file is ready"

    ###### BASHRC
	echo "Let us add bash_$USER to .bashrc"
	cp bash_nnlojet bash_$USER
	currentfol=${PWD}
	cp ~/.bashrc ~/.bashrc-backup0
	bashNNLO=$currentfol/bash_$USER

	echo "export LFC_HOME=$lfndir" >> $bashNNLO
	echo "export PATH=$gccdir/bin:$lhadir/bin:\${PATH}" >> $bashNNLO
	# This is the piece Dirac source file messes up with?
	echo "export LD_LIBRARY_PATH=$gccdir/lib64:$lhadir/lib:\${LD_LIBRARY_PATH}" >> $bashNNLO
	echo "export NNLOJET_PATH=$nnlodir" >> $bashNNLO

	echo "if [ -f $bashNNLO ]; then" >> ~/.bashrc
	echo "	source $bashNNLO " >> ~/.bashrc
	echo "fi" >> ~/.bashrc

	mkdir runcards
	mkdir LHAPDF
	mkdir gcc

	echo "Done!"
	firstrun=true
fi

if [ ! -f ~/.gangarcDefault ]; then
	echo "Configuring Dirac"
	read -rsp "Press any key to continue... " -n1 adsgasf
	echo ""
    ##### Dirac Installation
		# To Do
		# Currently assumes dirac is already installed
	read -p "Path for dirac installation $HOME/" diracpath
	cp ~/.gangarc ~/.gangarcDefault
	source $HOME/$diracpath/bashrc
	env > $HOME/$diracpath/envfile
	ganga -g -o[Configuration]RUNTIME_PATH=GangaDirac
	sed -i "/\[Configuration]/a RUNTIME_PATH = GangaDirac" ~/.gangarc
	sed -i "/\[Dirac\]/a DiracEnvFile = $HOME/$diracpath/envfile" ~/.gangarc
	sed -i "/\[defaults_GridCommand\]/a info = dirac-proxy-info \ninit = dirac-proxy-init -g pheno_user" ~/.gangarc
	cp ~/.gangarc ~/.gangarcDirac
	cp ~/.gangarcDefault ~/.gangarc
	source ~/.bashrc
	firstrun=true
fi

if $firstrun; then 
	echo "First Run completed, exiting..."
	exit
fi
########################## First Run Ended

## Proxy Creation
echo "Create proxy"
cp ~/.gangarc ~/.gangarcbak0
while true; do
	if [[ $1 == "ARC" ]] || [[ $1 == "DIRAC" ]] || [[ $1 == "ARCPROD" ]]; then
		echo "Running with option: " $1
		mode=$1
	else
		# Supports:
		#	ARC     - warmup
		#   DIRAC   - production
		#   ARCPROD - production
		echo "Select option: "
		read -p "ARC/DIRAC/ARCPROD: " mode
	fi
	if [[ $mode == "ARC" ]]; then
		echo "Setting up Arc proxy"
		cp ~/.gangarcDefault ~/.gangarc
		arcproxy -S pheno -c validityPeriod=24h -c vomsACvalidityPeriod=24h
		prodwarm=warmup
		break
	elif [[ $mode == "ARCPROD" ]]; then
		echo "Setting up arc proxy"
		cp ~/.gangarcDefault ~/.gangarc
		arcproxy -S pheno -c validityPeriod=24h -c vomsACvalidityPeriod=24h
		prodwarm=production
		break
	elif [[ $mode == "DIRAC" ]]; then
		echo "Setting up Dirac proxy"
		cp ~/.gangarcDirac ~/.gangarc
		dirac-proxy-init -g pheno_user -M
		prodwarm=production
		break
	fi
done

## Modify gridsubmit
echo "Modifying gridsubmit ... "
# Is gribsumit in this directory?
gsub="gridsubmit.py"
if [[ -f $gsub ]]; then
	echo "gridsubmit.py found and saved as variable gsubmit"
	submitdir=${PWD}
else
	echo "Can't find gridsubmit.py, where is it stored?"
	read -p "Please, write full path: " submitdir
fi
cp $submitdir/gridsubmit.py $submitdir/tmpsubmit.py

sed -i "/WIZARD MODE/a mode = \"$mode\" " $submitdir/tmpsubmit.py
sed -i "/WIZARD MODE/a prodwarm = \"$prodwarm\" " $submitdir/tmpsubmit.py

## Initialisation
if [[ $prodwarm == "warmup" ]]; then
	allFlag="all"
else
	# Pull back grid file
	echo "Preparing the grid file for your production run"
	read -p "Write the name of the runcard > " runcardname
	lcg-cp lfn:warmup/output$runcardname.run-w.tar.gz tmp.tar.gz
	tar xfz tmp.tar.gz
 	rm tmp.tar.gz
fi

python initialise.py $allFlag

## Running Ganga
echo "Running ganga"

echo gsubmit=\"$submitdir/tmpsubmit.py\" > tmp.py
echo "print '\nexecfile(gsubmit) will run your mod. gridsubmit.py script\n'">> tmp.py
ganga -i tmp.py
rm tmp.py
rm tmpsubmit.py

### On exit
### Finalise / Delete ???

read -p "Do you want to look at the stdout of the job? " yn
if [[ $yn == "y" ]]; then
	read -p "Which one? " jobN
	read -p "Which subjob? " subjobN
fi

gunzip $HOME/gangadir/workspace/$USER/LocalXML/$jobN/$subjobN/output/stdout.gz
cat $HOME/gangadir/workspace/$USER/LocalXML/$jobN/$subjobN/output/stdout


