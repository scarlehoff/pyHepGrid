#!/bin/bash

#
# Usage: ./wizard.sh ARC/DIRAC/ARCPROD
#					 clean
#


configfile=configTemp.py
firstrun=false

echo "Welcome to the Grid Wizard for NNLOJET"

########### FIRST RUN

if [ ! -f "config.py" ]; then
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
	echo "#Config file for NNLOJET-ganga" > $configfile
	# NNLOJET and Runcards
	while true; do
		echo "Please, write the full path for the NNLOJET installation"
		read -p " > " nnlodir
		echo "NNLOJETDIR = \"$nnlodir\"" >> $configfile
		if [[ -f $nnlodir/NNLOJET.mk ]]; then
			echo "Directory found!"
			break
		else
			echo "Couldn't find the directory. Are you sure?"
		fi
	done
	
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
	while true; do
		echo "Please write the path to your gcc compiler"
		read -p " > " gccdir
		if [[ -f $gccdir/bin/gcc ]]; then
			echo "Directory found!"
			break
		else
			echo "Couldn't find the directory. Are you sure?"
		fi
	done
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
#	lfc-mkdir /grid/pheno/$lfnname        
#	lfc-mkdir /grid/pheno/$lfnname/input  
#	lfc-mkdir /grid/pheno/$lfnname/output 
#	lfc-mkdir /grid/pheno/$lfnname/warmup 
	lfndir=/grid/pheno/$lfnname
	echo "LFNDIR = "\"$lfndir\" >> $configfile

	echo "#THIS PART IS RUN DEPENDANT" >> $configfile
	echo "NUMRUNS = 10" >> $configfile
	echo "NUMTHREADS = 1" >> $configfile

	echo "RUNS = {"test.run":"NNLOJET"} # WIZARDRUNCARDS" >> $configfile

	mv $configfile config.py
	echo "Congratulations, your config.py file is ready"

    ###### BASHRC
	echo "Let us add bash_$USER to .bashrc"
	currentfol=${PWD}
	cp ~/.bashrc ~/.bashrc-backup0
	bashNNLO=$currentfol/bash_$USER

	echo "export LFC_HOME=$lfndir" >> $bashNNLO
	echo "export PATH=$gccdir/bin:$lhadir/bin:\${PATH}" >> $bashNNLO
	# This is the piece Dirac source file messes up with?
	echo "export LD_LIBRARY_PATH=$gccdir/lib64:$lhadir/lib:\${LD_LIBRARY_PATH}" >> $bashNNLO
	echo "export NNLOJET_PATH=$nnlodir" >> $bashNNLO

	# this lines can be created by the script as well...
	# they are the same for everyone
	echo "export LFC_HOST=\"lfc.grid.sara.nl\"" >> $bashNNLO
	echo "export LCG_CATALOG_TYPE=\"lfc\"" >> $bashNNLO
    echo "export CC=gcc" >> $bashNNLO
    echo "export CXX=g++" >> $bashNNLO
	echo "export OMP_STACKSIZE=999999" >> $bashNNLO
	echo "export OMP_NUM_THREADS=1 # default to 1 for now" >> $bashNNLO
	echo "export PATH=/cvmfs/ganga.cern.ch/Ganga/install/6.1.2-patch/bin:${PATH}" >> $bashNNLO

	echo "if [ -f $bashNNLO ]; then" >> ~/.bashrc
	echo "	source $bashNNLO " >> ~/.bashrc
	echo "fi" >> ~/.bashrc

	mkdir -p runcards
	mkdir -p LHAPDF
	mkdir -p gcc

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
	sed -i "/\[DIRAC\]/a DiracEnvFile = $HOME/$diracpath/envfile" ~/.gangarc
	sed -i "/\[defaults_GridCommand\]/a info = dirac-proxy-info \ninit = dirac-proxy-init -g pheno_user" ~/.gangarc
	cp ~/.gangarc ~/.gangarcDirac
	cp ~/.gangarcDefault ~/.gangarc
	if [[ -f bash_$USER ]]; then
		echo "export sourcedirac=\"$HOME/$diracpath/bashrc\"" >> bash_$USER
	else
		echo "You need to source the following line to use this script with dirac:"
		echo "          export sourcedirac=\"$HOME/$diracpath/bashrc\""
		echo "Please add this to your .bashrc file, thank you"
	fi
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
mode=$1
while true; do
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
		echo "Setting up Dirac proxy"]#
		source $sourcedirac #We assume .gangadirac was created with this script
		cp ~/.gangarcDirac ~/.gangarc
		dirac-proxy-init -g pheno_user -M
		prodwarm=production
		break
	elif [[ $mode == "CLEANDIRAC" ]]; then
		echo "Cleaning Dirac enviromental mess..."
		unset PYTHONUNBUFFERED
		unset PYTHONOPTIMIZE
		unset X509_VOMS_DIR
		unset DIRAC
		unset DIRACBIN
		unset DIRACSCRIPTS
		unset DIRACLIB
		unset TERMINFO
		unset RRD_DEFAULT_FONT
		unset PATH
		unset LD_LIBRARY_PATH
		unset DYLD_LIBRARY_PATH
		unset PYTHONPATH
		# Reset gangaDefault just in case
		# Path from clean session before sourcing .bash_profile
		export PATH=/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/sbin
		cp ~/.gangarcDefault ~/.gangarc
		# Juan:
		# For Durham gridui we also need to add another path to PATH, this makes $PATH system dependent and that _might_ be bad... 
		export PATH=$PATH:/usr/lib64/qt-3.3/bin
		source ~/.bash_profile
		# Clean duplicates 
		export PATH=$(echo "$PATH" | awk -v RS=':' -v ORS=":" '!a[$1]++')
		echo "... done!"
		exit
	elif [[ $mode == "DEBUG" ]]; then
		break
	else
		# Supports:
		#	ARC     - warmup
		#   DIRAC   - production
		#   ARCPROD - production
		echo "Select option: "
		read -p "ARC/DIRAC/ARCPROD: " mode
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

# Autogeneration of runcard variable in config.py
# Read the value of RUNCARDS into runcarddir
runcarddir=$(python -c "from config import RUNCARDS ; print RUNCARDS")
runcardvariable=""
# Remove swap files
rm -f $runcarddir/*.swp
for f in $(ls $runcarddir/*.run)
do
	runcardnm=$(basename "$f")
	echo "Adding $runcardnm"
	if [[ $runcardvariable != "" ]]; then
		runcardvariable=$runcardvariable,
	fi
	runcardvariable=$runcardvariable\"$runcardnm\":\"NNLOJET$(echo $runcardnm | tr -d .run)\"
	# Initialisation
	if [[ $prodwarm == "warmup" ]]; then
		allFlag="all"
	elif [[ $prodwarm == "production" ]]; then
		# Pull back grid file
		echo "Preparing the grid file for your production run"
#		read -p "Write the name of the runcard > " runcardname 
#		# Check whether the user wrote ".run", if he didn't, append .run
#		if [[ $runcardname == *".run" ]]; then
#			runcardnm=$runcardname
#		else
#			runcardnm=$runcardname.run
#		fi
		echo "Bringin back warmup files... for $runcardnm"
		lcg-cp lfn:warmup/output$runcardnm-w.tar.gz tmp.tar.gz
		echo "Untaring said files... "
		tar xfvz tmp.tar.gz
		echo "Removing tar"
		rm tmp.tar.gz
	#	mkdir -p tempFolder
	#	lcg-cp lfn:warmup/output$runcardnm.run-w.tar.gz tempFolder/tmp.tar.gz
	#	cd tempFolder
	#	tar xfz tmp.tar.gz
	#	# In principle we only want grid files?
	#	mv VJ* ../
	#	mv *.run ../
	#	mv *.log ../
	#	cd ..
	#	rm -rf tempFolder
	fi
	#read -p "Run initialise.py? (y/n) " yn
	#if [ $yn == "y" ]; then
		echo "Running initialise.py for runcard " $runcardnm
		# Check the folders runcards, LHAPDF, gcc exist
		mkdir -p runcards
		mkdir -p LHAPDF
		mkdir -p gcc
		python initialise.py $runcardnm $allFlag
	#fi
done

# Option to modify config.py here
# like, vim config.py

sed -i "/WIZARDRUNCARDS/cRUNS = { $runcardvariable } #WIZARDRUNCARDS" config.py
echo "Runcards added to config.py"

## Running Ganga
echo "Running ganga"
#read -p "Run in completely automatic mode? " yn
yn="n" # No by default

echo gsubmit=\"$submitdir/tmpsubmit.py\" > tmp.py
echo "print '\nexecfile(gsubmit) will run your mod. gridsubmit.py script\n'">> tmp.py
ganga -i tmp.py
rm tmp.py
#rm tmpsubmit.py

### On exit
### Finalise / Delete ???

if [[ $mode == "ARC" ]] || [[ $mode == "ARCDEFAULT" ]]; then
	read -p "Do you want to look at the stdout of the job? (y/n)" yn
	if [[ $yn == "y" ]]; then
		read -p "Which one? " jobN
		read -p "Which subjob? " subjobN
		gunzip $HOME/gangadir/workspace/$USER/LocalXML/$jobN/$subjobN/output/stdout.gz
		cat $HOME/gangadir/workspace/$USER/LocalXML/$jobN/$subjobN/output/stdout
	fi
# Needs DIRAC version
elif [[ $mode == "DIRAC" ]]; then
	echo "Restoring .gangarc..."
	cp ~/.gangarcDefault ~/.gangarc
	read -p "Run finalise.py? (y/n)" yn
	if [[ $yn == "y" ]]; then
		python finalise.py
	fi
fi



echo "Good Bye!"


