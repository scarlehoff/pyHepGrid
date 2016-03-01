#!/bin/bash

#
# Usage: ./wizard.sh ARC/DIRAC  # Runs the wizard for ARC/DIRAC
#					 ARC/DIRAC ganga # Just runs the proxy and then opens ganga
#					 CLEANDIRAC # Cleans Dirac's enviromental mess
#					 HELP  # prints help
echo "Welcome to the Grid Wizard for NNLOJET"

declare -a arrayGridFiles=('RRa' 'RRb' 'vRa' 'vRb' 'vBa' 'vBb')


# Function definitions
# 
#	firstRun() l.15
#   createArcProxy() l. 162
#   createDiracProxy() l. 170
#	cleanDirac() l. 176
#   createGridSubmit() l.207
#   addRunCards() l.226
#   finalise() l.281
# 
# Program start l.300

firstRun(){
	#	
	# This function will create:
	#		config.py
	#		bash_$USER
	#		gangaDirac 
	# This assumes you already have installed:
	#	NNLOJET
	#   LHAPDF
	#   gcc
	#
	configfile="configTmp.py"
	if [ ! -f config.py ]; then
		echo "Let us create the config file for ganga"
		read -rsp "Press any key to continue... " -n1 adsgasf
		echo ""
		echo "#Config file for NNLOJET-ganga" > $configfile
		# NNLOJET and Runcards
		while true; do
			echo "Please, write the full path for the NNLOJET installation"
			read -ep " > " nnlodir
			echo "NNLOJETDIR = \"$nnlodir\"" >> $configfile
			if [[ -f $nnlodir/NNLOJET.mk ]]; then
				echo "Directory found!"
				break
			else
				echo "Couldn't find the directory. Are you sure?"
			fi
		done
		
		echo "Please write the directory for the NNLOJET runcards"
		read -ep " > " runcarddir
		echo "RUNCARDS = "\"$runcarddir\" >> $configfile

		# LHAPDF
		lhadir=$(lhapdf-config --prefix) || echo "Couldn't find a LHAPDF installation"
		if [[ $lhadir == *"/"* ]] ; then
			echo "LHAPDF found at "$lhadir
		else
			echo "Please, write the full path for the LHAPDF installation"
			read -ep " > " lhadir
		fi
		echo "LHAPDFDIR = \"$lhadir\"" >> $configfile

		# GCC
		while true; do
			echo "Please write the path to your gcc compiler"
			read -ep " > " gccdir
			if [[ -f $gccdir/bin/gcc ]]; then
				echo "Directory found!"
				break
			else
				echo "Couldn't find the directory. Are you sure?"
			fi
		done
		echo "GCCDIR = \"$gccdir\"" >> $configfile

		###### BASHRC
		echo "Let us add bash_$USER to .bashrc"
		currentfol=${PWD}
		cp ~/.bashrc ~/.bashrc-backup0
		bashNNLO=$currentfol/bash_$USER

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
		echo "export PATH=$gccdir/bin:$lhadir/bin:/cvmfs/ganga.cern.ch/Ganga/install/6.1.2-patch/bin\:\$PATH:" >> $bashNNLO

		echo "if [ -f $bashNNLO ]; then" >> ~/.bashrc
		echo "	source $bashNNLO " >> ~/.bashrc
		echo "fi" >> ~/.bashrc

		mkdir -p runcards
		mkdir -p LHAPDF
		mkdir -p gcc

		# lfc mkdirs
		echo "We are going to create the following folders:"
		echo "/grid/pheno/"$USER
		read -p "Is that ok with you? (y/n) " yn
		case $yn in
			[Yy]*) lfnname=$USER ;;
			*) read -p "Write the name of the folder: /grid/pheno/"lfnname ;;
		esac
		source $bashNNLO
	 	lfc-mkdir /grid/pheno/$lfnname        
	 	lfc-mkdir /grid/pheno/$lfnname/input  
	 	lfc-mkdir /grid/pheno/$lfnname/output 
	 	lfc-mkdir /grid/pheno/$lfnname/warmup 
		lfndir=/grid/pheno/$lfnname
		echo "LFNDIR = "\"$lfndir\" >> $configfile

		echo "#THIS PART IS RUN DEPENDANT" >> $configfile
		echo "NUMRUNS = 10" >> $configfile
		echo "NUMTHREADS = 1" >> $configfile

		echo "RUNS = {\"test.run\":\"NNLOJET\"} # WIZARDRUNCARDS" >> $configfile

		mv $configfile config.py
		
		echo "export LFC_HOME=$lfndir" >> $bashNNLO
		echo "Congratulations, your config.py file is ready"


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
		while true; do
			read -ep "Path for dirac installation: " diracpath
			if [[ -f $diracpath/bashrc ]]; then
				echo "Directory/bashrc found!"
				break
			else
				echo "Couldn't find bashrc inside the given directory"
				echo "Are you sure it's correct?"
			fi
		done
		cp ~/.gangarc ~/.gangarcDefault
		source $diracpath/bashrc
		env > $diracpath/envfile
		ganga -g -o[Configuration]RUNTIME_PATH=GangaDirac
		sed -i "/\[Configuration]/a RUNTIME_PATH = GangaDirac" ~/.gangarc
		sed -i "/\[DIRAC\]/a DiracEnvFile = $diracpath/envfile" ~/.gangarc
		sed -i "/\[defaults_GridCommand\]/a info = dirac-proxy-info \ninit = dirac-proxy-init -g pheno_user" ~/.gangarc
		cp ~/.gangarc ~/.gangarcDirac
		cp ~/.gangarcDefault ~/.gangarc
		if [[ -f bash_$USER ]]; then
			echo "export sourcedirac=\"$diracpath/bashrc\"" >> bash_$USER
		else
			echo "You need to source the following line to use this script with dirac:"
			echo "          export sourcedirac=\"$diracpath/bashrc\""
			echo "Please add this to your .bashrc file, thank you"
		fi
		source ~/.bashrc
		firstrun=true
	fi
	return 0
}

createArcProxy() { 
	echo "Setting up Arc proxy"
	cp ~/.gangarcDefault ~/.gangarc
	if [[ -f .passpass ]]; then
		arcproxy -S pheno -c validityPeriod=24h -c vomsACvalidityPeriod=24h -p all=file:.passpass
	else
		arcproxy -S pheno -c validityPeriod=24h -c vomsACvalidityPeriod=24h 
	fi
	return 0
}

createDiracProxy() {
	source $sourcedirac #We assume .gangadirac was created with this script
	cp ~/.gangarcDirac ~/.gangarc
	if [[ -f .passpass ]]; then
		passDirac=$(cat .passpass)
		echo $passDirac | dirac-proxy-init -g pheno_user -M -p
	else
		dirac-proxy-init -g pheno_user -M
	fi
	return 0
}

cleanDirac() {
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
	# For Durham gridui we also need to add another path to PATH, this makes $PATH system dependent and that _might_ be really bad... 
	export PATH=$PATH:/usr/lib64/qt-3.3/bin
	source ~/.bash_profile
	# Clean duplicates 
	export PATH=$(echo "$PATH" | awk -v RS=':' -v ORS=":" '!a[$1]++')
	echo "... done!"
	return 0
}

createGridSubmit(){
	echo "Creating modified gridsubmit ... "
	# Is gribsumit in this directory?
	gsub="gridsubmit.py"
	if [[ -f $gsub ]]; then
		echo "gridsubmit.py found and saved as variable gsubmit"
		submitdir=${PWD}
	else
		echo "Cannot find gridsubmit.py, where is it stored?"
		read -ep "Please, write full path: " submitdir
	fi
	cp $submitdir/gridsubmit.py $submitdir/tmpsubmit.py

	sed -i "/WIZARD MODE/a mode = \"$mode\" " $submitdir/tmpsubmit.py
	sed -i "/WIZARD MODE/a prodwarm = \"$prodwarm\" " $submitdir/tmpsubmit.py
	return 0
}

addRunCards() {
	# Autogeneration of runcard variable in config.py
	# Read the value of RUNCARDS into runcarddir
	runcarddir=$(python -c "from config import RUNCARDS ; print RUNCARDS")
	runcardvariable=""
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
			echo "Bringin back warmup files... for $runcardnm"
			tfol=tempWarmupFolder
			echo "Creating temporary folder: " $tfol
			mkdir -p $tfol
			cd $tfol
			lcg-cp lfn:warmup/output$runcardnm-w.tar.gz tmp.tar.gz
			echo "Untaring said files... "
			tar xfvz tmp.tar.gz
			echo "Removing tar"
			rm tmp.tar.gz
			echo "Copying over the grid files..."
			for gf in ${arrayGridFiles[*]}; do
				cp *.$gf ../
			done
#			cp *.RRa ../
#			cp *.vRa ../
#			cp *.vRb ../
#			cp *.RRb ../
#			cp *.vBa ../
#			cp *.vBb ../
			echo "Removing temporary folder (rm -Ir $tfol)"
			cd ..
			rm -Ir $tfol
		fi
	done

	sed -i "/WIZARDRUNCARDS/cRUNS = { $runcardvariable } #WIZARDRUNCARDS" config.py
	echo "Runcards added to config.py"
	
	# Not sure whether this is the most efficient way
	for f in $(ls $runcarddir/*.run)
	do
		echo "Running initialise.py for runcard " $runcardnm
		# Check the folders runcards, LHAPDF, gcc exist
		mkdir -p runcards
		mkdir -p LHAPDF
		mkdir -p gcc

		if test -f initialise-gccfree.py; then
			echo "Looking for a (very specific) set of libraries"
			python initialise-gccfree.py $runcardnm $allFlag
		else
			python initialise.py $runcardnm $allFlag
		fi
	done

	echo "Removing the grid files if any..."
	# To do: make the "grid files" into a list
	for gf in ${arrayGridFiles[*]}; do
		rm *.$gf ../
	done

	return 0
}

finalise() {
	if [[ $mode == "ARC" ]]; then
		read -p "Do you want to look at the stdout of the job? (y/n)" yn
		if [[ $yn == "y" ]]; then
			read -p "Which one? " jobN
			read -p "Which subjob? " subjobN
			#
			#	Needs to look for where gangadir is located
			gunzip $HOME/.gangadir/workspace/$USER/LocalXML/$jobN/$subjobN/output/stdout.gz
			cat $HOME/.gangadir/workspace/$USER/LocalXML/$jobN/$subjobN/output/stdout
		fi
	elif [[ $mode == "DIRAC" ]]; then
		echo "Restoring .gangarc..."
		cp ~/.gangarcDefault ~/.gangarc
		read -p "Run finalise.py? (for modules only) (y/n)" yn
		if [[ $yn == "y" ]]; then
			python finalise.py
		fi
		####### And now let's treat the results
		####### for runcard in runcard/
		for full in $(ls runcards/*.run)
		do
			f=${full##*/}
			folderName=results_$f
			python $NNLOJET_PATH/driver/combine.py ./$folderName >> /dev/null
			month=$(date +%B)
			day=$(date +%d)
			fullPath=$HOME/ResultsRunGrid/$month/$day/
			mkdir -p $fullPath
			for n in {0..1000}
			do
				fullName=$fullPath/$folderName-n$n.tar.gz
				if test -f $fullName; then
					:
				else
					break
				fi
			done
			tar -zcf $fullName $folderName
		done


	fi
	######### clean up, very dangerous because it means removing stuff...
	######### so we are going to ask before every rm
	### some of them should be already be removed actually
	rm -i NNLOJET
	rm -i *.tar.gz
	rm -i tmpsubmit.py
	rm -i .run

}

launchHelp() {
	echo "This is the GRID NNLOJET wizard script"
	echo "The use of this script is very simple"
	echo "> First Run:"
	echo "To run the script for the first time write ./wizard.sh FIRSTRUN and follow the steps"
	echo "The script will configure all necessary enviromental variables and scripts to send"
	echo "warmup and production runs to the grid"
	echo "> Warmup Run:"
	echo "Write ./wizard.sh ARC to send jobs to ARC. The script will guide you through the creation of the proxy"
	echo "> Production Run:"
	echo "Write ./wizard.sh DIRAC to send jobs to Dirac. The script will guide you through the creation of the proxy"
	echo ""
	echo "For both Warmup and Production runs the script will look for runcards inside the folder you defined during FIRSTRUN"
	echo ""
	echo "> Open ganga terminal:"
	echo "In order to open a ganga terminal with DIRAC or ARC proxies write ./wizard [ARC/DIRAC] ganga"
	echo ""
	echo "Enjoy!"
}

mode=$1
while true; do
	if [[ $mode == "ARC" ]]; then
		createArcProxy
		prodwarm=warmup
		break
	elif [[ $mode == "DIRAC" ]]; then
		createDiracProxy
		prodwarm=production
		break
	elif [[ $mode == "CLEANDIRAC" ]]; then
		cleanDirac
		break
	elif [[ $mode == "HELP" ]]; then
		launchHelp
		exit
	elif [[ $mode == "FIRSTRUN" ]]; then
		firstRun
		exit
	else
		echo "Select option: "
		read -p "ARC/DIRAC: " mode
	fi
done
if [[ $2 == "ganga" ]]; then
	ganga
	exit
fi
if [[ $mode == "ARC" ]] || [[ $mode == "DIRAC" ]]; then
	addRunCards
	createGridSubmit
	echo gsubmit=\"$submitdir/tmpsubmit.py\" > tmp.py
	echo "print '\nexecfile(gsubmit) will run your mod. gridsubmit.py script\n'">> tmp.py
	ganga -i tmp.py
	rm tmp.py
	finalise
fi

echo "Good Bye!"
