[![DOI](https://zenodo.org/badge/172027514.svg)](https://zenodo.org/badge/latestdoi/172027514)



(Incomplete, and always will be. The grid is a mysterious thing...)

# CONTENTS
1)  INITIAL SETUP
2)  LFN/GFAL SETUP
3)  GRID SCRIPTS SETUP 
4)  PROXY SETUP
5)  GRID SCRIPTS USAGE
6)  FINALISING RESULTS
7)  WORKFLOW
8)  RUNCARD.PY FILES DETAILS
9) DIRAC
10) MONITORING WEBSITES
11) GRID STORAGE MANAGEMENT
12) DISTRIBUTED WARMUP
13) HAMILTON QUEUES

# 1) INITIAL SETUP

Follow certificate setup as per Jeppe's tutorial here
[here](https://www.ippp.dur.ac.uk/~andersen/GridTutorial/gridtutorial.html "Grid Tutorial")

Make a careful note of passwords (can get confusing). Don't use passwords used elsewhere in case you
want to automate proxy renewal (like me and Juan)

# 2) LFN SETUP

This is now somewhat deprecated, and users should move to the fully supported GFAL/DPM protocols.

put this into your bashrc:
```
export CC=gcc
export XX=g++
export LCG_CATALOG_TYPE="lfc"
export LFC_HOME=/grid/pheno/<LFN_NAME>
export LFC_HOST="lfc01.dur.scotgrid.ac.uk"
source /opt/rh/devtoolset-4/enable              # Default gcc is version 4 and this DOES NOT WORK!
```
then
```
source ~/.bashrc

create lfndir
lfc-mkdir /grid/pheno/<LFN_NAME>

lfc-mkdir input
lfc-mkdir output
lfc-mkdir warmup
```
should be able to see the following using `lfc-ls`
```
input
output
warmup
```
generate more directories in analogy. A nice wrapper is included as described in the GRID
   STORAGE MANAGEMENT section later on.

To use GFAL instead, one needs to perform the above setup, except using
```gfal-mkdir gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/<directoryname>```

The folder setup that the GFAL setup uses should be the same as for LFN, so you will need an input, output and warmup folder as normal

GFAL should be much more stable and quick than the LFN, though I've not tested it through DIRAC yet. It has been tested for both production and warmup (plus ini) with Durham and Glasgow, so it just depends on whether Gfal is set up on Dirac systems

It works for ARC, and you can view the files in a web browser at
```
se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/
```
To enable, toggle use_gfal to True in your header file. This will also change over the finalise.py to use the gfal system where LFN files cannot be seen.

TODO:update this section to include more on GFAL storage management

# 3) GRID SCRIPTS SETUP

create own header, copy template header

adjust personal header

adjust headername in general header.py file (only add the name of your header file to top)

create folder for runcard storage and add to header file (e.g `NNLOJET/driver/grid/`)

add yourname_header.py file and altered header.py file to the repo, and COMMIT

generate runcard.py following template_runcard.py example

# 4) PROXY SETUP

By default, jobs will fail if the proxy ends before they finish running, so it's a good idea
to keep them synced with new proxies as you need:

By hand:
- Create new proxy
``` 
arcproxy -S pheno -N -c validityPeriod=24h -c vomsACvalidityPeriod=24h
```
- Sync current jobs with latest proxy
```
 arcsync -c ce1.dur.scotgrid.ac.uk
 arcrenew -a
```

Automated (set & forget):

I've added some proxy automation scripts to the repo in `gangaless_resources/proxy_renewal/`

To get these working, simply add your certificate password in to .script2.exp (plaintext I
   know, so it's bad...)

Make sure `.proxy.sh` is set up for your user (directories should point to your gangaless resources)

Run by hand to check (shouldn't need your password)

Then set up `.proxy.sh` to run as a cron job at least once per day (I suggest 2x in case of failure)

# 5) GRID SCRIPTS USAGE

## Initialise libraries [LHAPDF,(OPENLOOPS?)]
```
python3 main.py ini -L
```

## Initialise runcard
```
python3 main ini runcard.py -Bn -w warmup.file
 -B is production in arc -D in dirac
 -A is warmup in arc
```
## Run jobs using:
```
python3 main.py run <runcard.py> -A # ARC WARMUP
```
```
python3 main.py run <runcard.py> -B # ARC PRODUCTION
```
```
python3 main.py run <runcard.py> -D # DIRAC PRODUCTION
```
## Manage the jobs/view the job database with:
```
python3 main.py man <runcard.py> -(A/B/D)
```
include the flags:
```
     -S/-s for job stats
     -p to print the stdout of the job (selects last job of set if production)
     -P to print the job log file (selects last job of set if production)
     -I/-i for job info
```

For running anything on the grid, the help text in main.py (`python3 main.py -h`) is useful for
hidden options that aren't all necessarily documented(!)
       -- These features include warmup continuation, getting warmup data from running warmup jobs,
           initialising with your own warmup from elsewhere, database management stuff, local
     runcard testing

When running, the python script nnlorun.py is sent to the run location. This script then runs
     automatically, and pulls all of the appropriate files from grid storage (exe, runcard
     (warmups)). It then runs the executable in the appropriate mode, before tarring up output files and
     sending them back to the grid storage.

# 6) FINALISING RESULTS

-> The process of pulling the production results from grid storage to the gridui

-> You have a choice of setups for this (or you can implement your own)

=> DEFAULT SETUP
   By default, `./main.py` ships a "--get_data" script that allows you to retrieve jobs looking
   at the database.
```
       ./main.py man -A --get_data
``` 
   For ARC runs (either production or warmup)
   or
```
       ./main.py man -D --get_data
```
   for DIRAC runs.

   The script will then ask you which database entry do you want to retrieve and will put the contents in the folders defined in your header.
        warmup_base_dir/date/of/job/RUNNAME
   for warmups or
        production_base_dir/date/of/job/RUNNAME
   for production.

   For instance, let's supposse you sent 4000 production runs to Dirac on the 1st of March
   and this job's entry is 14, you can do
```
./main.py man -D -g -j 14
```
   and it will download all .dat and .log files to
       warmup_base_dir/March/1/RUNNAME

=> CUSTOM SETUPS
   For your own custom setup, you just need to write a finalisation script which exposes a
   function called do_finalise(). This function does the pulling from the grid storage. You
   then set the variable finalisation_script to the name of your script (without the .py suffix)
   Happy days!

   For example:
   ```
       ./finalise.py
   ```
   set finalisation_script = "finalise" in your header and
   ```
       ./main.py man --get_data
   ```
   This will find all of the runcards specified at the top of finalise_runcard.py (or
   other as specified in finalise_runcards) and pull all of the data it can find for them
   from the grid storage.
   The output will be stored in production_base_dir (as set in the header) with one folder
   for each set of runs, and the prefix as set in finalise_prefix. Corrupted data in the
   grid storage will be deleted.

 `[./src/finalise.py, ./main.py man --get_data]`

# 7) NORMAL WORKFLOW

0) Make sure you have a working proxy
1) initialise warmup runcard
2) run warmup runcard
3) switch warmup -> production in runcard
4) When warmup complete, reinitialise runcard for production
5) run production runcard as many times as you like w/ different seeds
6) pull down the results (finalisation)

# 8) RUNCARD.PY FILES DETAILS
-> Include a dictionary of all of the runcards you want to submit/initialise/manage, along with an
   identification tag that you can use for local accounting
-> template_runcard.py is the canonical example
-> Must be valid python to be used
-> Has a functionality whereby you can override any parameters in your header file for by specifying
   them in the runcard file. So you can e.g specify a different submission location for specific
   runs/ give different starting seeds/ numbers of production runs.
-> You can even link/import functions to e.g dynamically find the best submission location

# 9) DIRAC

Installing Dirac is quite easy nowadays! This information comes
directly from https://www.gridpp.ac.uk/wiki/Quick_Guide_to_Dirac.
Running all commands will install 
```
dirac version $DIRAC_VERSION
```
 to `$HOME/dirac_ui`.
You can change this by modifying the variable DIRAC_FOLDER
```
DIRAC_FOLDER="~/dirac_ui"
DIRAC_VERSION="-r v6r20p5 -i 27 -g v14r1"

mkdir $DIRAC_FOLDER
cd $DIRAC_FOLDER
wget -np -O dirac-install https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/dirac-install.py
chmod u+x dirac-install
./dirac-install $DIRAC_VERSION
source $DIRAC_FOLDER/bashrc # this is not your .bashrc but Dirac's bashrc, see note below
dirac-proxy-init -x  ## Here you will need to give your grid certificate password
dirac-configure -F -S GridPP -C dips://dirac01.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I
dirac-proxy-init -g pheno_user -M
```

### Note:
You need to `source $DIRAC_FOLDER/bashrc` every time you want to use dirac.
Running the following command will put the "source" in your own .bashrc
```
echo "source $DIRAC_FOLDER/bashrc" >> $HOME/.bashrc
```
## ALTERNATIVE

Instead of sourcing the dirac bashrc as above, you can alternatively add $DIRAC_FOLDER/scripts/
to your PATH variable directly in your bashrc. It all seems to work ok with python 2.6.6

# 10) DURHAM ARC MONITORING WEBSITE
https://grafana.dur.scotgrid.ac.uk/dashboard/db/uki-scotgrid-durham-grid-queues?refresh=15s&orgId=1

DIRAC MONITORING WEBSITE
https://dirac.gridpp.ac.uk:8443/DIRAC/

UK ARC INFO
`./get_site_info.py`

# 11) GRID STORAGE MANAGEMENT
I've written a wrapper to the gfal commands in order to simplify manual navigation of
the DPM filesystems. 

A frequently updated version can be found [here](https://github.com/DWalker487/dpm-manager "dpm_manager").

# 12) DISTRIBUTED WARMUP 

- compile NNLOJET with sockets=true to enable distribution
- set up server NNLOJET/driver/bin/vegas_socket.py 
```
./vegas_socket.py -p PORT -N NO_CLIENTS -w WAIT
```
 -> 1 unique port for each separate NNLOJET run going on [rough range 7000-9999]
 -> wait parameter is time limit (secs) from first client registering with the server before starting without all clients present. If not set, it will wait <forever>! Jobs die if they try and join after this wait limit.
    NB. [the wait parameter can't be used for distribution elsewhere -> it relies on nnlorun.py (grid script) in jobscripts to kill run]
 -> The server is automatically killed on finish by nnlorun.py (grid script)
- In the grid runcard set sockets_active >= N_clients, port = port number looking for.
 -> Will send sockets_active as # of jobs. If this is more than the # of sockets looked for by the server, any excess will be killed as son as they start running
- NNLOJET runcard # events is the total # of events (to be divided by amongst the clients)
- The server must set up on the same gridui as defined in the header parameter server_host. Otherwise the jobs will never be found by the running job.

# 13) HAMILTON QUEUES

- There are multiple queues I suggest using on the HAMILTON cluster:

 ### par6.q
    16 cores per node [set warmupthr = 16]
    No known job # limit, so you can chain as many nodes as you like to turbocharge warmups
    3 day job time limit
 ### par7.q
    24 cores per node [set warmupthr = 24]
    # jobs limited to 12, so you can use a maximum of 12*24 cores at a given time
    3 day job time limit
 ### openmp7.q               **NOT RECOMMENDED**
    58 cores total - this is tiny, so I would recommend par7 or par6
    The number of jobs limited to 12
    No time limit
    Often in competition with other jobs, so not great for sockets
- Use NNLOJET ludicrous mode if possible - it will have a reasonable speedup when using 16-24 core
  nodes in warmups
- Current monitoring info can be found using the sfree command, which gives the # of cores in use
  at any given time
