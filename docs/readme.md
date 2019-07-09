[![DOI](https://zenodo.org/badge/172027514.svg)](https://zenodo.org/badge/latestdoi/172027514)

(Incomplete, and always will be. The grid is a mysterious thing...)

# CONTENTS
1)  [INITIAL SETUP](#1-initial-setup)
2)  [NNLOJET SETUP](#2-for-nnlojet-developers-nnlojet-setup)
3)  [LFN/GFAL SETUP](#3-lfn-setup)
4)  [GRID SCRIPTS SETUP (GANGALESS)](#4-grid-scripts-setup)
5)  [PROXY SETUP](#5-proxy-setup)
6)  [GRID SCRIPTS USAGE](#6-grid-scripts-usage)
7)  [FINALISING RESULTS](#7-finalising-results)
8)  [NORMAL WORKFLOW](#8-normal-workflow)
9)  [RUNCARD.PY FILES DETAILS](#9-runcardpy-files-details)
10) [DIRAC](#10-dirac)
11) [MONITORING WEBSITES](#11-monitoring-websites)
12) [GRID STORAGE MANAGEMENT](#12-grid-storage-management)
13) [DISTRIBUTED WARMUP](#13-distributed-warmup)
14) [HAMILTON QUEUES](#14-hamilton-queues)

## 1. INITIAL SETUP

Follow certificate setup as per Jeppe's tutorial @
https://www.ippp.dur.ac.uk/~andersen/GridTutorial/gridtutorial.html

Make a careful note of passwords (can get confusing). Don't use passwords used
elsewhere in case you want to automate proxy renewal (like me and Juan)

## 2. (for nnlojet developers) NNLOJET SETUP

As usual - pull the NNLOJET repository, update to modules and `make -jXX`

**YOU MUST INSTALL WITH LHAPDF-6.1.6. 6.2.1 IS BROKEN, and will not work on the
grid outside of Durham(!)**

- When installing lhapdf, don't have the default prefix `$HOME` for installation
  as the entire home directory will be tarred up when initialising the grid
  libraries for LHAPDF(!)

- For this you will also need to install and link against boost (painful I
  know...)

- As of 20/4/2018, the minimum known compatible version of gcc with NNLOJET is
  gcc 4.9.1. Versions above this are generally ok

## 3. LFN SETUP

put this into your bashrc:
```bash
export CC=gcc
export XX=g++
export LCG_CATALOG_TYPE="lfc"
export LFC_HOME=/grid/pheno/<LFN_NAME>
export LFC_HOST="lfc01.dur.scotgrid.ac.uk"
source /opt/rh/devtoolset-4/enable              # Default gcc is version 4 and this DOES NOT WORK!
```

then `source ~/.bashrc`

create lfndir
```bash
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

generate more directories in analogy. A nice wrapper is included as described in
the [GRID STORAGE MANAGEMENT](#12-grid-storage-management) section later on.

To use GFAL instead, one needs to perform the above setup, except using
```bash
gfal-mkdir gsiftp://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/<directoryname>
```

The folder setup that the GFAL setup uses should be the same as for LFN, so you
will need an input, output and warmup folder as normal

GFAL should be much more stable and quick than the LFN, and has been testsed with DIRAC.
It has been tested for both production and warmup directly via ARC as well.

It works for ARC, and you can view the files in a web browser at
http://se01.dur.scotgrid.ac.uk/dpm/dur.scotgrid.ac.uk/home/pheno/

To enable, toggle `use_gfal` to `True` in your header file. This will also
change over the `finalise.py` to use the gfal system where LFN files cannot be
seen.

## 4. GRID SCRIPTS SETUP

To start using `pyHepGrid` you need to do the following steps.

0. Keep track of all your changes by committing them (e.g. fork the remote)
1. Create your own header (e.g. copy and edit the `src/pyHepGrid/headers/template_header.py`)
2. Add yourself to the `header_mappings` in `src/pyHepGrid/src/header.py`.
    This is used for a python import so a header in `some_folder/your_header.py`
    would require `your_name: some_folder.your_header`
3. Generate a `runcard.py` similar to `template_runcard.py` inside the `runcards`
    folder. `runcard.py` is used to run `pyHepGrid` *not your program*. The only
    required setting in there is `dictCard`, but you can also overwrite any
    setting you have in your personal header, e.g. `BaseSeed` or `producRun`.
4. Create folders on gfal/lfn to save your in and output. They have to match
    `lfn_input_dir`, `lfn_output_dir` and `lfn_warmup_dir` of your header
5. For non NNNLOJet developers: Write you own `runfile` similar to `nnlorun.py`.
    This script will be ran on each node, so it should be *self-contained* and
    *Python 2.4 compatible*. It should also be able to handle all arguments of
    the `nnnnlorun.py`, even if they are not used in the script itself. It is
    easiest to simply copy the `parse_arguments` function from there. Most
    arguments correspond to a similar named setting from the runcard/header.
    To run on Dirac make sure you do not depend on a specific local setup, i.e.
    download all required programs from gfal/lfn or use what is available on the
    `/cvmfs/`.
6. To install and run the scripts, run
```bash
python3 setup.py install --user
python3 setup.py develop --user
```
(Include the `--prefix` option and add to a location contained in `$PYTHONPATH` 
if you want to install it elsewhere). `--user` is used on the gridui as we don't 
have access to the python3 installation - if you have your own install, feel free
to drop it. 
We currently need to be in develop mode given the way that the header system works - 
the plan is for this to change at some point.

After this you should be able to run `pyHepGrid test runcards/your_runcard.py
-B`. This will execute the your `runfile` locally inside the `test_sandbox`
folder. You might want to try running it with a *clean* environment, e.g.
without sourcing your `~/.bashrc`. If this works fine you can try submitting to
the arc  test queue with `pyHepGrid run runcards/your_runcard.py -B --test`. The
test queue highly limited in resources. **Only submit a few short jobs to it**
(<10).

## 5. PROXY SETUP

By default, jobs will fail if the proxy ends before they finish running, so it's
a good idea to keep them synced with new proxies as you need:

### By hand

1. Create new proxy
```bash
arcproxy -S pheno -N -c validityPeriod=24h -c vomsACvalidityPeriod=24h
```
2. Sync current jobs with latest proxy
```bash
arcsync -c ce1.dur.scotgrid.ac.uk
arcrenew -a
```

### Automated (set & forget)

I've added some proxy automation scripts to the repo in
`gangaless_resources/proxy_renewal/` To get these working, simply add your
certificate password in to `.script2.exp` (plain text I know, so it's bad ... at
least hide it from the word with `chmod 400`) Make sure `.proxy.sh` is set up
for your user (directories should point to your gangaless resources)

Run by hand to check (shouldn't need your password) Then set up `.proxy.sh` to
run as a [cron job](https://crontab.guru/) at least once per day (I suggest 2x
in case of failure)

## 6. GRID SCRIPTS USAGE

1. initialise libraries [LHAPDF,(OPENLOOPS?)]
```bash
pyHepGrid ini -L
```

2. initialise runcard
```bash
pyHepGrid ini runcard.py -Bn -w warmup.file
```
 `-B` is production in arc `-D` in dirac `-A` is warmup in arc


3. send the jobs to run with one of:
```bash
pyHepGrid run <runcard.py> -A # ARC WARMUP
pyHepGrid run <runcard.py> -B # ARC PRODUCTION
pyHepGrid run <runcard.py> -D # DIRAC PRODUCTION
```

4. manage the jobs/view the database of runs with:
```bash
pyHepGrid man <runcard.py> -(A/B/D)
```

include the flags:

  - `-S/-s` for job stats
  - `-p` to print the stdout of the job (selects last job of set if production)
  - `-P` to print the job log file (selects last job of set if production)
  - `-I/-i` for job info

For running anything on the grid, the help text in `pyHepGrid` (`pyHepGrid
-h`) is useful for hidden options that aren't all necessarily documented(!).
These features include warmup continuation, getting warmup data from running
warmup jobs, initialising with your own warmup from elsewhere, database
management stuff, local runcard testing.

When running, the python script `runfile` (e.g. `nnlorun.py`) is sent to the run
location. This script then runs automatically, and pulls all of the appropriate
files from grid storage (NNNNLOJET exe, runcard (warmups)). It then runs
NNNNNLOJET in the appropriate mode, before tarring up output files and sending
them back to the grid storage.

## 7. FINALISING RESULTS
The process of pulling the production results from grid storage to the gridui

You have a choice of setups for this (or you can implement your own)

### DEFAULT SETUP

By default, `pyHepGrid ships a "--get_data"` script that allows you to retrieve
jobs looking at the database.
```bash
pyHepGrid man -A --get_data
```
For ARC runs (either production or warmup)
or
```bash
pyHepGrid man -D --get_data
```
for DIRAC runs.

The script will then ask you which database entry do you want to retrieve and
will put the contents in the folders defined in your header.
`warmup_base_dir/date/of/job/RUNNAME` for warm-ups or
`production_base_dir/date/of/job/RUNNAME` for production.

For instance, let's suppose you sent 4000 production runs to Dirac on the 1st of
March and this job's entry is 14, you can do
```bash
pyHepGrid man -D -g -j 14
```
and it will download all .dat and .log files to
```bash
warmup_base_dir/March/1/RUNNAME
```

### CUSTOM SETUPS

For your own custom setup, you just need to write a finalisation script which
exposes a function called `do_finalise()`. This function does the pulling from
the grid storage. You then set the variable `finalisation_script` to the name of
your script (without the `.py` suffix). Happy days!

#### For example:
```bash
./finalise.py
```
set `finalisation_script = "finalise"` in your header and
```bash
pyHepGrid man --get_data
```

This will find all of the runcards specified at the top of `finalise_runcard.py`
(or other as specified in `finalise_runcards`) and pull all of the data it can
find for them from the grid storage. The output will be stored in
`production_base_dir` (as set in the header) with one folder for each set of
runs, and the prefix as set in `finalise_prefix`. Corrupted data in the grid
storage will be deleted.

## 8. NORMAL WORKFLOW

0. Make sure you have a working proxy
1. initialise warmup runcard
2. run warmup runcard
3. switch warmup -> production in runcard
4. When warmup complete, reinitialise runcard for production
5. run production runcard as many times as you like w/ different seeds
6. pull down the results (finalisation)

## 9. RUNCARD.PY FILES DETAILS

- Include a dictionary of all of the runcards you want to
  submit/initialise/manage, along with an identification tag that you can use for
  local accounting

- `template_runcard.py` is the canonical example

- Must be valid python to be used

- Has a functionality whereby you can override any parameters in your header
  file by specifying them in the runcard file. So you can e.g specify a
  different submission location for specific runs, give different starting
  seeds/numbers of production runs.

- You can even link/import functions to e.g dynamically find the best submission
  location

## 10. DIRAC

Installing Dirac is quite easy nowadays! This information comes directly from
https://www.gridpp.ac.uk/wiki/Quick_Guide_to_Dirac. Running all commands will
install dirac version `$DIRAC_VERSION` to `$HOME/dirac_ui`. You can change this
by modifying the variable `DIRAC_FOLDER`

```bash
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
You need to source `$DIRAC_FOLDER/bashrc` every time you want to use dirac.
Running the following command will put the _source_ in your own `.bashrc`

```bash
echo "source $DIRAC_FOLDER/bashrc" >> $HOME/.bashrc
```
If you don't know how to source something in your `.bashrc` by now, I doubt that
this tutorial is for you.

#### ALTERNATIVE

Instead of sourcing the dirac `bashrc` as above, you can alternatively add
`$DIRAC_FOLDER/scripts/` to your `PATH` variable directly in your `.bashrc`. It
all seems to work ok with python 2.6.6

## 11. MONITORING WEBSITES
### DURHAM ARC MONITORING WEBSITE
https://grafana.dur.scotgrid.ac.uk/dashboard/db/uki-scotgrid-durham-grid-queues?refresh=15s&orgId=1

### DIRAC MONITORING WEBSITE
https://dirac.gridpp.ac.uk:8443/DIRAC/

### UK ARC INFO
```bash
./get_site_info.py
```

## 12. GRID STORAGE MANAGEMENT
I've written a wrapper to the gfal commands in order to simplify manual navigation of
the DPM filesystems.

A frequently updated version can be found [here](https://github.com/DWalker487/dpm-manager "dpm_manager").

Usage:
```bash
  dpm_manager <gfal_dir> -s <file search terms> -r <file reject terms>
    [-cp (copy to gridui)] [-rm (delete from grid storage)] [-j (number threads)]
    [-cpg (copy from gridui to storage)]...
```

More info is given in the help text (`dpm_manager -h`)

## 13. DISTRIBUTED WARMUP
***to clean up***

- compile NNLOJET with `sockets=true` to enable distribution

- set up server `NNLOJET/driver/bin/vegas_socket.py`,
  `./vegas_socket.py -p PORT -N NO_CLIENTS -w WAIT`

  - 1 unique port for each separate NNLOJET run going on [rough range 7000-9999]

  - wait parameter is time limit (secs) from first client registering with the
    server before starting without all clients present. If not set, it will wait
    *forever*! Jobs die if they try and join after this wait limit.

    NB. the wait parameter can't be used for distribution elsewhere -> it
    relies on `nnlorun.py` (grid script) in jobscripts to kill run

  - The server is automatically killed on finish by `nnlorun.py` (grid script)

- In the grid runcard set `sockets_active >= N_clients`, `port = port number`
  looking for.

  - Will send `sockets_active` as number of jobs. If this is more than the
    number of sockets looked for by the server, any excess will be killed as
    soon as they start running

- NNLOJET runcard `# events` is the total number of events (to be divided by
  amongst the clients)

- The server must set up on the same gridui as defined in the header parameter
  `server_host`. Otherwise the jobs will never be found by the running job.

## 14. HAMILTON QUEUES

- There are multiple queues I suggest using on the HAMILTON cluster:
    - par6.q

      16 cores per node [set warmupthr = 16]\
      No known job \# limit, so you can chain as many nodes as you like to
      turbocharge warmups\
      3 day job time limit

    - par7.q

      24 cores per node [set warmupthr = 24]\
      \# jobs limited to 12, so you can use a maximum of 12\*24 cores at a given
      time\
      3 day job time limit

    - openmp7.q **NOT RECOMMENDED**

      58 cores total - this is tiny, so I would recommend par7 or par6\
      \# jobs limited to 12\
      No time limit\
      Often in competition with other jobs, so not great for sockets

- Use NNLOJET ludicrous mode if possible - it will have a reasonable speedup
  when using 16-24 core nodes in warmups
- Current monitoring info can be found using the `sfree` command, which gives
  the number of cores in use at any given time
