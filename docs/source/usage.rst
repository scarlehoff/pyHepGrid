.. _usage-label:

===============
Usage 
===============

This page contains the pyHepGrid usage guide.

.. contents::
   :local:
   :depth: 3

pyHepGrid instruction set
=========================

1. initialise libraries [LHAPDF,(OPENLOOPS?)]

.. code-block:: bash

   pyHepGrid ini -L

2. initialise runcard

.. code-block:: bash

    pyHepGrid ini runcard.py -Bn -w warmup.file

where ``-B`` is production in arc ``-D`` in dirac ``-A`` is warmup in arc


3. send the jobs to run with one of:

.. code-block:: bash

    pyHepGrid run <runcard.py> -A # ARC WARMUP
    pyHepGrid run <runcard.py> -B # ARC PRODUCTION
    pyHepGrid run <runcard.py> -D # DIRAC PRODUCTION

4. manage the jobs/view the database of runs with:

.. code-block:: bash

    pyHepGrid man <runcard.py> -(A/B/D)

include the flags:

  - ``-S/-s`` for job stats
  - ``-p`` to print the stdout of the job (selects last job of set if production)
  - ``-P`` to print the job log file (selects last job of set if production)
  - ``-I/-i`` for job info

For running anything on the grid, the help text in ``pyHepGrid`` (``pyHepGrid
-h``) is useful for hidden options that aren't all necessarily documented(!).
These features include warmup continuation, getting warmup data from running
warmup jobs, initialising with your own warmup from elsewhere, database
management stuff, local runcard testing.

When running, the python script ``runfile`` (e.g. ``nnlorun.py``) is sent to the run
location. This script then runs automatically, and pulls all of the appropriate
files from grid storage (NNLOJET exe, runcard (warmups)). It then runs
NNLOJET in the appropriate mode, before tarring up output files and sending
them back to the grid storage.

Finalising results
==================

The process of pulling the production results from grid storage to the gridui

You have a choice of setups for this (or you can implement your own)

Default setup
-------------

By default, ``pyHepGrid ships a "--get_data"`` script that allows you to retrieve
jobs looking at the database.

.. code-block:: bash

    pyHepGrid man -A --get_data

For ARC runs (either production or warmup)
or for Dirac runs:

.. code-block:: bash
    
    pyHepGrid man -D --get_data

The script will then ask you which database entry do you want to retrieve and
will put the contents in the folders defined in your header.
``warmup_base_dir/date/of/job/RUNNAME`` for warm-ups or
``production_base_dir/date/of/job/RUNNAME`` for production.

For instance, let's suppose you sent 4000 production runs to Dirac on the 1st of
March and this job's entry is 14, you can do

.. code-block:: bash
    
    pyHepGrid man -D -g -j 14

and it will download all .dat and .log files to ``warmup_base_dir/March/1/RUNNAME``

Custom setups
------------

For your own custom setup, you just need to write a finalisation script which
exposes a function called ``do_finalise()``. This function does the pulling from
the grid storage. You then set the variable ``finalisation_script`` to the name of
your script (without the ``.py`` suffix). For example:

.. code-block:: bash

    ./finalise.py

set ``finalisation_script = "finalise"`` in your header and just do

.. code-block:: bash

    pyHepGrid man --get_data

This will find all of the runcards specified at the top of ``finalise_runcard.py``
(or other as specified in ``finalise_runcards``) and pull all of the data it can
find for them from the grid storage. The output will be stored in
``production_base_dir`` (as set in the header) with one folder for each set of
runs, and the prefix as set in ``finalise_prefix``. Corrupted data in the grid
storage will be deleted.

Normal workflow
===============

0. Make sure you have a working proxy
1. initialise warmup runcard
2. run warmup runcard
3. switch warmup -> production in runcard
4. When warmup complete, reinitialise runcard for production
5. run production runcard as many times as you like w/ different seeds
6. pull down the results (finalisation)

runcard.py files details
========================

- Include a dictionary of all of the runcards you want to submit/initialise/manage, along with an identification tag that you can use for local accounting

- ``template_runcard.py`` is the canonical example

- Must be valid python to be used

- Has a functionality whereby you can override any parameters in your header file by specifying them in the runcard file. So you can e.g specify a different submission location for specific runs, give different starting
seeds/numbers of production runs.

- You can even link/import functions to e.g dynamically find the best submission location
