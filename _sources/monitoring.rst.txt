.. _monitor-label:

===============
Monitoring
===============

This page includes some backend specific instruction for monitoring the different sites.

.. contents::
   :local:
   :depth: 3


DIRAC
=====

Installing Dirac is quite easy nowadays! This information comes directly from
https://www.gridpp.ac.uk/wiki/Quick_Guide_to_Dirac. Running all commands will
install dirac version ``$DIRAC_VERSION`` to ``$HOME/dirac_ui``. You can change this
by modifying the variable ``DIRAC_FOLDER``

.. code-block:: bash

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

.. note::
    Remember you might need to source ``$DIRAC_FOLDER/bashrc`` every time you want to use dirac.


Monitoring sites
===================

Durham ARC monitoring website
-----------------------------
https://grafana.mon.dur.scotgrid.ac.uk/d/LNUGi5yWk/general-grid

DIRAC monitoring website
------------------------
https://dirac.gridpp.ac.uk:8443/DIRAC/

UK ARC INFO
-----------

.. code-block:: bash

    ./get_site_info.py


Grid storage management
=======================
`Duncan <https://github.com/DWalker487>`_ has written a wrapper to the gfal commands in order to simplify manual navigation of the DPM filesystems. A frequently updated version can be found at `dpm_manager <https://github.com/DWalker487/dpm-manager>`_.

.. code-block:: bash

    dpm_manager <gfal_dir> -s <file search terms> -r <file reject terms> [-cp (copy to gridui)] [-rm (delete from grid storage)] [-j (number threads)] [-cpg (copy from gridui to storage)] ...

More info is given in the help text (``dpm_manager -h``)

NNLOJET distributed warmup
==========================

NNLOJET can run warmups in distributed mode by using the `vegas-socket <https://github.com/scarlehoff/vegasSocket>`_ implementation.

- compile NNLOJET with ``sockets=true`` to enable distribution

- set up server ``./vegas_socket.py -p PORT -N NO_CLIENTS -w WAIT``

  - 1 unique port for each separate NNLOJET run going on [rough range 7000-9999]

  - wait parameter is time limit (secs) from first client registering with the server before starting without all clients present. If not set, it will wait *forever*! Jobs die if they try and join after this wait limit.

    NB. the wait parameter can't be used for distribution elsewhere -> it
    relies on ``nnlorun.py`` (grid script) in jobscripts to kill run

  - The server is automatically killed on finish by ``nnlorun.py`` (grid script)

- In the grid runcard set ``sockets_active >= N_clients``, ``port = port number``
  looking for.

  - Will send ``sockets_active`` as number of jobs. If this is more than the
    number of sockets looked for by the server, any excess will be killed as
    soon as they start running

- NNLOJET runcard ``# events`` is the total number of events (to be divided by
  amongst the clients)

- The server must set up on the same gridui as defined in the header parameter
  ``server_host``. Otherwise the jobs will never be found by the running job.

Hamilton queues
===============

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
- Current monitoring info can be found using the ``sfree`` command, which gives
  the number of cores in use at any given time
