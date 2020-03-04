# Basic example program

In this folder we provide a simple program, that can be loaded via `pyHepGrid
MODE runcard_example.py -B`. This example contains a custom [grid
script](simplerun.py), which can run on the grid, and a custom
[`runmode`](backend_example.py) to setup `pyHepGrid`.

In total this folder contains the following files:
* [`runcard_example.py`](runcard_example.py) is the configuration (*runcard*) for
  `pyHepGrid`. This runcard has to provided when executing `pyHepGrid`.
* [`simplerun.py`](simplerun.py) is the grid script, that is *run on the
  grid nodes*. It will setup the run environment for the actual executable and
  uploads the results to `gfal`.
* [`exectuable_example.sh`](exectuable_example.sh) is a dummy executable that
  [`simplerun.py`](simplerun.py) will download & execute. This could be your
  actual program.
* [`dummy_folder`](dummy_folder) is a folder that contains a *configuration* to
  pass to [`exectuable_example.sh`](exectuable_example.sh) when run on the grid,
  see also `dictCard` in [`simplerun.py`](simplerun.py).
* [`backend_example.py`](backend_example.py) implements a `runmode`
  (`ProgramInterface`), which steers the background process of `pyHepGrid`.
  Currently it uploads [`exectuable_example.sh`](exectuable_example.sh) and the
  [`configuration`](dummy_folder/config) to `gfal` when running `pyHepGrid ini`.

In conclusion `pyHepGrid ini runcard_example.py -B` will use
[`backend_example.py`](backend_example.py) to upload all requirements to `gfal`.
`pyHepGrid run runcard_example.py -B` will submit jobs to the grid. On each node
[`simplerun.py`](simplerun.py) will download the setup from `gfal`, run
[`exectuable_example.sh`](exectuable_example.sh) with the
[`configuration`](dummy_folder/config) from [`dummy_folder`](dummy_folder), and
uploaded the *results* back to `gfal`.
