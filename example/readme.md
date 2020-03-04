# Basic example program

In this folder we provide a simple program, that can be loaded via `pyHepGrid
MODE runcard_example.py -B`. This example contains a custom [grid
script](simplerun.py), which can run on the grid, and a custom
[`runmode`](backend_example.py) to setup `pyHepGrid`.

## Outline

In total this folder contains the following files:

* [`runcard_example.py`](runcard_example.py) is the configuration (*'runcard'*) for
  `pyHepGrid`. This runcard has to provided when executing `pyHepGrid` and tells
   `pyHepGrid` what to do.
   
* [`simplerun.py`](simplerun.py) is the grid script, that will be *run on the
  grid nodes*.  This will setup the run environment for the actual executable 
  you want to run, run it, and upload the results to the grid storage *via*
  `gfal`.
  
* [`executable_example.sh`](executable_example.sh) is a dummy executable that
  [`simplerun.py`](simplerun.py) will download and execute. This is a placeholder
  for the computational 'work' you want to do, and could be replaced with your
  actual program.
  
* [`dummy_folder`](dummy_folder) is a folder that contains a *configuration* file
  that will be passed to [`executable_example.sh`](executable_example.sh) when run
  on the grid.  This could be a runcard for your program, but is an entirely general
 'input file'.  See also `dictCard` in [`simplerun.py`](simplerun.py).
 
* [`backend_example.py`](backend_example.py) implements a `runmode` (a subclass
  of `ProgramInterface`), which steers the background process of `pyHepGrid`.
  Currently it uploads [`executable_example.sh`](executable_example.sh) and the
  [`configuration`](dummy_folder/config) to `gfal` when running `pyHepGrid ini`.
  Default behaviours are inherited from those in the `ProgramInterface` class, and
  can be replaced with your own in your `runmode` subclass as necessary.


## Usage

On the `gridui`:

* `pyHepGrid ini runcard_example.py -B` will use
	[`backend_example.py`](backend_example.py) to upload all required files via `gfal`
	to `grid_input_dir`;

* `pyHepGrid run runcard_example.py -B` will submit jobs to the grid (as many as specified
	in `producRun`.

Then, on each node, [`simplerun.py`](simplerun.py) will:
 
* download the input from `grid_input_dir` on `gfal`, and untar it;

* run [`executable_example.sh`](executable_example.sh) with the configuration
[`config`](dummy_folder/config) from [`dummy_folder`](dummy_folder);

* tar the results and upload them via `gfal` to `grid_output_dir`.

In order to download the results through `pyHepGrid` you will need to write a custom
`finalisation_script` to match the expected form of your output.