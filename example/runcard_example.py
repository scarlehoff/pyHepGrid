import pyHepGrid.src.Database as dbapi
import os
print("Sourcing runcard")
dictCard = {
    # folder       :   config file / runcard
    "dummy_folder": "config",
}
# for the example, keep everything in 'example' folder.
exampleDir = os.path.dirname(os.path.realpath(__file__))

# Run Variables

# run-specific variables (overrides header.py variables)
runcardDir = exampleDir
# number of concurrent jobs to submit
producRun = 5
# ARC jobname to appear in squeue output
jobName = "EXAMPLE_RUN"

# ARC's jobs database: put this wherever you want
arcbase = os.path.expanduser("~/.arc/jobs.dat")
# pyHepGrid SQL database
dbname = F"{exampleDir}/ARCdb_example.dat"


# user-specific global variables (could go in user's header)
copy_log = True

# automatically pick next unused seed
baseSeed = dbapi.get_next_seed(dbname=dbname) + 1
# baseSeed = 2173  # (setting seed manually)

# Project-level Variables

# Your custom subclass of `ProgramInterface` class in
# "src/pyHepGrid/src/ProgramInterface.py"
# import should be relative to this runcard
runmode = "backend_example.ExampleProgram"

runfile = exampleDir+"/simplerun.py"

# path to your executable: src_dir/exe
executable_src_dir = exampleDir
executable_exe = "executable_example.sh"

grid_input_dir = "example/input"
grid_output_dir = "example/output"
# grid_warmup_dir = "example/warmup" # not used

provided_warmup_dir = runcardDir

# Path to executable on gfal (non-standard/custom setting)
grid_executable = "example/executable.tar.gz"

# User Variables

# your DIRAC username: only needed if you run on DIRAC.
# dirac_name = "your.username"

# custom post-run local download (and processing) script
# finalisation_script = "path/to/your/script.py"
