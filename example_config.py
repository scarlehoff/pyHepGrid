# This is the main configuration file for the grid files, it is user specific
# make the necessary changes to example_config.py and then rename it to config.py
# Do not commit your hand modified version


# A dictionary of runcards and their input tars on the LFN
RUNS = {'TEST.run' : 'NNLOJET'}

# Number of runs used for production per runcard
NUMRUNS = 500

# Number of threads used for warmup runs
NUMTHREADS = 10

# NNLOJET main directory
NNLOJETDIR='/mt/home/morgan/NNLOJET'

# LHAPDF main directory
LHAPDFDIR='/mt/home/morgan/NNLOJET/driver/LHAPDF'

# Directory containing the run cards you want to submit to the grid
RUNCARDS='/mt/home/morgan/NNLOJET/driver/grid'

# GCC compiler directory
GCCDIR='/mt/home/morgan/gcc-5.2.0/'

# LFN directory 
LFNDIR = '/grid/pheno/morgan/'



