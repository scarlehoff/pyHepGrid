#!/bin/bash
#SBATCH -o {stdoutfile}
{array}

cd {runcard_dir}
OMP_NUM_THREADS={threads} ./NNLOJET -run {runcard} {socketstr}

exit 0
