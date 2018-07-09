#!/bin/bash
#SBATCH -o {stdoutfile}
#SBATCH --error {stderrfile}
#SBATCH --mem {memsize}M         
{array}

cd {runcard_dir}
export OMP_STACKSIZE={stacksize}M

export OMP_NUM_THREADS={threads}
./NNLOJET -run {runcard} {socketstr}

exit 0
