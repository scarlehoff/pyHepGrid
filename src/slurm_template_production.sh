#!/bin/bash
#SBATCH -o {stdoutfile}
#SBATCH --array=1-{producRun}
#SBATCH --error {stderrfile}

cd {runcard_dir}
OMP_NUM_THREADS={threads} ./NNLOJET -run {runcard} -iseed $((${{SLURM_ARRAY_TASK_ID}} + {baseSeed}))

exit 0
