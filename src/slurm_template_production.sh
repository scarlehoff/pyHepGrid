#!/bin/bash
#SBATCH -o {stdoutfile}
#SBATCH --error {stderrfile}
#SBATCH --array=1-{producRun}
#SBATCH --error {stderrfile}
#SBATCH --job-name={jobName}
{exclude_list}


cd {runcard_dir}
OMP_NUM_THREADS={threads} ./{exe} -run {runcard} -iseed $((${{SLURM_ARRAY_TASK_ID}} + {baseSeed}))

exit 0
