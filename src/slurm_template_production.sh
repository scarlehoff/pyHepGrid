#!/bin/bash
#SBATCH -o {stdoutfile}
#SBATCH --error {stderrfile}
#SBATCH --array=1-{producRun}
#SBATCH --job-name={jobName}
{exclude_list}
{exclusive}
{partition}


cd {runcard_dir}
hostname
export OMP_NUM_THREADS={threads} 
./{exe} -run {runcard} -iseed $((${{SLURM_ARRAY_TASK_ID}} + {baseSeed}))

exit 0
