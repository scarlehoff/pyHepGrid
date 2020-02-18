#!/bin/bash
#SBATCH --get-user-env
#SBATCH -o {stdoutfile}
#SBATCH --error {stderrfile}
#SBATCH --array=1-{producRun}
#SBATCH --job-name={jobName}
#SBATCH --mem-per-cpu {memsize}
#SBATCH -c {threads}
{exclude_list}
{partition}


cd {runcard_dir}
hostname
which python
export OMP_NUM_THREADS={threads}
{exe} {runcard} $((${{SLURM_ARRAY_TASK_ID}} + {baseSeed}))

exit 0
