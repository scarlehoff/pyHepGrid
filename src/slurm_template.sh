#!/bin/bash
#SBATCH -o {stdoutfile}
#SBATCH --error {stderrfile}
#SBATCH --mem {memsize}M         
#SBATCH --cpus-per-task={threads}
#SBATCH --job-name={jobName}
{array}
{exclusive}
{exclude_list}

cd {runcard_dir}
export OMP_STACKSIZE={stacksize}M

export OMP_NUM_THREADS={threads}
./{exe} -run {runcard} {socketstr}

exit 0
