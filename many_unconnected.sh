#!/bin/bash

#SBATCH -c 1
#SBATCH --array=1-400
##SBATCH --mem-per-cpu=8000
#SBATCH -t 10:00:00
#SBATCH -o logs/%A_%a.out
#SBATCH -e logs/%A_%a.err

echo `hostname`

wdir=/projects/sequence_analysis/vol3/bizon/graphsampler
cd $wdir

. /projects/sequence_analysis/vol3/tools/anaconda/etc/profile.d/conda.sh
conda deactivate
conda activate translator

a="gene"
b="disease"
n=10000

python main.py -a $a -b $b -n $n -o ${a}_${b}/unconnected_$SLURM_ARRAY_TASK_ID
