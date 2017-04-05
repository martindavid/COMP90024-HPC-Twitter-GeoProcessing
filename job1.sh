#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=00:05:00
#SBATCH --mail-type=END,FAIL
#SBATCH --job-name=Twitter_1
#SBATCH -e output/error/job1/error_%j.err
#SBATCH -o output/job1/slurm-%j.out

echo ' '
echo 'Run job with 1 node and 1 core'

module load Python/3.4.3-goolf-2015a
mpiexec python rank.py
