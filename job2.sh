#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=00:05:00
#SBATCH --mail-type=END,FAIL
#SBATCH --job-name=Twitter_2
#SBATCH -e output/error/job2/error_%j.err
#SBATCH -o output/job2/slurm-%j.out

echo 'Run job with 1 node and 8 core'
echo ' '

module load Python/3.4.3-goolf-2015a
mpiexec python rank.py

