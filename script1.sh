#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=03:00:00
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=FAIL
#SBATCH --job-name=Twitter_1
#SBATCH -e output/error/job1/error_%j.err
#SBATCH -o output/job1/slurmp-%j.out

module load Python/3.4.3-goolf-2015a
mpiexec -n 1 python rank.py