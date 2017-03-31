#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=02:30:00
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=FAIL
#SBATCH --job-name=Twitter_2
#SBATCH -e output/error/job2/error_%j.err
#SBATCH -o output/job2/slurmp-%j.out

module load Python/3.4.3-goolf-2015a
mpiexec -n 8 python rank.py