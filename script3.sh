#!/bin/bash
#SBATCH -p physical
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --time=02:00:00
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=FAIL
#SBATCH --job-name=Twitter_3
#SBATCH -e output/error/job3/error_%j.err
#SBATCH -o output/job3/slurmp-%j.out

module load Python/3.4.3-goolf-2015a
mpiexec -n 8 python rank.py