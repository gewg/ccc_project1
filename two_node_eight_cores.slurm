#!/bin/bash
#SBATCH -p physical
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --ntasks=8

module purge
module load foss/2019b
module load mpi4py/3.0.2-timed-pingpong
module load python/3.7.4
module load openmpi/3.1.4

srun -n 8 python3 main.py "data/bigTwitter.json" "data/sydGrid.json" "data/grid_id_map.json" "data/language_code.json"

##DO NOT ADD/EDIT BEYOND THIS LINE##
##Job monitor command to list the resource usage
my-job-stats -a -n -s