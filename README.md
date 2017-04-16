COMP90024 HPC Twitter Geoprocessing
-----------------------------------

My solution to COMP90024 Cluster and Cloud Computing Assignment 1.

#### Problem Case
Implement a simple, parallelized application leveraging the
University of Melbourne HPC facility SPARTAN. The application will search a large geocoded Twitter dataset to identify tweet hotspots around Melbourne.

Code Structure
--------------
    ├── data                    
    ├──── melbGrid.json         # melbourne coordinates box
    ├──── tinyTwitter.json      # array of twitter json data
    ├──── smallTwitter.json     # array of twitter json data (bigger than tinyTwitter.json)
    ├── output                  # output result from slurm job
    ├── slurm-job               #
    ├──── job1.sh               # slurm job with 1 node and 1 core configuration
    ├──── job2.sh               # slurm job with 1 node and 8 core configuration
    ├──── job3.sh               # slurm job with 2 node and 8 core configuration
    ├── .gitignore               
    ├── LICENSE               
    ├── rank.py                 # main python script
    └── README.md

Getting Started
---------------

### Prerequisites
```bash
brew install mpich
```

### Setup Python and VirtualEnv
VirtualEnv is a way to create isolated Python environments for every project and VirtualEnvWrapper "wraps" the virtualenv API to make it more user friendly.

```bash
$ pip install pip --upgrade
$ pip install virtualenv
$ pip install virtualenvwrapper
```

To complete the virtualenv setup process, put the following in you ~/.bash_profile
```bash
export WORKON_HOME=$HOME/.virtualenvs
source /usr/local/bin/virtualenvwrapper.sh
```

### Create VirtualEnv and Install Dependencies
The following commands will ensure you have the Python dependencies installed inside your `virtualenv`.

```bash
$ mkvirtualenv hpc-project --python=python3
$ pip install -r requirements.txt
```

### Run The Program
For example run in parallel with 4 node

    $ mpiexec -n 4 rank.py
