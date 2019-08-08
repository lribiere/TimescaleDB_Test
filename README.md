# Aura_LoadTesting

Ansible role to automate data loading test to evaluate Aura infrastructure's data storage capacity. 
Data are randomly generated and injected in TimescaleDB database.

## Aura Infrastructure

At first, you need to install Aura Infrastructure (https://github.com/RGirard94/TimescaleDB) with TimescaleDB

## Playbook's details

This Ansible playbook consists of two roles:
  - prerequisite : installs tools used to perform well the different python scripts.
  - copy_directories : copies a directory containing the python scripts to randomly generate data and an other directory
                       containing the python scripts to inject the generated data in influxdb database. These directories are
                       copied from your local machin to the virtual machin containing the Aura Infrastructure.

## Usage

At first you need to launch the playbook to install prerequisite and copy_directories roles. It allows to set the work environment to perform well the loading tests.

`ansible-playbook -i inventories/dev.yml install.yml`

Two tests are located in python_test_script directory
  - loading_test_timescale.py inject daily collected data into TimescaleDB and check the number of injected data.
  - reading_test_timescale.py performs some queries.
  
To execute these scripts :
 `sudo python3.6 loading_test_timescale.py`
 `sudo python3.6 reading_test_timescale.py`
 
## Problem

 When TimescaleDB docker container is deployed, you have to modify manually following line's value :
  `host all all all md5` by `host all all all trust` located in `var/lib/postgresql/data/pg_hba.conf`
