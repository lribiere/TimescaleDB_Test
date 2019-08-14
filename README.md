# Aura - Timescale Load Testing

This ansible project allows to deploy TimescaleDB as well as some data generation and injection python scripts. The data is generated randomly (with values lying within the physiological ranges of the corresponding signals).

## Playbook's details

This Ansible playbook consists of three roles:
  - prerequisite : installs tools and dependencies necessary for the stack,
  - timescaledb : installs the timescale database,
  - copy_python_scripts : copies the python scripts responsible for data generation and injection into the db.

## Usage

At first you need to launch the playbook to install prerequisite and copy_directories roles. It allows to set the work environment to perform well the loading tests.

`ansible-playbook -i inventories/<env>.yml install.yml`

Two tests are located in python_test_script directory
  - loading_test_timescale.py inject daily collected data into TimescaleDB and check the number of injected data.
  - reading_test_timescale.py performs some queries.
  
To execute these scripts use the following commands:
 `python3.6 loading_test_timescale.py`
 `python3.6 reading_test_timescale.py`
 
## Development environment
### Prerequisites
 * [Vagrant v1.8.6](https://www.vagrantup.com/)
 * To have a public ssh key on your local machine at this location : `~/.ssh/id_rsa.pub`

### tl;dr
 0. `vagrant up`
 1. `ansible-playbook -i inventories/dev.yml install.yml`
 2. Enjoy
 3. `vagrant destroy`

 Note: you need to remove the line corresponding to the vm public key each time you destroy the virtual machine. You can do this easily with the following command: `sed -i '' '/192.168.33.22 /d' ~/.ssh/known_hosts`.

### Usage
Your local vagrant environment is configured inside the inventory `inventories/dev.yml`.

You can run any playbook on this environment.

To have a local url that route to this development environment you can add this line in your hosts file (/etc/hosts) : `192.168.33.22   db.aura.healthcare.local`

You can ssh to the virtual machine with : `ssh ansible@192.168.33.22`


## Problem

 When TimescaleDB docker container is deployed, you have to manually modify the following file : `var/lib/postgresql/data/pg_hba.conf`. You should replace `host all all all md5` by `host all all all trust` in this file.
