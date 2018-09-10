# Experiment Running Guide for OMQ

The journal paper explaining how OMQ works are still in process of writing and internal review.
In the paper, we conducted an experiment for OMQ running in two different scenario :
  * Benchmark: Using a synthesized data to measure througput (sensor input/second) OMQ can process with a given number of simultaneous queries and different types of query. In this scenario, we compared the performance of OMQ running independently inside x86-based machine and ARM SBC (Single-Board Computer) machine.
  * Small IoT Deployment: Using four IoT nodes installed with oneM2M CSE and three sensors (RAM usage, CPU usage, CPU temp of node's SOC), we run several queries to the IoT system with and without OMQ/QE installed.
  
This document will guides the reader on how to reproduce the experiment as it is explained in the journal paper.

## Before running experiment

All experiment code is written in Node.js and located inside `/experiment` directory in the main repo. Before running these scripts, make sure you already installed Node.js inside the target system and perform `npm install` inside `/experiment` directory to install required packages.

## Scenario 1: Benchmark

To run the benchmark scenario in either x86 or ARM-based PC, follow these steps in the target computer:
  - Decide on number of simultaneous queries (1,10,100,500,1000), query type (q11, q12, q13), and number of repeated tests.
  - Make sure the target computer is already running the Main QE software (even in IoT node, it should be installed with the main QE instead of local QE for this benchmark purpose). There is an installation instruction in README.md for installation. Note that this benchmark does not uses any data from CSE so there is no specific configuration neede for the CSE.
    - However, before running the main QE, change QE's `config.json` configuration for number of simultaneous queries and number of repeated tests.
  - Open directory `/experiment/benchmark` from main repo.
  - Extract `trafficCombined.csv.zip` into `trafficCombined.zip`. This data is taken from some subset of Citypulse dataset for traffic.
  - Open `test_tput_with_dataset.js` and edit some configurable variables, such as:
    - `n_queries`: Number of simultaneous queries
    - `n_test`: Number of repeated test
    - `n_src`: Number of data sources per query
    - `type_of_query`: Choose among q11, q12, q13
  - Run `node test.js`
  - The result should be shown by the QE, not the test script.
  - If the interval between test is too fast, you can set it on the same script.

## Scenario 2: Small IoT Deployment

  - To reproduce this scenario, you need to prepare four Raspberry Pis (for IoT nodes) and one x86 Linux-based PC (for infrastructure node/IN) connected in a single network. Then, set the IP of each network into: `192.168.0.100` for the IN and `192.168.0.101-192.168.0.104` for all IoT nodes.
  - Make sure local QE are installed in all IoT nodes and the main QE are installed in the IN. Follow README.md for the detailed installation instruction.
    - However, each IoT node need to be installed with a specialized version of CSE (you can find it in: `/experiment/iotdeployment/mn-mobius.zip`). This customized CSE adds a timestamping into each notification so we can measure delay correctly.
  - Install a specialized AE in each IoT nodes. The AE is in the file `/experiment/iotdeployment/soc-monitoring-ae.zip`.
    - Don't forget to configure CSE location in `config.json`.
    - Run the AE by using `./things.js` command. This AE will register itself automatically into the CSE automatically.
  - Copy `netmon.py` script into home directory of each IoT nodes.
  - Make sure that IN can perform SSH without any password into all four IoT nodes. You can find tutorial on how to achieve this in Google.
  - Run the `node executor.js <type-of-experiment>` from IN. There are two type of experiments:
    - `cse`: Without using QE
    - `qe`: Using QE
  - All experiment data will be put into `plot` directory.
  - To generate plot, enter the `plot` directory and run `gnuplot plot_all_2.plt`.


    
