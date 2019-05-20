# DXRAM dxa-pageRank
This DXRAM application provides a pageRank implementaion for the in-memory key-value Store 
[DXRAM](https://github.com/hhu-bsinfo/dxram/).

# License
Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems.
Licensed under the [GNU General Public License](LICENSE.md).

# Installation
## DXRAM / DXapps / DXterminal
- clone the [DXRAM](https://github.com/hhu-bsinfo/dxram/) repository and follow the documentation [here](https://github.com/hhu-bsinfo/dxram/blob/master/doc/QuickStart.md). dxa-pageRank is working with version 0.7.0
- optionally clone the [DXapps] repository and follow the instructions [here](https://github.com/hhu-bsinfo/dxram/blob/master/doc/Applications.md) on how to build and deploy. Build the terminal Application to deploy applications via the Terminal. Instructions [here](https://github.com/hhu-bsinfo/dxapps/blob/master/dxa-terminal/README.md)

## cdepl
- to deploy DXRAM instances via cdepl clone the [repository](https://github.com/hhu-bsinfo/cdepl) and follow the documentation [here](https://github.com/hhu-bsinfo/cdepl/blob/master/README.md).

## dxa-pageRank
- clone the repository and build with:
```
build.sh
```
- the project jar will be copied into the *dxram/plugins* folder and can be started via the terminal or autostart.

# Run DXRAM
deploy DXRAM instace via cdepl with --ms-sevice and --terminal Parameter:
```
cdepl> run dxram --terminal --superpeers 1 --peers 3 --storage 1024 --handler 2--ms-service
```

# Run dxa-pageRank: Parameters
dxa-pageRanks needs the following parameters in this order to run correctly:

- Vertexcount: (int) Number of vertices in the Graphfile or snythetic Graph
- Damping Factor: (double) pageRank damping value, used for the calculation
- Threshold: (double) Error Threshold when the Algorithm converges
- max Rounds: (int) maximum Number of pageRank Iterations
- print pageRanks: (boolean) Create the output Files for the final pageRank values
Either (Graphfile):
- Graphfile: (String) Path to the Graphfile
Or (Synthetic Graph):
- Locality: (double) Degree of grap partioning
- mean indegree: (int) Expected value of the exponential distribution of indegrees
- random Seed: (int) random Seed for the Graph creation

Example call from DXterminal:
```
apprun ~/dxram/plugins/dxa-pageRank.jar 1000000 0.85 1e-5 50 true 0.6 6 21
```

# Graph Input Format
List of incoming vertices. Ordered numerically, starting from 1 (line 1 is vertex 1). 0 means no incoming edges.
```
 2 3 4 5
 3 4
 5
 0
 3 6 1
 2 4
 3 5
```
# Output
Output folder: *~/dxa-pageRank_out/$current_run$*
- If print pageRanks is true: Every Slave Peer will print to *$NodeID$.pageRank* the final pageRank values
- statistics.out file with Informations about the Run. Example:
```
NUM_SLAVES	2
NUM_VERTICES	10000000
NUM_EDGES	55138504
DAMPING_VAL	0.85
THRESHOLD	0.001
LOCALITY	0.8
MEAN_INDEG	6
NUM_ROUNDS	10
INPUT_TIME	52.9460s
EXECUTION_TIME	657.3241s

MEM_USAGE	755.9158MB
--------ROUNDS--------
Round	Error	Time
1	0.149395704651	96.8942s
2	0.846578420143	64.0615s
3	0.586333265711	60.6574s
4	0.201951943084	63.5613s
5	0.078345148038	61.5563s
6	0.031342536528	63.8611s
7	0.012660532731	60.2535s
8	0.005131265884	62.7613s
9	0.002084785990	60.7562s
10	0.000847273356	62.9613s
```
