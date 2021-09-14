# sgearray

usage: sgearray.py [-h] -l RESOURCE [-q QUEUE] [-P PROJECT] [-m MAXJOB] [-c CUT] [-n NAME] [-v] input_file

Sge array job submit and check. Authour:zhouyiqi

positional arguments:
  input_file

optional arguments:<br>
  -h, --help            show this help message and exit  
  -l RESOURCE           equal to -l in qsub.Example: -l vf=1g,p=1  
  -q QUEUE              queue(s),equal to -q in qsub  
  -P PROJECT            project name,equal to -P in qsub  
  -m MAXJOB, --maxjob   MAXJOB maximum number of job run simultaneously.(default=300)
  -c CUT, --cut CUT     number of lines to form a job in input_file.(default=1)  
  -n NAME, --name NAME  job name. if not set, use input_file prefix plus two random characters  
  -v, --version         show program's version number and exit  

# sgesubmit
Sge many single jobs submit and check. modified:luyang@novogene.com

## feature  
  - any non-zero exit status will be recorded as a failure
  - submit many single jobs without using job-array(prevented by administrators)
  - record cpu, io, vmem and maxvmem consumption
  - any line starting with a '?' symbol will be added at the beginning of the single job shell scripts.(it is useful to set environment variables)
  - multi-line command can be splitted into a single job by using '-c' parameter
  - compatible with python2 and python3 
  - generate a xxx.sh_done file when all scripts success

## usage 
sgesubmit.py [-h] -l RESOURCE [-q QUEUE] [-P PROJECT] [-c CUT] [-n NAME] [-v] input_file


## positional arguments
 input_file

## optional arguments:<br>
  -h, --help            show this help message and exit  
  -l RESOURCE           equal to -l in qsub.Example: -l vf=1g,p=1  
  -q QUEUE              queue(s),equal to -q in qsub  
  -P PROJECT            project name,equal to -P in qsub  
  -c CUT, --cut CUT     number of lines to form a job in input_file.(default=1)  
  -n NAME, --name NAME  job name. if not set, use input_file prefix plus three random characters  
  -v, --version         show program's version number and exit   
  
## example
  - test.sh
```bash
?#!/usr/bin/env bash
?export PATH=xxxx:$PATH
touch rm1
touch fir1 | sleep 120 | rm rm1 | touch sec1
touch rm2
touch fir2 | sleep 120 | rm rm2 | touch sec2
touch rm3
touch fir3 | sleep 120 | rm rm3 | touch sec3
```
  - command
```bash
nohup python sgesubmit.py -l vf=1g,p=1 -c 2 -P xxx -q xxx.q -n xxx test.sh &
```

  - file tree
```
|-- fir1
|-- fir2
|-- fir3
|-- sec1
|-- sec2
|-- sec3
|-- test.sh
|-- test.sh_done
|-- testrrn.log
|   |-- shell
|   |   |-- testrrn_1.sh
|   |   |-- testrrn_2.sh
|   |   `-- testrrn_3.sh
|   |-- testrrn_1.err
|   |-- testrrn_1.out
|   |-- testrrn_2.err
|   |-- testrrn_2.out
|   |-- testrrn_3.err
|   `-- testrrn_3.out
`-- testrrn_all.log
```
