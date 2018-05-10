# sgearray

usage: sgearray.py [-h] -l RESOURCE [-q QUEUE] [-P PROJECT] [-m MAXJOB]
                   [-c CUT] [-n NAME] [-v]
                   input_file

Sge array job submit and check. Authour:zhouyiqi

positional arguments:
  input_file

optional arguments:
  -h, --help            show this help message and exit
  -l RESOURCE           equal to -l in qsub.Example: -l vf=1g,p=1
  -q QUEUE              queue(s),equal to -q in qsub
  -P PROJECT            project name,equal to -P in qsub
  -m MAXJOB, --maxjob MAXJOB
                        maximum number of job run simultaneously.(default=300)
  -c CUT, --cut CUT     number of lines to form a job in
                        input_file.(default=1)
  -n NAME, --name NAME  job name. if not set, use input_file prefix plus two
                        random characters
  -v, --version         show program's version number and exit
