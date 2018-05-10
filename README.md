# sgearray

usage: sgearray.py [-h] -l RESOURCE [-q QUEUE] [-P PROJECT] [-m MAXJOB]
                   [-c CUT] [-n NAME] [-v]
                   input_file

Sge array job submit and check. Authour:zhouyiqi

positional arguments:
  input_file

optional arguments:
  -h, --help            show this help message and exit<br>
  -l RESOURCE           equal to -l in qsub.Example: -l vf=1g,p=1<br>
  -q QUEUE              queue(s),equal to -q in qsub<br>
  -P PROJECT            project name,equal to -P in qsub<br>
  -m MAXJOB, --maxjob MAXJOB
  maximum number of job run simultaneously.(default=300)<br>
  -c CUT, --cut CUT     number of lines to form a job in
                        input_file.(default=1)<br>
  -n NAME, --name NAME  job name. if not set, use input_file prefix plus two
                        random characters<br>
  -v, --version         show program's version number and exit<br>
