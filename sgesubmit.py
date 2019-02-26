#! /usr/bin/env python
import argparse
import os
import sys
import subprocess
import time
from random import choice
import re

try:
    from string import ascii_letters as letters
except:
    from string import letters


def parse_input():
    parser = argparse.ArgumentParser(description='Sge array job submit and check. \nAuthour:zhouyiqi\n')
    parser.add_argument('-l', required=True, dest="resource", help="equal to -l in qsub.Example: -l vf=1g,p=1")
    parser.add_argument('-q', required=False, dest="queue", help="queue(s),equal to -q in qsub")
    parser.add_argument('-P', required=False, dest="project", help="project name,equal to -P in qsub")
    # parser.add_argument('-f', '--filelimit', required = False, dest = "filelimit", default = "500G", help = "The largest file a command can create without being killed. (Preserves fileservers.) Default: 500G")
    parser.add_argument('-m', '--maxjob', required=False, dest="maxjob", default="300", help="maximum number of job run simultaneously.(default=300)")
    parser.add_argument('-c', '--cut', required=False, dest="cut", default="1", help="number of lines to form a job in input_file.(default=1)")
    parser.add_argument('-n', '--name', required=False, dest="name", default=None, help="job name. if not set, use input_file prefix plus two random characters")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')
    parser.add_argument('input_file', action="store", type=str)
    args = parser.parse_args()
    job = getattr(args, 'input_file')
    return args, job


def cutjob(args, job, name):
    abs_path = os.getcwd()
    lines = int(args.cut)
    job_number = 0
    index = 0
    with open(job) as jobfile:
        env = ""
        for line in jobfile:
            line = line.strip(" ").strip('\n')
            if line == "":
                continue
            if line[0] == "?":
                env += ''.join([line.strip("?"), '\n'])
            else:
                index += 1
                if index == 1:
                    job_number += 1
                    split_job_name = ''.join([name, '_', str(job_number), '.sh'])
                    split_job = open(split_job_name, 'w')
                    split_job.write(''.join([env, '\n', line, '\n']))
                if index != 1:
                    split_job.write(line + "\n")
                if index == lines:
                    split_job.write('''
echo "Job-Exit-Code:"$? >&2
echo "This-Job-Is-Completed!" >&2
qstat -j {0}_{1}.sh | grep usage >&2
mv {2}/$0 {2}/{1}.log/shell
'''.format(name, job_number, abs_path))
                    split_job.close()
                    index = 0
        if index != 0:
            split_job.write('''
echo "Job-Exit-Code:"$? >&2
echo "This-Job-Is-Completed!" >&2
qstat -j {0}_{1}.sh | grep usage >&2
mv {2}/$0 {2}/{1}.log/shell
'''.format(name, job_number, abs_path))
            split_job.close()
    return job_number, env


def write_qsub(args, name, index):
    submit_sh = ''.join([name, '_', str(index + 1), '.sh'])
    opts = '-o ./{0}.log/{0}_{1}.out -e ./{0}.log/{0}_{1}.err'.format(name, index + 1)
    if args.project:
        opts += ' '.join(['-P', args.project])
    if args.queue:
        opts += ' '.join(['-q', args.queue])
    cmd = 'qsub -cwd -V -l {0} opts -n {1} {1}'.format(args.resource, submit_sh)
    return cmd


def getlist():
    pass


def summarize():
    pass


def main():
    (args, job) = parse_input()
    if args.name != None:
        name = args.name
    else:
        name = ''.join([os.path.basename(job).split(".")[0], choice(letters), choice(letters), choice(letters)])
    log_dir = ''.join([name, '.log'])
    try:
        os.mkdir(''.join([name, '.log']), mode=0o777)
    except:
        name = ''.join([os.path.basename(job).split(".")[0], choice(letters), choice(letters), choice(letters)])
        os.mkdir(''.join([name, '.log']), mode=0o777)
    job_number, env = cutjob(args, job, name)
    # prepare cmd
    cmd_list = [write_qsub(args, name, i) for i in range(job_number)]
    try:
        os.mkdir(log_dir + '/shell')
    except:
        print("Can not make shell dir.Mayby exist.")

    # start
    all_log = open(name + "_all.log", 'w')
    all_log.write(''.join(['Start at: ', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n']))
    # submit
    for cmd in cmd_list:
        all_log.write("Submit command: " + cmd + "\n")
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            print('{0} cannot excute'.format(cmd))
            sys.exit(-1)
        except:
            print('Other unclassified error')
            sys.exit(-1)

    # check jobs


if __name__ == '__main__':
    main()
