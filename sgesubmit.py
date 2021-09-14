#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File    : sgesubmit.py
# @Date    : 2019-02-26
# @Author  : luyang(luyang@novogene.com)


import argparse
import os
import subprocess
import sys
import time
from random import choice

try:
    from string import ascii_letters as letters
except:
    from string import letters


def parse_input():
    parser = argparse.ArgumentParser(description='Sge array job submit and check. \nAuthour:zhouyiqi\n')
    parser.add_argument('-l', required=True, dest='resource', help='equal to -l in qsub.Example: -l vf=1g,p=1')
    parser.add_argument('-q', required=False, dest='queue', help='queue(s),equal to -q in qsub')
    parser.add_argument('-P', required=False, dest='project', help='project name,equal to -P in qsub')
    # parser.add_argument('-f', '--filelimit', required = False, dest = 'filelimit', default = '500G', help = 'The largest file a command can create without being killed. (Preserves fileservers.) Default: 500G')
    parser.add_argument('-m', '--maxjob', required=False, dest='maxjob', default='300', help='maximum number of job run simultaneously.(default=300)')
    parser.add_argument('-c', '--cut', required=False, dest='cut', default='1', help='number of lines to form a job in input_file.(default=1)')
    parser.add_argument('-n', '--name', required=False, dest='name', default=None, help='job name. if not set, use input_file prefix plus two random characters')
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.1')
    parser.add_argument('input_file', action='store', type=str)
    args = parser.parse_args()
    job = getattr(args, 'input_file')
    return args, job


def cutjob(args, job, name):
    abs_path = os.getcwd()
    lines = int(args.cut)
    job_number = 0
    index = 0
    with open(job) as jobfile:
        env = ''
        for line in jobfile:
            line = line.strip(' ').strip('\n')
            if line == '':
                continue
            if line[0] == '?':
                env += ''.join([line.strip('?')])
            else:
                index += 1
                if index == 1:
                    job_number += 1
                    split_job_name = ''.join([name, '_', str(job_number), '.sh'])
                    split_job = open(split_job_name, 'w')
                    split_job.write(''.join([env, '\n']))
                    split_job.write('''set -o pipefail
function get_exit_code {{
    echo 'Job-Exit-Code:'$? >&2
    echo 'This-Job-Is-Completed!' >&2
    qstat -j {0} | grep usage >&2
    mv {2}/{0} {2}/{1}.log/shell/
    exit 0
}}
'''.format(split_job_name, name, abs_path))
                    split_job.write(''.join(['( ', line, ' )', ' || ', 'get_exit_code', '\n']))
                if index != 1:
                    split_job.write(''.join(['( ', line, ' )', ' || ', 'get_exit_code', '\n']))
                if index == lines:
                    split_job.write(''.join(['get_exit_code', '\n']))
                    split_job.close()
                    index = 0
        if index != 0:
            split_job.write(''.join(['get_exit_code', '\n']))
            split_job.close()
    return job_number, env


def write_qsub(args, name, index):
    submit_sh = ''.join([name, '_', str(index), '.sh'])
    opts = '-o ./{0}.log/{0}_{1}.out -e ./{0}.log/{0}_{1}.err'.format(name, index)
    if args.project:
        opts += ' '.join([' -P', args.project])
    if args.queue:
        opts += ' '.join([' -q', args.queue])
    cmd = 'qsub -cwd -V -l {0} {2} -N {1} {1}'.format(args.resource, submit_sh, opts)
    return cmd


def check_job(log_dir, name, job_number):
    finished = 0
    for i in range(1, job_number + 1):
        try:
            err = open(''.join([log_dir, '/', name, '_', str(i), '.err'])).read()
        except:
            break
        else:
            if err.find('This-Job-Is-Completed!') != -1:
                finished += 1
    if finished == job_number:
        return True
    else:
        return False


def getlist(dic, job_number):
    # initial
    maxid = max(dic, key=dic.get)
    minid = min(dic, key=dic.get)
    maxv = dic[maxid]
    minv = dic[minid]
    total = round(sum(dic.values()), 3)
    average = round(total / job_number, 3)
    v_list = [maxv, maxid, minv, minid, average, total]
    return v_list


def summarize(name, job_number):
    log_dir = ''.join([name, '.log'])
    use_dic = {}
    exit_code = {}
    cpu_dic = {}
    io_dic = {}
    vmem_dic = {}
    maxvmem_dic = {}
    non_zero = 0
    non_zero_list = []
    for i in range(1, job_number + 1):
        with open(''.join([log_dir, '/', name, '_', str(i), '.err'])) as err:
            for line in err:
                if line.find('maxvmem') != -1 and line.find('usage') != -1:
                    use_dic[i] = line
                if line.find('Job-Exit-Code:') != -1:
                    exit_code[i] = line.split(':')[1].strip('\n')
    for key in exit_code:
        if exit_code[key] != '0':
            non_zero += 1
            non_zero_list.append(key)
    for key in use_dic:
        attr = use_dic[key].strip('\n').split(',')
        cpu = attr[0].split('=')[1]
        mem = attr[1].split('=')[1]
        io = attr[2].split('=')[1]
        vmem = attr[3].split('=')[1]
        maxvmem = attr[4].split('=')[1]
        # cpu
        cpu_attr = cpu.split(':')
        cpu_len = len(cpu_attr)
        cpu_int = [int(i) for i in cpu_attr]

        if cpu_len == 3:
            cpu_insec = cpu_int[2] + cpu_int[1] * 60 + cpu_int[0] * 3600
        elif cpu_len == 4:
            cpu_insec = cpu_int[3] + cpu_int[2] * 60 + cpu_int[1] * 3600 + cpu_int[0] * 3600 * 24

        cpu_inhour = round(cpu_insec / float(3600), 3)
        cpu_dic[key] = cpu_inhour
        # io
        io_float = round(float(io), 3)
        io_dic[key] = io_float
        # vmem
        if vmem[-1] == 'M':
            vmem_ingb = float(vmem.strip('M')) / 1024
        elif vmem[-1] == 'G':
            vmem_ingb = float(vmem.strip('G'))
        else:
            vmem_ingb = 0.0
        vmem_ingb = round(vmem_ingb, 3)
        vmem_dic[key] = vmem_ingb

        if maxvmem[-1] == 'M':
            maxvmem_ingb = float(maxvmem.strip('M')) / 1024
        elif maxvmem[-1] == 'G':
            maxvmem_ingb = float(maxvmem.strip('G'))
        else:
            maxvmem_ingb = 0.0
        maxvmem_ingb = round(maxvmem_ingb, 3)
        maxvmem_dic[key] = maxvmem_ingb

    cpu_list = getlist(cpu_dic, job_number)
    io_list = getlist(io_dic, job_number)
    vmem_list = getlist(vmem_dic, job_number)
    maxvmem_list = getlist(maxvmem_dic, job_number)
    return non_zero, non_zero_list, cpu_list, io_list, vmem_list, maxvmem_list


def main():
    args, job = parse_input()
    if args.name != None:
        name = args.name
    else:
        name = ''.join([os.path.basename(job).split('.')[0], choice(letters), choice(letters), choice(letters)])
    os.chdir(os.path.abspath(os.path.dirname(job)))
    log_dir = ''.join([name, '.log'])
    while True:
        if os.path.exists(''.join([name, '.log'])):
            name = ''.join([os.path.basename(job).split('.')[0], choice(letters), choice(letters), choice(letters)])
        else:
            os.mkdir(''.join([name, '.log']))
            break
    job_number, env = cutjob(args, job, name)
    # prepare cmd
    cmd_list = [write_qsub(args, name, i) for i in range(1, job_number + 1)]
    try:
        os.mkdir(log_dir + '/shell')
    except:
        print('Can not make shell dir.Mayby exist.')

    # start
    all_log = open(name + '_all.log', 'w')
    all_log.write(''.join(['Start at: ', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), '\n']))
    # submit
    for cmd in cmd_list:
        all_log.write('Submit command: ' + cmd + '\n')
        try:
            subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            print('{0} cannot excute'.format(cmd))
            sys.exit(-1)
        except:
            print('Other unclassified error')
            sys.exit(-1)

    # check jobs
    while True:
        if check_job(log_dir, name, job_number):
            break
        else:
            job_status = os.popen('qstat -xml|grep -c ' + name).readlines()[0]
            if int(job_status.strip()) == 0:
                # wait for 30s and check again
                time.sleep(30)
                if not check_job(log_dir, name, job_number):
                    print('no ' + name + ' job found in cluster! Maybe qdel,sgearray will exit with -1 status')
                    sys.exit(-1)
                else:
                    break
            time.sleep(60)

    # Finish
    all_log.write(''.join(['Finish at: ', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), '\n']))
    all_log.write(''.join(['=' * 50, '\n']))

    # summary
    non_zero, non_zero_list, cpu_list, io_list, vmem_list, maxvmem_list = summarize(name, job_number)
    all_log.write(''.join([str(non_zero), ' jobs have non-zero exit status.\n']))
    if non_zero != 0:
        all_log.write('These jobid may be failed:\n')
        for failed in non_zero_list:
            all_log.write(str(failed) + ' ')
    else:
        with open('_'.join([os.path.basename(job), 'done']), 'w') as f:
            f.write('Done')
    all_log.write(''.join(['\n', '=' * 50, '\n']))
    all_log.write('\t\tcpu(h)\t\tio\t\tvmem(G)\t\tmaxvmem(G)\n')
    item_list = ['max', 'max_id', 'min', 'min_id', 'mean', 'total']
    for i in range(6):
        all_log.write(item_list[i] + '\t\t' + str(cpu_list[i]) + '\t\t' + str(io_list[i]) + '\t\t' + str(vmem_list[i]) + '\t\t' + str(maxvmem_list[i]) + '\n')
    all_log.close()
    if non_zero != 0:
        print('sgearray exit with 1 status')
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    main()
