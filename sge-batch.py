#! /usr/bin/env python
import argparse

def parse_input():

    parser = argparse.ArgumentParser(description='sge batch job submit. \nExample usage: sge-batch input_file')
    parser.add_argument('-l', required = True, dest = "resource", help = "equal to -l in qsub.Example: -l vf=1g,p=1")
    parser.add_argument('-q', required = False, dest = "queue", help = "queue(s),equal to -q in qsub")
    parser.add_argument('-P', required = False, dest = "project",  help = "project name,equal to -P in qsub")
    #parser.add_argument('-f', '--filelimit', required = False, dest = "filelimit", default = "500G", help = "The largest file a command can create without being killed. (Preserves fileservers.) Default: 500G")
    parser.add_argument('-m', '--maxjob', required = False, dest = "maxjob", default = "300", help = "maximum number of job run simultaneously.(default=300)")
    parser.add_argument('-c', '--cut', required = False, dest = "cut",default = "1", help = "number of lines to form a job in input_file.(default=1)")
    parser.add_argument('-n', '--name',required = False,dest = "name", help= "job name. if not set, use input_file prefix plus two random characters")
    parser.add_argument('-v', '--version', action = 'version', version = '%(prog)s 1.0')
    parser.add_argument('input_file',action="store",type=str)

    args = parser.parse_args()
    job = getattr(args, 'input_file')

    return args,job

def cutjob(args,job,name):
    #name = args.name
    lines=int(args.cut)
    job_number = 0
    index = 0
    with open(job) as jobfile:
        for line in jobfile:
            index += 1
            if index == 1:
                job_number += 1
                split_job_name = name+"_"+str(job_number)+".sh"
                split_job = open(split_job_name,'w')
                split_job.write(line)
            if index != 1:
                split_job.write(line)
            if index == lines:
                split_job.write('echo "Job-Exit-Code:"$? >&2\n')
                split_job.write('echo "This-Job-Is-Completed!" >&2\n')
                split_job.write('''qstat -xml |grep -B 5 '''+ name +\
'''|grep "<JB_job_number>.*</JB_job_number>"|\
grep -o "[0-9]*"|xargs qstat -j|grep "usage *'''+str(job_number)+''':" >&2\n''')
                split_job.write('mv $0 ./'+name+'.log/shell')
                split_job.close()
                index = 0
    return job_number

def write_qsub(args,name,job_number):
    submit_sh = name+ "_all.sh"
    opts = " -o ./" + name + ".log/" + name +"_\$TASK_ID.out -e ./"\
 + name + ".log/" + name +"_\$TASK_ID.err "
    if args.project:
        opts += "-P "+ args.project
    if args.queue:
        opts += "-q "+ args.queue

    cmd = "qsub -cwd -V -l "+args.resource+opts+" -t 1-"+str(job_number)+" -tc\
 "+str(args.maxjob)+" "+submit_sh

    return cmd

def write_submit(name):
    submit_sh = name+ "_all.sh"
    with open(submit_sh,'w') as submit:
        submit.write("jobid=$SGE_TASK_ID\n")
        submit.write("sh "+name+"_${jobid}.sh\n")


def summarize(name):
    log_dir=name+".log"



#main        
import os
import time
import random
(args,job) = parse_input()
if args.name:
    name = args.name
else:
    name = job.split(".")[0] + chr(random.randint(97,123)) + chr(random.randint(97,123))
log_dir=name+".log"
try:
    os.system("mkdir "+log_dir)
except:
    os.system("mkdir "+log_dir+"_new")
job_number = cutjob(args,job,name)
write_submit(name)
cmd = write_qsub(args,name,job_number)
try:
    os.system("mkdir "+log_dir+"/shell")
except:
    print ("Can not make shell dir.Mayby exist.")

#start
all_log = open(name+"_all.log",'w')
start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) 
start_line = "Start at: " + start_time + "\n"
all_log.write(start_line)
all_log.write("Submit command: "+cmd+"\n")
os.system(cmd)

#check jobs
while True:
    finished = 0
    for i in range(1,job_number+1):
        try:
            err = open(log_dir+"/"+name+"_"+str(i)+".err").read()
        except:
            break
        else:
            if err.find("This-Job-Is-Completed!") != -1:
                finished += 1
    if finished == job_number:
        break
    else:
        time.sleep(10)


#Finish
finish_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
finish_line="Finish at: " + finish_time + "\n"
all_log.write(finish_line)
os.system("mv "+name+ "_all.sh ./"+name+".log/shell")

all_log.close()





