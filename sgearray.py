#! /usr/bin/env python
import argparse

def parse_input():

	parser = argparse.ArgumentParser(description='Sge array job submit and check. \nAuthour:zhouyiqi\n')
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
		env = ""
		for line in jobfile:
			line = line.strip(" ")
			line = line.strip("\n")
			if line[0] == "?":
				env += line.strip("?")
			elif line == "":
				pass
			else:
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
    return job_number,env

def write_qsub(args,name,job_number):
    submit_sh = name+ "_all.sh"
    opts = " -o ./" + name + ".log/" + name +"_\$TASK_ID.out -e ./"\
 + name + ".log/" + name +"_\$TASK_ID.err "
    if args.project:
        opts += " -P "+ args.project
    if args.queue:
        opts += " -q "+ args.queue

    cmd = "qsub -cwd -V -l "+args.resource+opts+" -t 1-"+str(job_number)+" -tc\
 "+str(args.maxjob)+" "+submit_sh

    return cmd

def write_submit(name,env):
    submit_sh = name+ "_all.sh"
    with open(submit_sh,'w') as submit:
		submit.write(env)
		submit.write("jobid=$SGE_TASK_ID\n")
		submit.write("sh "+name+"_${jobid}.sh\n")

def getlist(dic,job_number):
	#initial
	maxv = 0.0
	minv = float("inf")
	maxid = 1
	minid = 1
	total = 0
	for key in dic:
		if dic[key] > maxv:
			maxv = dic[key]
			maxid = key
		if dic[key] < minv:
			minv = dic[key]
			minid = key
		total += dic[key]
	average = round(total/job_number,3)
	v_list=[maxv,maxid,minv,minid,average,total]
	return v_list

def summarize(name,job_number):
	log_dir=name+".log"
	use_dic = {}
	exit_code = {}
	cpu_dic = {}
	io_dic = {}
	vmem_dic = {}
	maxvmem_dic = {}
	non_zero = 0
	non_zero_list = []
	for i in range(1,job_number+1):
		with open(log_dir+"/"+name+"_"+str(i)+".err") as err:
			for line in err:
				if line.find("maxvmem") != -1 and line.find("usage") != -1:
					use_dic[i] = line
				if line.find("Job-Exit-Code:") != -1:
					exit_code[i] = line.split(":")[1].strip("\n")
	
	for key in exit_code:
		if exit_code[key] != "0":
			non_zero += 1
			non_zero_list.append(key)
	for key in use_dic:
		attr = use_dic[key].strip("\n").split(",")
		cpu = attr[0].split("=")[1]
		mem = attr[1].split("=")[1]
		io = attr[2].split("=")[1]
		vmem = attr[3].split("=")[1]
		maxvmem = attr[4].split("=")[1]
		#cpu	
		cpu_attr = cpu.split(":")
		cpu_len = len(cpu_attr)
		cpu_int = [int(i) for i in cpu_attr]

		if cpu_len == 3:
			cpu_insec = cpu_int[2] + cpu_int[1]*60 + cpu_int[0]*3600
		elif cpu_len == 4:
			cpu_insec = cpu_int[3] + cpu_int[2]*60 + cpu_int[1]*3600 + cpu_int[0]*3600*24

		cpu_inhour = round(cpu_insec/float(3600),3)
		cpu_dic[key] = cpu_inhour	
		#io
		io_float = round(float(io),3)
		io_dic[key] = io_float
		#vmem
		if vmem[-1] == "M":
			vmem_ingb = float(vmem.strip("M"))/1024
		elif vmem[-1] == "G":
			vmem_ingb = float(vmem.strip("G"))
		vmem_ingb = round(vmem_ingb,3)
		vmem_dic[key] = vmem_ingb

		if maxvmem[-1] == "M":
			max_vmem_ingb = float(maxvmem.strip("M"))/1024
		elif maxvmem[-1] == "G":
			maxvmem_ingb = float(maxvmem.strip("G"))
		maxvmem_ingb = round(maxvmem_ingb,3)
		maxvmem_dic[key] = maxvmem_ingb
	
	cpu_list = getlist(cpu_dic,job_number)
	io_list = getlist(io_dic,job_number)
	vmem_list = getlist(vmem_dic,job_number)
	maxvmem_list = getlist(maxvmem_dic,job_number)
	return non_zero,non_zero_list,cpu_list,io_list,vmem_list,maxvmem_list

def main():
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
	(job_number,env) = cutjob(args,job,name)
	write_submit(name,env)
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
	#submit
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
	all_log.write("="*50+"\n")
	os.system("mv "+name+ "_all.sh ./"+name+".log/shell")
	
	#summary
	(non_zero,non_zero_list,cpu_list,io_list,vmem_list,maxvmem_list) = summarize(name,job_number)
	all_log.write(str(non_zero)+" jobs have non-zero exit status.\n")
	if non_zero != 0:
		all_log.write("These jobid may be failed:\n")
		for failed in non_zero_list:
			all_log.write(str(failed)+" ")
	all_log.write("="*50+"\n")

	all_log.write("\tcpu(h)\tio\tvmem\tmaxvmem\n")
	item_list = ['max','max_id','min','min_id','mean','total']
	for i in range(6):
		all_log.write(item_list[i]+"\t"+str(cpu_list[i])+"\t"+str(io_list[i])+"\t"+str(vmem_list[i])+"\t"+str(maxvmem_list[i])+"\n")

	all_log.close()

if __name__=='__main__':
	main()


