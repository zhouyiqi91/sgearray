#! /usr/bin/env python
import argparse

def parse_input():

	parser = argparse.ArgumentParser(description='sge batch job submit. \nExample usage: sge-batch work.sh')
	parser.add_argument('-l', required = True, dest = "resource", help = "equal to -l in qsub.Example: -l vf=1g,p=1")
	parser.add_argument('-q', '--queue', required = False, dest = "queue", help = "queue(s),equal to -q in qsub")
	parser.add_argument('-P', '--Project', required = False, dest = "project",  help = "project name,equal to -P in qsub")
	#parser.add_argument('-f', '--filelimit', required = False, dest = "filelimit", default = "500G", help = "The largest file a command can create without being killed. (Preserves fileservers.) Default: 500G")
	parser.add_argument('-m', '--maxjob', required = False, dest = "maxjob", default = "300", help = "maximum number of job run simultaneously.(default=300)")
	parser.add_argument('-c', '--cut', required = False, dest = "cut",default = "1", help = "number of lines to form a job in work.sh.(default=1)")
	parser.add_argument('-n', '--name',required = False,dest = "name", help= "job name. if not set, use a random 6-character name")
	parser.add_argument('-v', '--version', action = 'version', version = '%(prog)s 1.0')
	parser.add_argument('work.sh',action="store",type=str)
	
	args = parser.parse_args()
	job = getattr(args, 'work.sh')
	
	return args,job

def cutjob()
def write_qsub(args,submit_sh,jobnumber):
	c = args.cut
	
	
(args,job) = parse_input()


