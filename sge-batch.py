#! /usr/bin/env python
def parse_input():
	
	parser = argparse.ArgumentParser(description='Runs a list of commands specified on stdin as an SGE array job. \nExample usage: cat `commands.txt | SGE_Array` or `SGE_Array -c commands.txt`')
	parser.add_argument('-c', '--commandsfile', required = False, dest = "commandsfile", default = "-", help = "The file to read commands from. Default: -, meaning standard input.")
	parser.add_argument('-q', '--queue', required = False, dest = "queue", help = "The queue(s) to send the commands to. Default: all queues you have access to.")
	parser.add_argument('-m', '--memory', required = False, dest = "memory", default = "4G", help = "Amount of free RAM to request for each command, and the maximum that each can use without being killed. Default: 4G")
	parser.add_argument('-f', '--filelimit', required = False, dest = "filelimit", default = "500G", help = "The largest file a command can create without being killed. (Preserves fileservers.) Default: 500G")
	parser.add_argument('-b', '--concurrency', required = False, dest = "concurrency", default = "50", help = "Maximum number of commands that can be run simultaneously across any number of machines. (Preserves network resources.) Default: 50")
	parser.add_argument('-P', '--processors', required = False, dest = "processors", default = "1", help = "Number of processors to reserve for each command. Default: 1")
	parser.add_argument('-r', '--rundir', required = False, dest = "rundir", help = "Job name and the directory to create or OVERWRITE to store log information and standard output of the commands. Default: 'jYEAR-MON-DAY_HOUR-MIN-SEC_<cmd>_etal' where <cmd> is the first word of the first command.")
	parser.add_argument('-p', '--path', required = False, dest = "path", help = "What to use as the PATH for the commands. Default: whatever is output by echo $PATH.")
	parser.add_argument('--hold', required = False, action = 'store_true', dest = "hold", help = "Hold the execution for these commands until all previous jobs arrays run from this directory have finished. Uses the list of jobs as logged to .sge_array_jobnums.")
	parser.add_argument('--hold_jids', required = False, dest = "hold_jid_list", help = "Hold the execution for these commands until these specific job IDs have finished (e.g. '--hold_jid 151235' or '--hold_jid 151235,151239' )")
	parser.add_argument('--hold_names', required = False, dest = "hold_name_list", help = "Hold the execution for these commands until these specific job names have finished (comma-sep list); accepts regular expressions. (e.g. 'SGE_Array -c commands.txt -r this_job_name --hold_names previous_job_name,other_jobs_.+'). Uses job information as logged to .sge_array_jobnums.")
	parser.add_argument('-v', '--version', action = 'version', version = '%(prog)s 0.6.8')
	parser.add_argument('--showchangelog', required = False, action = 'store_true', dest = "showchangelog", help = "Show the changelog for this program.")

	changelog = textwrap.dedent('''\
		Version 0.6.8.1: Fixed bug so that -r option strips trailing slashes properly; e.g. -r log_dir/ now works properly
		Version 0.6.8: --hold_names option now accepts regular expressions for holding against sets of jobs easily. Eg. --hold_names assembly_.+
		Version 0.6.7.1: Fixed the -r option to now accept paths. e.g SGE_Array -c commands.txt -r logs_dir/log_dir. The "name" of the job (for --hold_names purposes) is logs_dir/log_dir; the SGE name is just log_dir.
		Version 0.6.7: Added new option --hold_names for holding for specific job names.
		Version 0.6.6: Added new option --hold_jid for holding for specific job ids (in addition to --hold which holds for all jobs previously run in the current dir.)
		Version 0.6.5: Fixed some bugs, also, new option --hold
		Version 0.6: Initial version. Reads commands on stdin or from a file, runs them as an array job.
		''')

