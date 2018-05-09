#! /usr/bin/env perl 

=head1 Name

qsub-sge.pl -- control processes running on linux SGE system

=head1 Description

=head1 Version

  Author: Fan Wei, fanw@genomics.org.cn
  Author: Hu Yujie  huyj@genomics.org.cn
  modified:Zhou yiqi
  Version: 1.0,  Date: 2018-05-09

=head1 Usage

  perl qsub-sge.pl <jobs.txt>
  --global          only output the global shell, but do not excute
  -q <str>     specify the queue to use, default all availabile queues
  -P <str>   project name
  --check <num>  set interval time of checking by qstat, default 300 seconds
  --lines <num>     set number of lines to form a job, default 1
  --maxjob <num>    set the maximum number of jobs to throw out, default 30
  --convert <yes/no>   convert local path to absolute path, default yes
  --secure <mark>   set the user defined job completition mark, default no need
  --reqsub          reqsub the unfinished jobs untill they are finished, default no
  -l <str>  set the required resource used in qsub -l option, default vf=1.2G
  --name <str> set the prefix tag for qsubed jobs, default work
  --verbose         output verbose information to screen
  --help            output help information to screen

  --getmem          output the usage (example: cpu=00:26:45, mem=111.63317 GBs, io=0.00000, vmem=259.148M, maxvmem=315.496M);

=head1 Exmple

	perl qsub-sge.pl -l vf=1G,p=1 -P aliyun -q alyun.q --lines 2 --maxjob 100 --check 20 run.sh

=cut


use strict;
use Getopt::Long;
use FindBin qw($Bin $Script);
use File::Basename qw(basename dirname);
use File::Path qw(rmtree);
use Cwd qw(abs_path);
use Data::Dumper;

##get options from command line into variables and set default values
my ($Global,$Queue, $Interval, $Lines, $Maxjob, $Convert,$Secure,$Reqsub,$Resource,$Job_prefix,$Verbose, $Help, $getmem, $Project);
GetOptions(
	"global"=>\$Global,
	"lines:i"=>\$Lines,
	"maxjob:i"=>\$Maxjob,
	"check:i"=>\$Interval,
	"q:s"=>\$Queue,
	"P:s"=>\$Project,
	"convert:s"=>\$Convert,
	"secure:s"=>\$Secure,
	"reqsub"=>\$Reqsub,
	"l:s"=>\$Resource,
	"name:s"=>\$Job_prefix,
	"verbose"=>\$Verbose,
	"help"=>\$Help,
	"getmem"=>\$getmem,
);
$Queue ||= "gr.q";
$Interval ||= 300;
$Lines ||= 1;
$Maxjob ||= 30;
$Convert ||= 'yes';
$Resource ||= "vf=1.2G";
$Job_prefix ||= "work";
die `pod2text $0` if (@ARGV == 0 || $Help);

my $work_shell_file = shift;

##global variables
my $work_shell_file_globle = $work_shell_file.".$$.globle";
my $work_shell_file_error = $work_shell_file.".$$.log";
my $Work_dir = $work_shell_file.".$$.qsub";
my $current_dir = abs_path(".");

### get mem ;  add by nixiaoming nixiaoming@genomics.cn
my $work_shell_mem = $work_shell_file.".$$.mem.log";
my %meminfo=();
open GETMEM,'>',$work_shell_mem or die "can't open the mem info $work_shell_mem ";

my $whoami=`whoami`;
chomp($whoami);
	print GETMEM "User:\t\t$whoami\nShellPath:\t$current_dir/$Work_dir\n";
close GETMEM;
###

if ($Convert =~ /y/i) {
	absolute_path($work_shell_file,$work_shell_file_globle);
}else{
	$work_shell_file_globle = $work_shell_file;
}

if (defined $Global) {
	exit();
}

my $time='`date +%F'."'  '".'%H:%M`'; ### add by nixiaoming

## read from input file, make the qsub shell files
my $line_mark = 0;
my $Job_mark="00001";
mkdir($Work_dir);
my @Shell;  ## store the file names of qsub sell
open IN, $work_shell_file_globle || die "fail open $work_shell_file_globle";
while(<IN>){
	chomp;
	#s/&/;/g;
	next unless($_);
	if ($line_mark % $Lines == 0) {
		open OUT,">$Work_dir/$Job_prefix\_$Job_mark.sh" || die "failed creat $Job_prefix\_$Job_mark.sh";
		print OUT 'echo start at time '.$time."\n";
		push @Shell,"$Job_prefix\_$Job_mark.sh";
		$Job_mark++;
	}
	s/;\s*$//;  ##delete the last character ";", because two ";;" characters will cause error in qsub
	s/;\s*;/;/g;
	print OUT $_.' &&  echo This-Work-is-Completed!'."\n";
	#print OUT $_."; echo This-Work-is-Completed!\n";

	if ($line_mark % $Lines == $Lines - 1) {
		print OUT 'echo finish at time '.$time."\n";
		close OUT;
	}

	$line_mark++;
}
close IN;
close OUT;


print STDERR "make the qsub shell files done\n" if($Verbose);


## run jobs by qsub, until all the jobs are really finished
my $qsub_cycle = 1;
while (@Shell) {

	## throw jobs by qsub
	##we think the jobs on died nodes are unfinished jobs
	my %Alljob; ## store all the job IDs of this cycle
	my %Runjob; ## store the real running job IDs of this cycle
	my %Error;  ## store the unfinished jobs of this cycle
	chdir($Work_dir); ##enter into the qsub working directoy
	my $job_cmd = "qsub -cwd -V -S /bin/sh ";  ## -l h_vmem=16G,s_core=8
	if (-d "/PROJ")
	{
		$job_cmd = "qsub -cwd -V -S /bin/bash ";  ## -l h_vmem=16G,s_core=8
	}
	$job_cmd .= "-q $Queue "  if(defined $Queue); ##set queue
	$job_cmd .= "-P $Project " if(defined $Project);
	my @resources=split /,/,$Resource;   # fixed by hezengquan 2009-10-19 allowed different resources
	die "no valid resource\n" if(@resources<1);
	my $part_resource;
	$job_cmd .= "-l $Resource " if(defined $Resource); ##set resource
	##warn $job_cmd;
	print STDERR "$job_cmd";

	for (my $i=0; $i<@Shell; $i++) {
		while (1) {
			my $run_num = run_count(\%Alljob,\%Runjob,\%meminfo);
			if ($i < $Maxjob || ($run_num != -1 && $run_num < $Maxjob) ) {
				if(@resources>0){
					$part_resource=shift @resources;
				}

                                ## multiple resources for one line
                                my @part_res_arr = split ':',$part_resource;
                                my $part_res_str = '';
                                foreach my $cur_res (@part_res_arr) {
                                        $part_res_str.=" -l $cur_res";
                                }
                                ## multiple resources for one line

				my $jod_return = `$job_cmd$part_res_str $Shell[$i]`;
				my $job_id = $1 if($jod_return =~ /Your job (\d+)/);
				$Alljob{$job_id} = $Shell[$i];  ## job id => shell file name
				print STDERR "throw job $job_id in the $qsub_cycle cycle\n" if($Verbose);	
				last;
			}else{
				print STDERR "wait for throwing next job in the $qsub_cycle cycle\n" if($Verbose);
				sleep $Interval;
			}
		}
	}
	chdir($current_dir); ##return into original directory


	###waiting for all jobs fininshed
	while (1) {
		my $run_num = run_count(\%Alljob,\%Runjob,\%meminfo);
		last if($run_num == 0);
		print STDERR "There left $run_num jobs runing in the $qsub_cycle cycle\n" if(defined $Verbose);
		
		if(defined $getmem){ ### get mem ;  add by nixiaoming nixiaoming@genomics.cn
			open GETMEM,'>',$work_shell_mem or die "can't open the mem info $work_shell_mem ";
			print GETMEM "User:\t\t$whoami\nShellPath:\t$current_dir/$Work_dir\n";
			foreach my $shname (sort keys %meminfo){
				my $jobinfo=$meminfo{$shname};
				chomp $jobinfo;
				$jobinfo =~ s/usage\s*\w*:\s*//g;
				print GETMEM "$whoami\t$shname\t$jobinfo\n";
			}
			close GETMEM;
		}
		
		sleep $Interval;
	}

	print STDERR "All jobs finished, in the firt cycle in the $qsub_cycle cycle\n" if($Verbose);


	##run the secure mechanism to make sure all the jobs are really completed
	open OUT, ">>$work_shell_file_error" || die "fail create $$work_shell_file_error";
	chdir($Work_dir); ##enter into the qsub working directoy
	foreach my $job_id (sort keys %Alljob) {
		my $shell_file = $Alljob{$job_id};

		##read the .o file
		my $content;
		if (-f "$shell_file.o$job_id") {
			#open IN,"$shell_file.o$job_id" || warn "fail $shell_file.o$job_id";
			#$content = join("",<IN>);
			#close IN;
			$content = `tail -n 1000 $shell_file.o$job_id`;
		}
		##check whether the job has been killed during running time
		if ($content !~ /This-Work-is-Completed!/) {
			$Error{$job_id} = $shell_file;
			print OUT "In qsub cycle $qsub_cycle, In $shell_file.o$job_id,  \"This-Work-is-Completed!\" is not found, so this work may be unfinished\n";
		}


		##read the .e file
		my $content;
		if (-f "$shell_file.e$job_id") {
			#open IN,"$shell_file.e$job_id" || warn "fail $shell_file.e$job_id";
			#$content = join("",<IN>);
			#close IN;
			$content = `tail  -n 1000 $shell_file.e$job_id`;
		}
		##check whether the C/C++ libary is in good state
		if ($content =~ /GLIBCXX_3.4.9/ && $content =~ /not found/) {
			$Error{$job_id} = $shell_file;
			print OUT "In qsub cycle $qsub_cycle, In $shell_file.e$job_id,  GLIBCXX_3.4.9 not found, so this work may be unfinished\n";
		}

		##check whether iprscan is in good state
		if ($content =~ /iprscan: failed/) {
			$Error{$job_id} = $shell_file;
			print OUT "In qsub cycle $qsub_cycle, In $shell_file.e$job_id, iprscan: failed , so this work may be unfinished\n";
		}

		##check the user defined job completion mark
		if (defined $Secure && $content !~ /$Secure/) {
			$Error{$job_id} = $shell_file;
			print OUT "In qsub cycle $qsub_cycle, In $shell_file.o$job_id,  \"$Secure\" is not found, so this work may be unfinished\n";
		}
	}

	##make @shell for next cycle, which contains unfinished tasks
	@Shell = ();
	foreach my $job_id (sort keys %Error) {
		my $shell_file = $Error{$job_id};
		push @Shell,$shell_file;
	}

	$qsub_cycle++;
	if($qsub_cycle > 10000){
		print OUT "\n\nProgram stopped because the reqsub cycle number has reached 10000, the following jobs unfinished:\n";
		foreach my $job_id (sort keys %Error) {
			my $shell_file = $Error{$job_id};
			print OUT $shell_file."\n";
		}
		print OUT "Please check carefully for what errors happen, and redo the work, good luck!";
		die "\nProgram stopped because the reqsub cycle number has reached 10000\n";
	}

	print OUT "All jobs finished!\n" unless(@Shell);

	chdir($current_dir); ##return into original directory
	close OUT;
	print STDERR "The secure mechanism is performed in the $qsub_cycle cycle\n" if($Verbose);

	last unless(defined $Reqsub);
}

if(defined $getmem){ ### get mem ;  add by nixiaoming nixiaoming@genomics.cn
	open GETMEM,'>',$work_shell_mem or die "can't open the mem info $work_shell_mem ";
	print GETMEM "User:\t\t$whoami\nShellPath:\t$current_dir/$Work_dir\n";
	foreach my $shname (sort keys %meminfo){
		my $jobinfo=$meminfo{$shname};
		chomp $jobinfo;
		$jobinfo =~ s/usage\s*\w*:\s*//g;
		print GETMEM "$whoami\t$shname\t$jobinfo\n";
	}
	close GETMEM;
}

#my $gather="/ifshk1/DGE_SR/nixiaoming/work/job.mem.info/";
#if (-d "/ifs1/DGE_SR/")
#{
#	$gather="/ifs1/DGE_SR/nixiaoming/work/job.mem.info/";
#}
my $date=` date +"%Y/week%W"`;
chomp $date;
#if (! -e "$gather/$date" )
#{
#	my $dir = dirname("$gather/$date");
#	system "mkdir -m 777 $dir 2> /dev/null " unless (-d $dir);
#	system "touch $gather/$date 2> /dev/null  ";
#	system "chmod 777 $gather/$date 2> /dev/null ";
#}
my $endtime=` date +"%F %T" `;
chomp $endtime;
#system "echo $endtime  >> $gather/$date 2>  /dev/null ";
#system" cat $work_shell_mem >> $gather/$date  2> /dev/null ";
print STDERR "\nqsub-sge.pl finished\n" if($Verbose);


####################################################
################### Sub Routines ###################
####################################################

sub absolute_path{
	my($in_file,$out_file)=@_;
	my($current_path,$shell_absolute_path);

	#get the current path ;
	$current_path=abs_path(".");

	#get the absolute path of the input shell file;
	if ($in_file=~/([^\/]+)$/) {
		my $shell_local_path=$`;
		if ($in_file=~/^\//) {
			$shell_absolute_path = $shell_local_path;
		}
		else{$shell_absolute_path="$current_path"."/"."$shell_local_path";}
	}

	#change all the local path of programs in the input shell file;
	open (IN,"$in_file");
	open (OUT,">$out_file");
	while (<IN>) {
		chomp;
		##s/>/> /; ##convert ">out.txt" to "> out.txt"
		##s/2>/2> /; ##convert "2>out.txt" to "2> out.txt"
		my @words=split /\s+/, $_;

		##improve the command, add "./" automatically
		for (my $i=1; $i<@words; $i++) {
			if ($words[$i] !~ /\//) {
				if (-f $words[$i]) {
					$words[$i] = "./$words[$i]";
				}elsif($words[$i-1] eq ">" || $words[$i-1] eq "2>"){
					$words[$i] = "./$words[$i]";
				}
			}

		}
		for (my $i=0;$i<@words ;$i++) {
			if (($words[$i]!~/^\//) && ($words[$i]=~/\//)) {
				$words[$i]= "$shell_absolute_path"."$words[$i]";
			}
		}
		print OUT join("  ", @words), "\n";
	}
	close IN;
	close OUT;
}


##get the IDs and count the number of running jobs
##the All job list and user id are used to make sure that the job id belongs to this program
##add a function to detect jobs on the died computing nodes.
sub run_count {
	my $all_p = shift;
	my $run_p = shift;
	my $memlist = shift;
	my $run_num = 0;

	%$run_p = ();
	my $user = $ENV{"USER"} || $ENV{"USERNAME"};
	my $qstat_result = `qstat -u $user`;
	$user = substr($user, 0, 12);
	if ($qstat_result =~ /failed receiving gdi request/) {
		$run_num = -1;
		return $run_num; ##系统无反应
	}
	my @jobs = split /\n/,$qstat_result;
	my %died;
	died_nodes(\%died) if (@jobs > 0); ##the compute node is down, 有的时候节点已死，但仍然是正常状态
	foreach my $job_line (@jobs) {
		$job_line =~s/^\s+//;
		my @job_field = split /\s+/,$job_line;
		next if($job_field[3] ne $user);
		if (exists $all_p->{$job_field[0]}){
			my $node_name = $1 if($job_field[7] =~ /(compute-\d+-\d+)/);
			if ( !exists $died{$node_name} && ($job_field[4] eq "qw" || $job_field[4] eq "r" || $job_field[4] eq "t") ) {
				$run_p->{$job_field[0]} = $job_field[2]; ##job id => shell file name
				$run_num++;
				if ((defined $getmem) && ($job_field[4] eq "r")){### get mem ;  add by nixiaoming nixiaoming@genomics.cn
					my $jobinfo=`qstat -j $job_field[0] 2>&1 |grep usage `;
					$$memlist{$all_p->{$job_field[0]}}=$jobinfo;
				}
			}else{
				`qdel $job_field[0]`;
			}
		}
	}

	return $run_num; ##qstat结果中的处于正常运行状态的任务，不包含那些在已死掉节点上的僵尸任务
}


##HOSTNAME                ARCH         NCPU  LOAD  MEMTOT  MEMUSE  SWAPTO  SWAPUS
##compute-0-24 lx26-amd64 8 - 15.6G - 996.2M -
sub died_nodes{
	my $died_p = shift;

	my @lines = split /\n/,`qhost`;
	shift @lines for (1 .. 3); ##remove the first three title lines

	foreach  (@lines) {
		my @t = split /\s+/;
		my $node_name = $t[0];
		my $memory_use = $t[5];
		$died_p->{$node_name} = 1 if($t[3]=~/-/ || $t[4]=~/-/ || $t[5]=~/-/ || $t[6]=~/-/ || $t[7]=~/-/);
	}

}
