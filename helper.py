#!usr/bin/env python
#encoding:utf-8
import subprocess
import os
import logging
import glob
import math

logging.basicConfig(filename='nohup.out',level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class step:

    def __init__(self,cpu,mem,shell_prefix,shell_env,shell_cont,cut,bin_path):
        self.cpu = cpu
        self.mem = mem
        if shell_env:
            self.shell_env = "?" + shell_env + "\n"
        else:
            self.shell_env=""
        self.shell_name = shell_prefix + ".sh"
        self.shell_cont = shell_cont + "\n"
        self.cut = cut
        self.bin_path = bin_path
        #self.output_path = output_path
        #self.full_path = self.output_path + "/" + self.shell_name


    def write_shell(self):
        with open(self.shell_name,'w') as shell:
            shell.write(self.shell_env)
            shell.write(self.shell_cont)


    def shell_submit(self):
        if not os.path.exists(self.shell_name+".done"):
            template = "python {bin_path}/sgearray.py -l m={mem},x={cpu} -c {cut} {shell_name}\n"
            cmd = template.format(bin_path=self.bin_path,mem=str(self.mem),cpu=str(self.cpu),cut=str(self.cut),shell_name=self.shell_name)
            logging.info(cmd)
            ret = subprocess.call(cmd,shell=True)
            if ret == 0:
                logging.info(self.shell_name+" done.")
                os.system("touch "+self.shell_name+".done")
            else:
                logging.error(self.shell_name+" failed!")
                exit()
        else:
            logging.info(self.shell_name+" already finished!")

    def shell_local(self):
        if not os.path.exists(self.shell_name+".done"):
            cmd = "sh " + self.shell_name
            print (cmd)
            ret = subprocess.call(cmd,shell=True)
            if ret == 0:
                logging.info(self.shell_name+" done.")
                os.system("touch "+self.shell_name+".done")
            else:
                logging.error(self.shell_name+" failed!")
                exit()
        else:
            logging.info(self.shell_name+" already finished!")

if __name__=='__main__':
    bin_path = os.path.abspath(__file__).strip("helper.py")
    step1 = step(1,1,"step1","source activate old","echo step1",1,bin_path)
    step2 = step(1,1,"step2","source activate old","echo step2",1,bin_path)
    step3 = step(1,1,"step3","","echo step2",1,bin_path)
    step1.write_shell()
    step2.write_shell()
    step3.write_shell()
    step1.shell_submit()
    step2.shell_submit()
    step3.shell_local()


