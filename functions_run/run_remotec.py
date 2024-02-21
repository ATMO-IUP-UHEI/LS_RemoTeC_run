import sys
import os
import time

import subprocess
from subprocess import Popen
from subprocess import DEVNULL, STDOUT

################################################################################

def default(rundir, remotec_path, lst_file_list):
    processes = []
    for i, lst_file in enumerate(lst_file_list): # one process for each lst file
        run_id = str(i+1) # because i starts at 0
        process = Popen([remotec_path, run_id, lst_file], cwd=rundir)
        print(f'{remotec_path} {run_id} {lst_file}')
        processes.append(process)
    exitcodes = [process.wait() for process in processes]

################################################################################

def condor(rundir, remotec_path, lst_file_list):
    os.system(f'mkdir {rundir}/condor')
    job_file = f'{rundir}/condor/job_file.condor'
    with open(job_file, 'w') as file:
        for i, lst_file in enumerate(lst_file_list): # one process for each lst file
            run_id = str(i+1) # because i starts at 0
            file.writelines(f'executable = {remotec_path}\n')
            file.writelines(f'arguments = {run_id} {lst_file}\n')
            file.writelines('output = condor/outputfile\n')
            file.writelines('error = condor/errorfile\n')
            file.writelines('log = condor/remotec.log\n')
            file.writelines('should_transfer_files = IF_NEEDED\n')
            file.writelines('queue\n')

    os.chdir(rundir)
    os.system('condor_submit condor/job_file.condor')

    username = os.popen('whoami').read()
    done = False
    while not done:
        status = os.popen(f'condor_q {username}').read()
        for line in status.split('\n'):
            if line.startswith('Total for query: 0 jobs;'):
                done = True
        time.sleep(10)

################################################################################
