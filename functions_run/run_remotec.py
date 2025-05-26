import sys
import os
import time

import subprocess
from subprocess import Popen
from subprocess import DEVNULL, STDOUT


def main(rundir, remotec_path, lst_file_list, scheduler):
    match scheduler:
        case "none":
            func = default
        case "condor":
            func = condor
        # case "slurm":  # use slurm_jobarrays
        #     func = slurm
        case "slurm_jobarrays":
            func = slurm_jobarrays
        case _:
            sys.exit(f"scheduler {scheduler} invalid.")

    func(rundir, remotec_path, lst_file_list)


def default(rundir, remotec_path, lst_file_list):
    # one process for each lst file
    processes = []
    for i, lst_file in enumerate(lst_file_list):
        # i starts at 0
        run_id = str(i+1)
        process = Popen([remotec_path, run_id, lst_file], cwd=rundir)
        print(f'{remotec_path} {run_id} {lst_file}')
        processes.append(process)
    exitcodes = [process.wait() for process in processes]


def condor(rundir, remotec_path, lst_file_list):
    os.system(f'mkdir {rundir}/condor')
    job_file = f'{rundir}/condor/job_file.condor'
    with open(job_file, 'w') as file:
        # one process for each lst file
        for i, lst_file in enumerate(lst_file_list):
            # i starts at 0
            run_id = str(i+1)
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


# def slurm(rundir, remotec_path, lst_file_list):
#     print("using scheduler slurm")
#     os.system(f"mkdir {rundir}/slurm")
#     processes = []
#     for i, lst_file in enumerate(lst_file_list):
#         # i starts at 0
#         run_id = str(i+1)
#         job_file = f"{rundir}/slurm/job_file_{run_id:>06}.sh"
#         outdir = f"{rundir}/slurm"
#         with open(job_file, "w") as file:
#             file.writelines("#!/bin/bash\n")
#             file.writelines("#SBATCH --partition=cpu-single\n")
#             file.writelines("#SBATCH --ntasks=1\n")
#             file.writelines("#SBATCH --time=6:00:00\n")
#             file.writelines("#SBATCH --mem=8gb\n")
#             file.writelines(
#                 f"#SBATCH --output={outdir}/out_{run_id:>06}.out\n")
#             file.writelines(f"#SBATCH --error={outdir}/out_{run_id:>06}.out\n")
#             file.writelines(f"{remotec_path} {run_id} {lst_file}\n")
#         process = Popen(["sbatch", "--wait", job_file], cwd=rundir)
#         processes.append(process)
#
#     exitcodes = [process.wait() for process in processes]

def slurm_jobarrays(rundir, remotec_path, lst_file_list):
    print("using scheduler slurm with job arrays")
    outdir = f"{rundir}/slurm/run_remotec"
    job_file = f"{outdir}/job_array.sh"
    num_jobs = len(lst_file_list)

    start_time = time.time()

    with open(job_file, "w") as file:
        file.writelines("#!/bin/bash\n")
        file.writelines("#SBATCH --partition=cpu-single\n")
        file.writelines("#SBATCH --ntasks=1\n")
        file.writelines("#SBATCH --time=48:00:00\n")
        file.writelines("#SBATCH --mem=16gb\n")
        file.writelines(f"#SBATCH --array=1-{num_jobs}\n")
        file.writelines(f"#SBATCH --output={outdir}/out_%a.out\n")
        file.writelines(f"#SBATCH --error={outdir}/out_%a.out\n")
        file.writelines("printf -v padded_id '%06d' $SLURM_ARRAY_TASK_ID\n")
        file.writelines(
            f"mv {outdir}/out_$SLURM_ARRAY_TASK_ID.out {outdir}/out_${{padded_id}}.out\n")

        file.writelines("lst_file_list=(" + " ".join(lst_file_list) + ")\n")
        file.writelines("lst_file=${lst_file_list[$SLURM_ARRAY_TASK_ID-1]}\n")
        file.writelines(f"{remotec_path} $SLURM_ARRAY_TASK_ID $lst_file\n")

    # execute and wait
    # original job just starts the job array and kills itself right after
    # this means that, to wait for all jobs to finish, we need to get the
    # job_id to track all the smaller jobs.
    # after all the smaller jobs are done, a dummy job that does nothing is
    # run. the python script waits for this dummy job
    result = subprocess.run(["sbatch", job_file],
                            stdout=subprocess.PIPE, text=True)
    job_id = next((word for word in result.stdout.split()
                  if word.isdigit()), None)

    if job_id is None:
        raise RuntimeError("Failed to get SLURM job ID.")

    os.system(
        f"sbatch --dependency=afterok:{job_id} --output='{outdir}/slurm-{job_id}.out' --wrap='echo Job array {job_id} completed.' --wait")

    stop_time = time.time()
    with open(f"{outdir}/slurm-{job_id}.out", "a") as file:
        file.writelines(f"Total runtime: {stop_time - start_time:.2f} s.")
