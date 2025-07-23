import sys
import os

from functions_run import read_config
from functions_run import parallelize_lst_file
from functions_run import parallelize_nml_file
from functions_run import run_remotec
from functions_run import merge_faulty_spectra
from functions_run import merge_output
from functions_run import remove_file_list

# current working directory (must be RemoTeC rundir)
rundir = os.getcwd()

# location of this python script (contains settings.ini)
scriptdir = os.path.dirname(__file__)

# get settings
config = read_config.main(
    os.path.join(scriptdir, "settings.ini"))
remotec_path = os.path.join(config["remotec_path"], "RemoTeC_create")
cores = config["cores"]
scheduler = config["scheduler"]

print("creating parallel input files...")
parallel_lst_file_list = parallelize_lst_file.main(
    f"{rundir}/LST/full.lst", cores)
parallel_rtc_settings_file_list = parallelize_nml_file.main(
    f"{rundir}/INI/settings_RTC_create.nml", cores)
parallel_syn_settings_file_list = parallelize_nml_file.main(
    f"{rundir}/INI/syn_create.nml", cores)

print("running RemoTeC. This may take a while...")
run_remotec.main(rundir, remotec_path, parallel_lst_file_list, scheduler)

print("merging output files...")
parallel_faulty_spectra_list = merge_faulty_spectra.main(rundir)
parallel_atm_list = merge_output.main(
    "INPUT/RTC_INP_DATA.nc", "SYNTH_SPECTRA/ATM_DATA.nc", cores)
parallel_l1b_list = merge_output.main(
    "INPUT/RTC_INP_DATA.nc", "SYNTH_SPECTRA/L1B_DATA.nc", cores)

print("removing parallel input files...")
remove_file_list.main(parallel_lst_file_list)
remove_file_list.main(parallel_rtc_settings_file_list)
remove_file_list.main(parallel_syn_settings_file_list)

print("removing parallel output files...")
remove_file_list.main(parallel_faulty_spectra_list)
remove_file_list.main(parallel_atm_list)
remove_file_list.main(parallel_l1b_list)
