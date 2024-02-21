import sys
import os

from functions_run import run_read_config
from functions_run import parallelize_lst_file
from functions_run import parallelize_nml_file
from functions_run import run_remotec
from functions_run import merge_faulty_spectra
from functions_run import merge_output
from functions_run import remove_file_list

################################################################################
# import simulation specific variables
################################################################################
try:
    config_file = sys.argv[1]
except IndexError:
    sys.exit('provide settings file as command line argument')

config = run_read_config.read_create_config(config_file)
rundir = config['rundir']
remotec_path = config['remotec_path']
cores = config['cores']
input_file = config['input_file']
output_atm_file = config['output_atm_file']
output_l1b_file = config['output_l1b_file']
################################################################################



################################################################################
print('creating parallel input files...')
parallel_lst_file_list          = parallelize_lst_file.main(f'{rundir}/LST/full.lst', cores)
parallel_rtc_settings_file_list = parallelize_nml_file.main(f'{rundir}/INI/settings_RTC_create.nml', cores)
parallel_syn_settings_file_list = parallelize_nml_file.main(f'{rundir}/INI/syn_create.nml', cores)
################################################################################



################################################################################
print('running RemoTeC. This may take a while...')
run_remotec.default(rundir, remotec_path, parallel_lst_file_list)
################################################################################



################################################################################
print('merging output files...')
parallel_faulty_spectra_list    = merge_faulty_spectra.main(rundir)
parallel_atm_list               = merge_output.main(input_file, output_atm_file, cores)
parallel_l1b_list               = merge_output.main(input_file, output_l1b_file, cores)
################################################################################



################################################################################
print('removing parallel input files...')
remove_file_list.main(parallel_lst_file_list)
remove_file_list.main(parallel_rtc_settings_file_list)
remove_file_list.main(parallel_syn_settings_file_list)

print('removing parallel output files...')
remove_file_list.main(parallel_faulty_spectra_list)
remove_file_list.main(parallel_atm_list)
remove_file_list.main(parallel_l1b_list)
################################################################################
