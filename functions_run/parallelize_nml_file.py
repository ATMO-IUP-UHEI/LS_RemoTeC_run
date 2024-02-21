import os

def main(source_file, cores):
    source_path, source_name = os.path.split(source_file)
    basename = source_name.split('.')[0]

    tmp_nml_files = [] # return all the nml files so they can be cleaned up
                       # more easily later during the main script

    for target_number in range(1, cores+1):
        target_file = os.path.join(source_path, f'{basename}_{target_number:>06}.nml')
        tmp_nml_files.append(target_file)
        os.system(f'cp {source_file} {target_file}')

    return tmp_nml_files
