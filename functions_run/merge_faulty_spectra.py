import os

def main(rundir):
    parallel_faulty_spectra = []
    for file in os.listdir(f'{rundir}/CONTRL_OUT'):
        if file.startswith('faulty_spectra_'):
            parallel_faulty_spectra.append(f'{rundir}/CONTRL_OUT/{file}')

    with open(f'{rundir}/CONTRL_OUT/faulty_spectra.dat', 'w') as target:
        for source_file in parallel_faulty_spectra:
            with open(source_file, 'r') as source:
                for line in source:
                    target.writelines(f'{line}\n')

    return parallel_faulty_spectra
