import netCDF4
import time
import numpy
import os
import sys


def main(input_file, target_file, cores):
    input_file_path,  input_file_name = os.path.split(input_file)
    target_file_path, target_file_name = os.path.split(target_file)

    source_file_list = []
    if target_file_name.startswith('RTC_OUT'):
        prefix = 'RTC_OUT'
    if target_file_name.startswith('ATM'):
        prefix = 'ATM'
    if target_file_name.startswith('L1B'):
        prefix = 'L1B'
    for core in range(1, cores+1):
        source_file_path = f'{target_file_path}/{prefix}_{core:>06}.nc'
        if os.path.isfile(source_file_path):
            source_file_list.append(source_file_path)
    if len(source_file_list) == 0:
        err = f'Error: No output files {prefix}_XXXXXX.nc exist.'
        sys.exit(err)

    import xarray
    dataset = xarray.open_dataset(input_file)
    # latitude and longitude for create are called 'lat' and 'lon',
    # respectively
    if input_file_name.startswith('RTC_INP_'):
        nframe = dataset.lat.size
        nline = dataset.lon.size
    # dimensions for retrieve are called "frame" and "line", respectively
    elif input_file_name.startswith('ATM_'):
        nframe = dataset.frame.size
        nline = dataset.line.size
    else:
        sys.exit('Cannot read dimensions of input netcdf file, as naming convention was not used. Input files for \'create\' have to start with RTC_INP_, input files for \'retrieve\' have to start with ATM_.')
    dataset.close()

    # create target file and put it into the same shape as the source files
    # use the first source file as a template as it will always exist
    prepare_target_file(source_file_list[0], target_file, nframe, nline)

    # write data from source files into target file
    for source_file in source_file_list:
        copy_data(source_file, target_file, prefix, cores)

    return source_file_list


def prepare_target_file(source_file, target_file, nframe, nline):
    # open source file as nc dataset
    source_dataset = netCDF4.Dataset(source_file, 'r')

    # create target dataset
    target_dataset = netCDF4.Dataset(target_file, 'w', format='NETCDF4')
    target_dataset.history = 'Created {}'.format(time.ctime(time.time()))

    # create spatial dimensions
    target_dataset.createDimension('frame', nframe)
    target_dataset.createDimension('line', nline)

    # copy dimensions from source file
    for source_dimname, source_dim in source_dataset.dimensions.items():
        if source_dimname in ['level']:
            target_dataset.createDimension(source_dimname, len(source_dim))

    # copy spectrally independent variables from source dataset
    for source_varname, source_var in source_dataset.variables.items():
        if source_varname.lower() in ['x', 'y']:
            pass
        elif list(source_var.dimensions) == ['nobs']:
            target_var = target_dataset.createVariable(
                source_varname,
                source_var.dtype,
                ('frame', 'line')
            )
        elif list(source_var.dimensions) == ['nobs', 'level']:
            target_var = target_dataset.createVariable(
                source_varname,
                source_var.dtype,
                ('frame', 'line', 'level')
            )
        else:
            continue

        for source_attname in source_var.ncattrs():
            setattr(
                target_var,
                source_attname,
                getattr(source_var, source_attname)
            )

    # copy groups from source dataset
    for source_groupname, source_group in source_dataset.groups.items():
        target_group = target_dataset.createGroup(source_groupname)

        # copy dimensions from source dataset
        for source_dimname, source_dim in source_group.dimensions.items():
            target_group.createDimension(source_dimname, len(source_dim))

        # copy spectrally dependent variables from source dataset
        for source_varname, source_var in source_group.variables.items():
            if list(source_var.dimensions) == ['channel']:
                target_var = target_group.createVariable(
                    source_varname,
                    source_var.dtype,
                    ('channel')
                )
                target_var[:] = source_var[:]
            elif list(source_var.dimensions) == ['nobs']:
                target_var = target_group.createVariable(
                    source_varname,
                    source_var.dtype,
                    ('frame', 'line')
                )
            elif list(source_var.dimensions) == ['nobs', 'nwave']:
                target_var = target_group.createVariable(
                    source_varname,
                    source_var.dtype,
                    ('frame', 'line', 'channel')
                )
            else:
                continue

            for source_attname in source_var.ncattrs():
                setattr(
                    target_var,
                    source_attname,
                    getattr(source_var, source_attname)
                )

    # close datasets
    source_dataset.close()
    target_dataset.close()


def copy_data(source_file, target_file, prefix, cores):
    number = int(source_file[-9:-3])
    print(f'    copying data from {prefix} files '
          f'({int(number)}/{int(cores)}).', end='\r')
    if number == cores:
        print()

    # open files
    source_nc_dataset = netCDF4.Dataset(source_file, 'r')
    target_nc_dataset = netCDF4.Dataset(target_file, 'a')

    # convert to indices starting at 0
    xind = source_nc_dataset['x'][:]-1
    yind = source_nc_dataset['y'][:]-1

    # get spectrally independent data
    for target_varname, target_var in target_nc_dataset.variables.items():
        data = target_nc_dataset[target_varname][:]
        data[yind, xind] = source_nc_dataset[target_varname][:]
        target_nc_dataset[target_varname][:] = data[:]

    # get spectrally dependent data
    for target_groupname, target_group in target_nc_dataset.groups.items():
        for target_varname, target_var in target_group.variables.items():
            data = target_group[target_varname][:]
            if len(numpy.shape(target_group[target_varname][:])) == 2:
                data[yind, xind] = source_nc_dataset[target_groupname][target_varname][:]
            elif len(numpy.shape(target_group[target_varname][:])) == 3:
                data[yind, xind, :] = source_nc_dataset[target_groupname][target_varname][:]
            else:
                continue
            target_group[target_varname][:] = data[:]

    # close files
    source_nc_dataset.close()
    target_nc_dataset.close()
