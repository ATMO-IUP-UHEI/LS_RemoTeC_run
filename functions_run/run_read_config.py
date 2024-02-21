import configparser

def read_create_config(config_file):
    config = configparser.ConfigParser(interpolation = configparser.ExtendedInterpolation())
    config.read(config_file)
    config_dict = {}

    config_dict['rundir'] = config['Paths']['rundir_create']
    config_dict['remotec_path'] = config['Simulation']['remotec_create']
    config_dict['cores'] = int(config['Simulation']['cores'])
    config_dict['input_file'] = config['Paths']['input_file_create']
    config_dict['output_atm_file'] = config['Paths']['input_atm_file_retrieve']
    config_dict['output_l1b_file'] = config['Paths']['input_l1b_file_retrieve']

    return config_dict

def read_retrieve_config(config_file):
    config = configparser.ConfigParser(interpolation = configparser.ExtendedInterpolation())
    config.read(config_file)
    config_dict = {}

    config_dict['rundir'] = config['Paths']['rundir_retrieve']
    config_dict['remotec_path'] = config['Simulation']['remotec_retrieve']
    config_dict['cores'] = int(config['Simulation']['cores'])
    config_dict['input_file'] = config['Paths']['input_atm_file_retrieve']
    config_dict['output_file'] = config['Paths']['output_file_retrieve']

    return config_dict
