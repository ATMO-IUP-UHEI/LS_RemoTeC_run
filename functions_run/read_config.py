import configparser


def main(config_file):
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation())
    config.read(config_file)

    config_dict = {}
    config_dict["remotec_path"] = config["settings"]["remotec_path"]
    config_dict["cores"] = int(config["settings"]["cores"])
    config_dict["scheduler"] = config["settings"]["scheduler"]

    return config_dict
