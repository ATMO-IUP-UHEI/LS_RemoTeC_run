import os

def main(file_list):
    for file in file_list:
        os.system('rm {}'.format(file))
