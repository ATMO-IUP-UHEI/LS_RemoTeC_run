import os

def main(source_file, cores):
    source_path, source_name = os.path.split(source_file)
    basename = source_name.split('.')[0]

    with open(source_file, 'r') as file:
        source_scene_list = [scene.strip() for scene in file.readlines()]
    scenes = len(source_scene_list)

    # the scenes are split evenly onto different lst files.
    # we want as many lst files as we have cores
    #
    # we will get H files with h lines and L files with l lines.
    # solving
    #     cores = H + L
    #     scenes = H*h + L*l
    #     h = l + 1
    #     l = scenes // cores
    # yields:
    l = scenes // cores
    h = scenes // cores + 1
    L = cores * (1 + l) - scenes
    H = scenes - cores * l

    with open(source_file, 'r') as file:
        source_scenes = [scene.strip() for scene in file.readlines()]

    H_ = 0 # we currently have 0 files with h lines
    L_ = 0 # we currently have 0 files with l lines
    i  = 0 # index of current line in source_file

    tmp_lst_files = [] # return all the lst files so they can be cleaned up
                       # more easily later during the main script

    for target_number in range(1, cores+1):
        target_file = os.path.join(source_path, f'{basename}_{target_number:>06}.lst')
        with open(target_file, 'w') as file:
            if H_ < H: # if we have less files with h lines than we need:
                target_scenes = source_scenes[i:i+h]
                H_ += 1 # we just created a file with h lines
                i += h
            else: # if we have H files with h lines the rest of the files has l lines
                target_scenes = source_scenes[i:i+l]
                L_ += 1 # we just created a file with l lines
                i += l
            file.writelines(f'{scene}\n' for scene in target_scenes)
        tmp_lst_files.append(target_file)

    return tmp_lst_files
