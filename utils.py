from os import path

def file_exists_in_cwd(path_to_file:str):
    if not path.exists(path_to_file):
        raise FileNotFoundError(f'Could not execute due to missing {path_to_file} in directory {os.getcwd()}.')
    return True
