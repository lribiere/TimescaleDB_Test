import configparser
import os
import glob
import shutil


MEASUREMENTS = ["RrInterval", "MotionAccelerometer", "MotionGyroscope"]


def set_success_and_fail_directories(path_to_directories):
    config = configparser.ConfigParser()
    config.read('/home/tests/load_testing_python_scripts/manual_data_injection_in_timescale/config.conf')

    files_processing_paths = config["Paths"]

    PATH_TO_READ_DIRECTORY = path_to_directories
    PATH_FOR_WRITTEN_FILES = files_processing_paths["success_files_directory"]
    PATH_FOR_PROBLEMS_FILES = files_processing_paths["failed_files_directory"]

    if not os.path.exists(PATH_FOR_WRITTEN_FILES):
        os.makedirs(PATH_FOR_WRITTEN_FILES)
    if not os.path.exists(PATH_FOR_PROBLEMS_FILES):
        os.makedirs(PATH_FOR_PROBLEMS_FILES)

    return PATH_TO_READ_DIRECTORY, PATH_FOR_PROBLEMS_FILES, PATH_FOR_WRITTEN_FILES


def create_files_by_user_dict(files_list: list) -> dict:
    """
    Create a dictionary containing the corresponding list of RR-inteval files for each user.
    Arguments
    ---------
    files_list - list of files, must be sorted for function to operate correctly !
    Returns
    ---------
    files_by_user_dict - sorted dictionary
    ex :
    files_by_user_dict = {
            'user_1': ["file_1", "file_2", "file_3"],
            'user_2': ["file_4", "file_5"],
            'user_3': ["file_6", "file_7", "file_8"]
    }
    """
    # Create sorted user list
    user_list = list(set(map(lambda x: x.split("/")[-1].split("_")[0], files_list)))
    user_list.sort()

    files_by_user_dict = dict()
    file_list_for_a_user = []
    try:
        current_user = user_list[0]
    except IndexError:
        return dict()

    for filename in files_list:
        if current_user in filename:
            file_list_for_a_user.append(filename)
        else:
            files_by_user_dict[current_user] = file_list_for_a_user
            current_user = filename.split("/")[-1].split("_")[0]
            file_list_for_a_user = [filename]

    # Add list of files for last user in dictionary
    files_by_user_dict[current_user] = file_list_for_a_user
    return files_by_user_dict


def reorganize_data_directories(path_to_directories):
    files_list = glob.glob(path_to_directories + "*")
    files_list.sort()
    sorted_files_dict = create_files_by_user_dict(files_list)

    for user in sorted_files_dict.keys():
        if not os.path.exists(path_to_directories + user):
            os.mkdir(path_to_directories + user)
        for files in sorted_files_dict[user]:
            shutil.move(files, path_to_directories + user + "/" + files.split("/")[-1])
        for measurement in MEASUREMENTS:
            files_list = glob.glob(path_to_directories + user + "/" + "*" + measurement + "*")
            if not os.path.exists(path_to_directories + user + "/" + measurement):
                os.mkdir(path_to_directories + user + "/" + measurement)
            for files_measurement in files_list:
                shutil.move(files_measurement,
                            path_to_directories + user + "/" + measurement + "/" + files_measurement.split("/")[-1])

