import shutil
import math
import datetime
import os
import glob
import pandas as pd
import json
import numpy as np
import logging


def change_timestamp(unused_1, unused_2):
    '''sec and what is unused.'''
    change_timestamp_time = datetime.datetime.now()
    return change_timestamp_time.timetuple()


logging.Formatter.converter = change_timestamp
logging.basicConfig(
    filename='/opt/docker-data/logstash/personal_logs/timescaledb_manual_logs_input-json.log',
    filemode='a',
    format='%(asctime)s.%(msecs)03d : %(message)s',
    level=logging.INFO,
    datefmt="%B %d %Y, %H:%M:%S",
)


def inject_patient_id_into_patient_table(user: str, cur, my_connection):
    cur.execute("INSERT INTO \"Patient\" (uuid) SELECT '" + user + "' WHERE NOT EXISTS ( SELECT 1 FROM \"Patient\" WHERE uuid ='" + user + "');")
    my_connection.commit()


def get_patient_id_from_patient_table(user: str, cur):
    cur.execute("Select id from \"Patient\" where uuid = '" + user + "'")
    user_id = cur.fetchone()[0]
    return user_id


def inject_device_address_into_device_table(device_address: str, cur, my_connection):
    cur.execute("INSERT INTO \"Device\" (mac) SELECT '" + device_address + "' WHERE NOT EXISTS ( SELECT 1 FROM \"Device\" WHERE mac ='" + device_address + "');")
    my_connection.commit()


def get_device_address_from_device_table(device_address: str, cur):
    cur.execute("Select id from \"Device\" where mac = '" + device_address + "'")
    device_address = cur.fetchone()[0]
    return device_address


def inject_rrinterval_by_min_into_rrintervalbyid_table(concatenated_rr_dataframe, patient_measurement_subdirectories, id_patient, cur, my_connection, path_for_written_files, path_for_problem_files):
    rri_count_by_min = concatenated_rr_dataframe.resample("1min", label="right").count()
    rri_count_by_min.columns = ["RrInterval_by_min"]
    rri_count_by_min.insert(0, 'id_patient', id_patient, False)

    chunk_nb = math.ceil(len(rri_count_by_min) / 10000)
    print("There are {} chunks to write.".format(chunk_nb))

    rrinterval_by_min_dataframe_chunk_list = np.array_split(rri_count_by_min, chunk_nb)

    # Write each chunk in time series db
    for chunk in rrinterval_by_min_dataframe_chunk_list:
        chunk.to_csv(patient_measurement_subdirectories + "_rr_interval_by_min_" + str(rrinterval_by_min_dataframe_chunk_list.index(chunk)) + ".csv", encoding='utf-8',
                index=True, index_label=False, header=False)

    for csv_file in glob.glob(patient_measurement_subdirectories + "_rr_interval_by_min_*"):
        try:
            csv_file_to_inject = open(csv_file)
            cur.copy_from(csv_file_to_inject, '\"RrInterval_by_min\"', columns=('timestamp', 'id_patient', '\"RrInterval_by_min\"'), sep=",")

            my_connection.commit()

            os.remove(csv_file)
        except:
            print("Impossible to write csv file to TimescaleDB")


def inject_rrinterval_into_rrinterval_table(concatenated_rr_dataframe, patient_measurement_subdirectories, id_patient, device_address, cur, my_connection, path_for_written_files, path_for_problem_files):
    corrected_timestamp_list = create_corrected_timestamp_list(concatenated_rr_dataframe)
    concatenated_rr_dataframe.index = corrected_timestamp_list
    concatenated_rr_dataframe.index.names = ["timestamp"]

    concatenated_rr_dataframe.insert(0, 'id_patient', id_patient, False)
    concatenated_rr_dataframe.insert(1, 'device_address', device_address, False)

    chunk_nb = math.ceil(len(concatenated_rr_dataframe) / 10000)
    print("There are {} chunks to write.".format(chunk_nb))

    rrinterval_dataframe_chunk_list = np.array_split(concatenated_rr_dataframe, chunk_nb)

    for i in range(chunk_nb):
        rrinterval_dataframe_chunk_list[i].to_csv(patient_measurement_subdirectories + "_raw_rr_interval_" + str(i) + ".csv", encoding='utf-8',
                                    index=True, index_label=False, header=False)

    try:
        for csv_file in glob.glob(patient_measurement_subdirectories + "_raw_rr_interval_*"):
            csv_file_to_inject = open(csv_file)

            t1 = datetime.datetime.now()
            cur.copy_from(csv_file_to_inject, '\"RrInterval\"', columns=('timestamp', 'id_patient', 'id_device', '\"RrInterval\"'), sep=",")

            my_connection.commit()

            os.remove(csv_file)

            t2 = datetime.datetime.now()
            print("insert RrInterval duration {}".format(t2 - t1))
        write_success = True
    except:
        print("impossible to write files into TimescaleDB")
        write_success = False

    return write_success


def convert_rri_json_to_df(rri_json) -> pd.DataFrame:
    """
    Function converting RrInterval JSON data to pandas Dataframe.
    Arguments
    ---------
    acm_json - RrInterval JSON file sent from Web-socket
    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), rri_json["data"]))

    columns = ['timestamp', 'RrInterval']
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index('timestamp')

    # Convert string to numeric values
    df_to_write['RrInterval'] = df_to_write['RrInterval'].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def concat_rrinterval_files_into_single_dataframe(files_list: list) -> pd.DataFrame:
    """
    Concatenate JSON files content into a single pandas DataFrame.
    Arguments
    ---------
    files_list - list of files to sort
    Returns
    ---------
    concatenated_rr_interval_dataframe - resulting pandas DataFrame
    """
    dataframe_list = []
    for file in files_list:
        # Open Json file
        with open(file) as json_file:
            json_data = json.load(json_file)

        # Get tags from file
        measurement = json_data['type']

        # Extract data and create dataframe from JSON file
        if measurement == "RrInterval":
            rr_interval_dataframe = convert_rri_json_to_df(json_data)
            dataframe_list.append(rr_interval_dataframe)

    # Concat list of dataframe
    concatenated_rr_interval_dataframe = pd.concat(dataframe_list)
    return concatenated_rr_interval_dataframe


def create_corrected_timestamp_list(concatenated_rr_interval_dataframe: pd.DataFrame) -> list:
    """
    Create a corrected timestamp based on cumulative sum of RR-intervals values.
    Arguments
    ---------
    concatenated_df - pandas DataFrame containing all data of a specific user
    Returns
    ---------
    corrected_timestamp_list - Corrected timestamp generated
    """
    rri_list = concatenated_rr_interval_dataframe['RrInterval'].values
    polar_index = concatenated_rr_interval_dataframe.index

    current_timestamp = polar_index[0]
    next_timestamp = polar_index[1]

    # Set the first timestamp to be the first timestamp of the polar
    corrected_timestamp_list = [current_timestamp]

    for i in range(1, len(polar_index) - 1):
        next_corrected_timestamp = get_next_timestamp(next_timestamp, current_timestamp,
                                                      corrected_timestamp_list[-1], rri_list[i])
        corrected_timestamp_list.append(next_corrected_timestamp)

        # Update next timestamps to compute time difference
        current_timestamp = polar_index[i]
        next_timestamp = polar_index[i+1]

    # Deal with last timestamp value
    next_corrected_timestamp = get_next_timestamp(next_timestamp, current_timestamp,
                                                  corrected_timestamp_list[-1], rri_list[-1])
    corrected_timestamp_list.append(next_corrected_timestamp)

    return corrected_timestamp_list


def get_next_timestamp(next_timestamp, current_timestamp, last_corrected_timestamp,
                       next_rr_interval: float):
    """
    :param next_timestamp:
    :param current_timestamp:
    :param last_corrected_timestamp:
    :param next_rr_interval:
    :return:
    """
    # Deal with last timestamp value
    time_difference = next_timestamp - current_timestamp
    if abs(time_difference.seconds) < 3:
        next_corrected_timestamp = last_corrected_timestamp + \
                                   datetime.timedelta(milliseconds=np.float64(next_rr_interval))
        return next_corrected_timestamp
    else:
        return next_timestamp


def rri_files_write_pipeline(patient_measurement_subdirectories: str, id_patient, cur, my_connection, path_for_written_files, path_for_problem_files):
    rr_files = glob.glob(patient_measurement_subdirectories + "/*")

    with open(rr_files[0]) as json_file:
        json_data = json.load(json_file)
    device_address = json_data['device_address']

    t1 = datetime.datetime.now()
    inject_device_address_into_device_table(device_address, cur, my_connection)
    t2 = datetime.datetime.now()

    logging.info("injection duration for device_address : {}".format(t2-t1))

    t1 = datetime.datetime.now()
    device_address = get_device_address_from_device_table(device_address, cur)
    t2 = datetime.datetime.now()

    logging.info("query duration for device_address : {}".format(t2-t1))

    # concat multiple files of each user
    concatenated_dataframe = concat_rrinterval_files_into_single_dataframe(files_list=rr_files)

    # GET raw data count by min
    inject_rrinterval_by_min_into_rrintervalbyid_table(concatenated_dataframe, patient_measurement_subdirectories, id_patient, cur, my_connection, path_for_written_files, path_for_problem_files)

    # Create new timestamp
    write_success = inject_rrinterval_into_rrinterval_table(concatenated_dataframe, patient_measurement_subdirectories, id_patient, device_address, cur, my_connection, path_for_written_files, path_for_problem_files)

    if write_success:
        for file in rr_files:
            shutil.move(src=file, dst=path_for_written_files + file.split("/")[-1])
    else:
        for file in rr_files:
            shutil.move(src=file, dst=path_for_problem_files + file.split("/")[-1])


def convert_acm_json_to_df(acm_json: dict) -> pd.DataFrame:
    """
    Function converting accelerometer JSON data to pandas Dataframe.
    Arguments
    ---------
    acm_json - Accelerometer JSON file sent from Web-socket
    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), acm_json["data"]))

    columns = ['timestamp', 'x_acm', 'y_acm', 'z_acm', 'sensibility']
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index('timestamp')

    # Convert string to numeric values
    df_to_write[['x_acm', 'y_acm', 'z_acm']] = df_to_write[['x_acm', 'y_acm', 'z_acm']].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_gyro_json_to_df(gyro_json) -> pd.DataFrame:
    """
    Function converting gyroscope JSON data to pandas Dataframe.
    Arguments
    ---------
    acm_json - gyroscope JSON file sent from Web-socket
    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), gyro_json["data"]))

    columns = ['timestamp', 'x_gyro', 'y_gyro', 'z_gyro']
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index('timestamp')

    # Convert string to numeric values
    df_to_write[['x_gyro', 'y_gyro', 'z_gyro']] = df_to_write[['x_gyro', 'y_gyro', 'z_gyro']].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def create_df_with_unique_index(data_to_write: pd.DataFrame,
                                time_delta_to_add: int = 123456) -> pd.DataFrame:

    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    while not is_index_unique:
        data_to_write.index = data_to_write.index.where(~data_to_write.index.duplicated(),
                                                        data_to_write.index + pd.to_timedelta(time_delta_to_add,
                                                                                              unit='ns'))
        data_to_write = data_to_write.sort_index()
        is_index_unique = data_to_write.index.is_unique
    return data_to_write


def inject_motion_accelerometer_into_motionacc_table(patient_measurement_subdirectories, ma_json_files, id_patient, device_address, cur, my_connection, path_for_written_files, path_for_problem_files):

    for i in range(len(ma_json_files)):
        with open(ma_json_files[i]) as json_file:
            json_data = json.load(json_file)

        ma_df = convert_acm_json_to_df(json_data)

        # Checking if index of data is unique to avoid overwritten points in InfluxDB
        is_index_unique = ma_df.index.is_unique

        if not is_index_unique:
            ma_df = create_df_with_unique_index(ma_df)

        ma_df.insert(0, 'id_patient', id_patient, False)
        ma_df.insert(1, 'device_address', device_address, False)

        ma_df.to_csv(patient_measurement_subdirectories + "_ma_" + str(i) + ".csv", encoding='utf-8',
                                    index=True, index_label=False, header=False)

        try:
            csv_file_to_inject = open(patient_measurement_subdirectories + "_ma_" + str(i) + ".csv")

            cur.copy_from(csv_file_to_inject, '\"MotionAccelerometer\"',
                          columns=('timestamp', 'id_patient', 'id_device', 'x_acm', 'y_acm', 'z_acm', 'sensibility'),
                          sep=",")

            my_connection.commit()

            os.remove(patient_measurement_subdirectories + "_ma_" + str(i) + ".csv")

            shutil.move(src=ma_json_files[i], dst=path_for_written_files + ma_json_files[i].split("/")[-1])
        except:
            shutil.move(src=ma_json_files[i], dst=path_for_problem_files + ma_json_files[i].split("/")[-1])
            print("Impossible to write file into TimescaleDB")


def inject_motion_gyroscope_into_motiongyr_table(patient_measurement_subdirectories, mg_json_files, id_patient, device_address, cur, my_connection, path_for_written_files, path_for_problem_files):

    for i in range(len(mg_json_files)):
        with open(mg_json_files[i]) as json_file:
            json_data = json.load(json_file)

        mg_df = convert_gyro_json_to_df(json_data)

        # Checking if index of data is unique to avoid overwritten points in InfluxDB
        is_index_unique = mg_df.index.is_unique

        if not is_index_unique:
            mg_df = create_df_with_unique_index(mg_df)

        mg_df.insert(0, 'id_patient', id_patient, False)
        mg_df.insert(1, 'device_address', device_address, False)

        mg_df.to_csv(patient_measurement_subdirectories + "_mg_" + str(i) + ".csv", encoding='utf-8',
                                    index=True, index_label=False, header=False)

        try:
            csv_file_to_inject = open(patient_measurement_subdirectories + "_mg_" + str(i) + ".csv")

            cur.copy_from(csv_file_to_inject, '\"MotionGyroscope\"',
                          columns=('timestamp', 'id_patient', 'id_device', 'x_gyro', 'y_gyro', 'z_gyro'),
                          sep=",")

            my_connection.commit()

            os.remove(patient_measurement_subdirectories + "_mg_" + str(i) + ".csv")

            shutil.move(src=mg_json_files[i], dst=path_for_written_files + mg_json_files[i].split("/")[-1])
        except:
            shutil.move(src=mg_json_files[i], dst=path_for_problem_files + mg_json_files[i].split("/")[-1])
            print("Impossible to write file into TimescaleDB")


def acm_gyro_write_pipeline(patient_measurement_subdirectories: str, id_patient, cur, my_connection, path_for_written_files, path_for_problem_files):
    ma_mg_files = glob.glob(patient_measurement_subdirectories + "/*")

    with open(ma_mg_files[0]) as json_file:
        json_data = json.load(json_file)
    device_address = json_data["device_address"]

    t1 = datetime.datetime.now()
    inject_device_address_into_device_table(device_address, cur, my_connection)
    t2 = datetime.datetime.now()

    logging.info("injection duration for device_address : {}".format(t2 - t1))

    t1 = datetime.datetime.now()
    device_address = get_device_address_from_device_table(device_address, cur)
    t2 = datetime.datetime.now()

    logging.info("query duration for device_address : {}".format(t2 - t1))

    if patient_measurement_subdirectories.split("/")[-1] == "MotionAccelerometer":
        t1 = datetime.datetime.now()
        inject_motion_accelerometer_into_motionacc_table(patient_measurement_subdirectories, ma_mg_files, id_patient, device_address, cur, my_connection, path_for_written_files, path_for_problem_files)
        t2 = datetime.datetime.now()
        logging.info("injection duration for motion accelerometer : {}".format(t2 - t1))
        shutil.rmtree(patient_measurement_subdirectories)
    else:
        t1 = datetime.datetime.now()
        inject_motion_gyroscope_into_motiongyr_table(patient_measurement_subdirectories, ma_mg_files, id_patient, device_address, cur, my_connection, path_for_written_files, path_for_problem_files)
        t2 = datetime.datetime.now()
        logging.info("injection duration for motion gyroscope : {}".format(t2 - t1))
        shutil.rmtree(patient_measurement_subdirectories)


def execute_write_pipeline(path_to_read_directory: str, cur, my_connection, path_for_written_files, path_for_problem_files):

    subdirectories = glob.glob(path_to_read_directory + '*')
    for user_directory in subdirectories:
        user = user_directory.split("/")[-1]

        t1 = datetime.datetime.now()
        inject_patient_id_into_patient_table(user, cur, my_connection)
        t2 = datetime.datetime.now()
        logging.info("injection duration for patient : {}".format(t2 - t1))

        t1 = datetime.datetime.now()
        patient_id = get_patient_id_from_patient_table(user, cur)
        t2 = datetime.datetime.now()
        logging.info("query duration for patient : {}".format(t2 - t1))

        patient_subdirectories = glob.glob(user_directory + "/*")
        for patient_measurement_subdirectories in patient_subdirectories:
            if patient_measurement_subdirectories.split("/")[-1] == "RrInterval":
                rri_files_write_pipeline(patient_measurement_subdirectories, patient_id, cur, my_connection, path_for_written_files, path_for_problem_files)
                shutil.rmtree(patient_measurement_subdirectories)
            else:
                acm_gyro_write_pipeline(patient_measurement_subdirectories, patient_id, cur, my_connection, path_for_written_files, path_for_problem_files)
        shutil.rmtree(user_directory)
