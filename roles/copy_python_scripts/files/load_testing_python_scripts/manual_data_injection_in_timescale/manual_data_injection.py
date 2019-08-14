import argparse
import connect_to_timescaledb as ctt
import set_and_reorganize_directories as set_directories
import aura_table as create_tables
import library as l


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--directory", required=True, help="path to the data directory")
    args = vars(ap.parse_args())

    PATH_TO_READ_DIRECTORY, PATH_FOR_PROBLEMS_FILES, PATH_FOR_WRITTEN_FILES = set_directories.set_success_and_fail_directories(args["directory"])

    set_directories.reorganize_data_directories(args["directory"])

    my_connection, cur = ctt.connect_to_timescaledb_physio_signals_db()

    create_tables.create_data_model(my_connection, cur)

    l.execute_write_pipeline(PATH_TO_READ_DIRECTORY, cur, my_connection, PATH_FOR_WRITTEN_FILES, PATH_FOR_PROBLEMS_FILES)
    print("-----------------------")
