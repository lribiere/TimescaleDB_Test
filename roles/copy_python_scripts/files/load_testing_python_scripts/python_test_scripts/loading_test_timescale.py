import os
import datetime
import time
import configparser
import psycopg2
import logging


logging.basicConfig(
    filename='/home/tests/personal_logs/injection_logs_timescale.log',
    filemode='a',
    format='%(message)s',
    level=logging.INFO
)


def get_number_output_rrinterval(cur):
    cur.execute('select count(*) from \"RrInterval\"')
    return cur.fetchone()[0]


def get_number_output_motionaccelerometer(cur):
    cur.execute('select count(*) from \"MotionAccelerometer\"')
    return cur.fetchone()[0]


def get_number_output_motiongyroscope(cur):
    cur.execute('select count(*) from \"MotionGyroscope\"')
    return cur.fetchone()[0]


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('/home/tests/load_testing_python_scripts/manual_data_injection_in_timescale/config.conf')

    postgres_client_constants = config["Timescale Client"]

    DB_NAME = postgres_client_constants["dbname"]
    DB_CONN = postgres_client_constants["db_conn"]
    HOST = postgres_client_constants["host"]
    PORT = postgres_client_constants["port"]
    USER = postgres_client_constants["user"]

    myConnection = psycopg2.connect(
        host=HOST,
        port=int(PORT),
        dbname=DB_NAME,
        user=USER)

    cur = myConnection.cursor()

    for i in range(10):

        os.system("sudo python3.6 /home/tests/load_testing_python_scripts/random_data_generator/source/random_data_generator.py -nbu 10 -hr 8")

        t1 = datetime.datetime.now()

        os.system("sudo python3.6 /home/tests/load_testing_python_scripts/manual_data_injection_in_timescale/manual_data_injection.py --directory /home/data/aura_generated_data/ --chunk_size 1000")

        t2 = datetime.datetime.now()

        logging.info('injection {} starts at {}'.format(i, t1))

        logging.info('injection {} duration : {}'.format(i, t2-t1))

        logging.info('monitoring-RrInterval-output : {}'.format(get_number_output_rrinterval(cur)))

        logging.info('monitoring-MotionAccelerometer-output count_x_acm : {}'.format(
            get_number_output_motionaccelerometer(cur)))

        logging.info(
            'monitoring-MotionGyroscope-output count_x_gyro : {}'.format(get_number_output_motiongyroscope(cur)))

        logging.info('--------------------------------------------')

        logging.info('{}'.format(i))

        if i != 9:
            time.sleep(900)
