import os
import datetime
import time
import configparser
import psycopg2
import logging


logging.basicConfig(
    filename='/opt/docker-data/tests/personal_logs/reading_logs_timescale.log',
    filemode='a',
    format='%(message)s',
    level=logging.INFO
)


def get_rrinterval_for_sing_patient_during_last_6_months(cur):
    cur.execute("SELECT count(\"RrInterval\") FROM \"RrInterval\" WHERE id_patient = '1' and timestamp > NOW() - interval '6 month'")
    return cur.fetchone()[0]


def get_rrinterval_mean_for_each_15min_interval_for_sing_patient(cur):
    cur.execute("SELECT time_bucket('15 minutes', timestamp) as fifteen_min, AVG(\"RrInterval\") FROM \"RrInterval\" WHERE \"id_patient\" = '1' GROUP BY fifteen_min ORDER BY fifteen_min ASC;")
    return cur.fetchone()[0]


def get_number_rrinterval_for_each_1min_interval_for_sing_patient(cur):
    cur.execute("SELECT time_bucket('1 minutes', timestamp) as one_min, count(\"RrInterval\") FROM \"RrInterval\" WHERE \"id_patient\" = '1' GROUP BY one_min ORDER BY one_min ASC;")
    return cur.fetchone()[0]


def get_total_rrinterval_by_user(cur):
    cur.execute("SELECT sum(\"RrInterval_by_min\"), id_patient FROM \"RrInterval_by_min\" GROUP BY id_patient ORDER BY id_patient ASC;")
    return cur.fetchone()[0]


def get_motionacc_for_sing_patient_during_last_day(cur):
    cur.execute("SELECT x_acm, y_acm, z_acm FROM \"MotionAccelerometer\" WHERE id_patient = '1' and timestamp > NOW() - interval '6 month'")
    return cur.fetchone()


def get_motiongyr_for_sing_patient_during_last_day(cur):
    cur.execute("SELECT x_gyro, y_gyro, z_gyro FROM \"MotionGyroscope\" WHERE id_patient = '1' and timestamp > NOW() - interval '6 month'")
    return cur.fetchone()


def get_inner_join_on_patient_and_mg_hyp(cur):
    cur.execute("SELECT a.x_gyro, b.uuid FROM \"MotionGyroscope\" a INNER JOIN \"Patient\" b ON a.id_patient = b.id WHERE id_patient = '1' and timestamp > NOW() - interval '6 month'")
    return cur.fetchone()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('/opt/docker-data/tests/load_testing_python_scripts/manual_data_injection_in_timescale/config.conf')

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

    t1 = datetime.datetime.now()

    logging.info('RrInterval during last 6 months reading begins at : {}'.format(t1))

    get_rrinterval_for_sing_patient_during_last_6_months(cur)

    t2 = datetime.datetime.now()

    logging.info('RrInterval during last 6 months : {}'.format(t2 - t1))

    t1 = datetime.datetime.now()

    logging.info('rrinterval mean 15 min interval reading begins at : {}'.format(t1))

    get_rrinterval_mean_for_each_15min_interval_for_sing_patient(cur)

    t2 = datetime.datetime.now()

    logging.info('rrinterval mean 15 min interval reading duration : {}'.format(t2 - t1))

    t1 = datetime.datetime.now()

    logging.info('rrinterval number 1 min interval reading begins at : {}'.format(t1))

    get_number_rrinterval_for_each_1min_interval_for_sing_patient(cur)

    t2 = datetime.datetime.now()

    logging.info('rrinterval number 1 min interval reading duration : {}'.format(t2 - t1))

    t1 = datetime.datetime.now()

    logging.info('total rrinterval by user reading begins at : {}'.format(t1))

    get_total_rrinterval_by_user(cur)

    t2 = datetime.datetime.now()

    logging.info('total rrinterval by user reading duration : {}'.format(t2 - t1))

    t1 = datetime.datetime.now()

    logging.info('motion acc during last day reading begins at : {}'.format(t1))

    get_motionacc_for_sing_patient_during_last_day(cur)

    t2 = datetime.datetime.now()

    logging.info('motion acc during last day reading duration : {}'.format(t2 - t1))

    t1 = datetime.datetime.now()

    logging.info('motion gyr during last day reading begins at : {}'.format(t1))

    get_motiongyr_for_sing_patient_during_last_day(cur)

    t2 = datetime.datetime.now()

    logging.info('motion gyr during last day reading duration : {}'.format(t2 - t1))

    t1 = datetime.datetime.now()

    logging.info('Inner join between Patient and MG tables begins at : {}'.format(t1))

    get_inner_join_on_patient_and_mg_hyp(cur)

    t2 = datetime.datetime.now()

    logging.info('Inner join between Patient and MG tables reading duration : {}'.format(t2 - t1))

    logging.info('--------------------------------------------')
