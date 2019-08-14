import configparser
import psycopg2


def connect_to_timescaledb_postgres_db():

    ####### VIRER CREATION PHYSIO_SIGNALS QUAND TESTS TERMINES #######

    config = configparser.ConfigParser()
    config.read('/home/tests/load_testing_python_scripts/manual_data_injection_in_timescale/config.conf')

    postgres_client_constants = config["Timescale Client"]

    DB_NAME = postgres_client_constants["dbname"]
    DB_CONN = postgres_client_constants["db_conn"]
    HOST = postgres_client_constants["host"]
    PORT = postgres_client_constants["port"]
    USER = postgres_client_constants["user"]

    my_connection = psycopg2.connect(
        host=HOST,
        port=int(PORT),
        dbname=DB_CONN,
        user=USER)

    my_connection.autocommit = True

    cur = my_connection.cursor()

    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = '" + DB_NAME + "'")
    exists = cur.fetchone()

    if not exists:
        cur.execute('CREATE DATABASE ' + DB_NAME)

    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")


    cur.close()

    my_connection.close()


def connect_to_timescaledb_physio_signals_db():

    config = configparser.ConfigParser()
    config.read('/home/tests/load_testing_python_scripts/manual_data_injection_in_timescale/config.conf')

    postgres_client_constants = config["Timescale Client"]

    DB_NAME = postgres_client_constants["dbname"]
    HOST = postgres_client_constants["host"]
    PORT = postgres_client_constants["port"]
    USER = postgres_client_constants["user"]

    my_connection = psycopg2.connect(
        host=HOST,
        port=int(PORT),
        dbname=DB_NAME,
        user=USER)

    cur = my_connection.cursor()

    return my_connection, cur
