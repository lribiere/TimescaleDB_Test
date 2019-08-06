def create_patient_table(cur):
    cur.execute(
        'CREATE TABLE "Patient" ('
        ' id SERIAL,'
        ' uuid VARCHAR(36)'
        ' );'
    )
    constraint_patient = (
        """ALTER TABLE "Patient" ADD CONSTRAINT id_PatientPK PRIMARY KEY (id)""",
        """ALTER TABLE "Patient" ADD CONSTRAINT uuidUnique UNIQUE (uuid)"""
    )
    for constraint in constraint_patient:
        cur.execute(constraint)


def create_device_table(cur):
    cur.execute(
        'CREATE TABLE "Device" ('
        ' id SERIAL,'
        ' mac VARCHAR(36)'
        ' );'
    )
    constraint_device = (
        """ALTER TABLE "Device" ADD CONSTRAINT id_DevicePK PRIMARY KEY (id)""",
        """ALTER TABLE "Device" ADD CONSTRAINT macUnique UNIQUE (mac)"""
    )
    for constraint in constraint_device:
        cur.execute(constraint)


def create_rrinterval_hypertable(cur):
    cur.execute(
        'CREATE TABLE "RrInterval" ('
        ' timestamp timestamp(3),'
        ' id_patient INTEGER,'
        ' id_device INTEGER,'
        ' "RrInterval" INTEGER'
        ' );'
    )
    constraint_rr = (
        """ALTER TABLE "RrInterval" ADD CONSTRAINT RrIntervalPK PRIMARY KEY (timestamp, id_patient, id_device)""",
        """ALTER TABLE "RrInterval" ADD CONSTRAINT RrIntervalPatientFK FOREIGN KEY (id_patient) REFERENCES "Patient" (id)""",
        """ALTER TABLE "RrInterval" ADD CONSTRAINT RrIntervalDeviceFK FOREIGN KEY (id_device) REFERENCES "Device" (id)"""
    )

    for constraint in constraint_rr:
        cur.execute(constraint)

    cur.execute(
        "select create_hypertable('\"RrInterval\"', 'timestamp', 'id_patient', '4', chunk_time_interval => interval '1 day', if_not_exists => TRUE);")


def create_count_rrinterval_by_min_hypertable(cur):
    cur.execute(
        'CREATE TABLE "RrInterval_by_min" ('
        ' timestamp timestamp,'
        ' id_patient INTEGER,'
        ' "RrInterval_by_min" INTEGER'
        ' );'
    )
    constraint_rr_count = (
        """ALTER TABLE "RrInterval_by_min" ADD CONSTRAINT RrInterval_by_minPK PRIMARY KEY (id_patient, timestamp)""",
        """ALTER TABLE "RrInterval_by_min" ADD CONSTRAINT RrInterval_by_minPatientFK FOREIGN KEY (id_patient) REFERENCES "Patient" (id)"""
    )

    for constraint in constraint_rr_count:
        cur.execute(constraint)

    cur.execute(
        "select create_hypertable('\"RrInterval_by_min\"', 'timestamp', 'id_patient', '4', chunk_time_interval => interval '1 day', if_not_exists => TRUE);")


def create_motiongyroscope_hypertable(cur):
    cur.execute(
        'CREATE TABLE "MotionGyroscope" ('
        ' timestamp timestamp,'
        ' id_patient INTEGER,'
        ' id_device INTEGER,'
        ' x_gyro REAL,'
        ' y_gyro REAL,'
        ' z_gyro REAL'
        ' );'
    )

    constraint_mg = (
        """ALTER TABLE "MotionGyroscope" ADD CONSTRAINT MotionGyroscopePK PRIMARY KEY (timestamp, id_patient, id_device)""",
        """ALTER TABLE "MotionGyroscope" ADD CONSTRAINT MotionGyroscopePatientFK FOREIGN KEY (id_patient) REFERENCES "Patient" (id)""",
        """ALTER TABLE "MotionGyroscope" ADD CONSTRAINT MotionGyroscopeDeviceFK FOREIGN KEY (id_device) REFERENCES "Device" (id)"""
    )

    for constraint in constraint_mg:
        cur.execute(constraint)

    cur.execute(
        "select create_hypertable('\"MotionGyroscope\"', 'timestamp', 'id_patient', '4', chunk_time_interval => interval '1 day', if_not_exists => TRUE);")


def create_motionaccelerometer_hypertable(cur):
    cur.execute(
        'CREATE TABLE "MotionAccelerometer" ('
        ' timestamp timestamp,'
        ' id_patient INTEGER,'
        ' id_device INTEGER,'
        ' x_acm REAL,'
        ' y_acm REAL,'
        ' z_acm REAL,'
        ' sensibility VARCHAR(2)'
        ' );'
    )
    constraint_ma = (
        """ALTER TABLE "MotionAccelerometer" ADD CONSTRAINT MotionAccelerometerPK PRIMARY KEY (timestamp, id_patient, id_device)""",
        """ALTER TABLE "MotionAccelerometer" ADD CONSTRAINT MotionAccelerometerPatientFK FOREIGN KEY (id_patient) REFERENCES "Patient" (id)""",
        """ALTER TABLE "MotionAccelerometer" ADD CONSTRAINT MotionAccelerometerDeviceFK FOREIGN KEY (id_device) REFERENCES "Device" (id)"""
    )

    for constraint in constraint_ma:
        cur.execute(constraint)

    cur.execute(
        "select create_hypertable('\"MotionAccelerometer\"', 'timestamp', 'id_patient', '4', chunk_time_interval => interval '1 day', if_not_exists => TRUE);")


def create_cont_query_ma_count_by_day(cur):
    cur.execute(
        "CREATE VIEW count_acm_by_day WITH (timescaledb.continuous, timescaledb.refresh_lag = '1 minute', timescaledb.refresh_interval = '1 second') "
        "AS "
        "SELECT"
        "   time_bucket('1 minute', timestamp) as bucket,"
        "   id_patient,"
        "   count(x_acm) count_x_acm, count(y_acm) count_y_acm, count(z_acm) count_z_acm "
        "FROM "
        "   \"MotionAccelerometer\" "
        "GROUP BY bucket, id_patient;"
    )


def create_cont_query_mg_count_by_day(cur):
    cur.execute(
        "CREATE VIEW count_gyro_by_day WITH (timescaledb.continuous, timescaledb.refresh_lag = '1 second', timescaledb.refresh_interval = '1 second') "
        "AS "
        "SELECT"
        "   time_bucket('1 minute', timestamp) as bucket,"
        "   id_patient,"
        "   count(x_gyro) count_x_gyro, count(y_gyro) count_y_gyro, count(z_gyro) count_z_gyro "
        "FROM "
        "   \"MotionGyroscope\" "
        "GROUP BY bucket, id_patient;"
    )


def create_cont_query_rr_count_by_day(cur):
    cur.execute(
        "CREATE VIEW count_rr_by_day WITH (timescaledb.continuous, timescaledb.refresh_lag = '1 minute', timescaledb.refresh_interval = '1 second') "
        "AS "
        "SELECT"
        "   time_bucket('1 minute', timestamp) as bucket,"
        "   id_patient,"
        "   count(\"RrInterval\") count_rr_interval "
        "FROM "
        "   \"RrInterval\" "
        "GROUP BY bucket, id_patient;"
    )


def create_data_model(my_connection, cur):
    cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)'")
    exists = cur.fetchall()

    if ('Patient',) not in exists:
        create_patient_table(cur)

    if ('Device',) not in exists:
        create_device_table(cur)

    if ('RrInterval',) not in exists:
        create_rrinterval_hypertable(cur)

    if ('MotionAccelerometer',) not in exists:
        create_motionaccelerometer_hypertable(cur)

    if ('MotionGyroscope',) not in exists:
        create_motiongyroscope_hypertable(cur)

    if ('RrInterval_by_min',) not in exists:
        create_count_rrinterval_by_min_hypertable(cur)

    cur.execute("select table_name from INFORMATION_SCHEMA.views WHERE table_schema = ANY (current_schemas(false))")
    exists = cur.fetchall()

    if ('count_acm_by_day',) not in exists:
        create_cont_query_ma_count_by_day(cur)

    if ('count_gyro_by_day',) not in exists:
        create_cont_query_mg_count_by_day(cur)

    if ('count_rr_by_day',) not in exists:
        create_cont_query_rr_count_by_day(cur)

    my_connection.commit()
