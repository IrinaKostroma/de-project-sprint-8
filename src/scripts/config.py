
JDBC_OPTIONS_IN = {
    "url": 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    "driver": "org.postgresql.Driver",
    "schema": 'public',
    "dbtable": 'subscribers_restaurants',
    "user": 'student',
    "password": 'de-student',
}

JDBC_OPTIONS_OUT = {
    "url": 'jdbc:postgresql://localhost:5432/postgres',
    "driver": "org.postgresql.Driver",
    "schema": 'public',
    "dbtable": 'subscribers_feedback',
    "user": 'jovyan',
    "password": 'jovyan',
}

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";'
}
KAFKA_TOPIC_IN = "universeta_in"
KAFKA_TOPIC_OUT = "universeta_out"
