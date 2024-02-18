from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

KAFKA_TOPIC_IN = 'universeta_in'
KAFKA_TOPIC_OUT = 'universeta_out'


def main():
    print('start')
    # необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = (SparkSession.builder
             .appName("RestaurantSubscribeStreamingService")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.jars.packages", spark_jars_packages)
             .getOrCreate()
             )

    restaurants = get_restaurants(spark)
    subscribers = get_subscribers(spark)
    offers = get_offers(restaurants, subscribers)

    # запускаем стриминг
    (offers.writeStream
     .foreachBatch(foreach_batch_function)
     .start()
     .awaitTermination()
     )


def get_restaurants(spark):
    # читаем из топика Kafka сообщения с акциями от ресторанов
    restaurant_df = (spark.readStream
                     .format('kafka')
                     .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
                     .option('kafka.security.protocol', 'SASL_SSL')
                     .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
                     .option('kafka.sasl.jaas.config',
                             'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";')
                     .option("subscribe", KAFKA_TOPIC_IN)
                     .load()
                     )

    # определяем схему входного сообщения для json
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType()),
        StructField("adv_campaign_id", StringType()),
        StructField("adv_campaign_content", StringType()),
        StructField("adv_campaign_owner", StringType()),
        StructField("adv_campaign_owner_contact", StringType()),
        StructField("adv_campaign_datetime_start", LongType()),
        StructField("adv_campaign_datetime_end", LongType()),
    ])

    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    return (restaurant_df
            .withColumn('value', F.col('value').cast(StringType()))
            .withColumn('message', F.from_json(F.col("value"), incomming_message_schema))
            .select("message.*")
            .where((F.col('adv_campaign_datetime_start') <= current_timestamp_utc)
                   & (F.col('adv_campaign_datetime_end') <= current_timestamp_utc))
            )


def get_subscribers(spark):
    # вычитываем всех пользователей с подпиской на рестораны
    return (spark.read
            .format('jdbc')
            .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
            .option('driver', 'org.postgresql.Driver')
            .option('dbtable', 'public.subscribers_restaurants')
            .option('user', 'student')
            .option('password', 'de-student')
            )


def get_offers(subscribers: DataFrame, restaurants: DataFrame) -> DataFrame:
    # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
    # Добавляем время создания события.
    return (restaurants.join(subscribers, 'restaurant_id', 'inner')
            .withColumn("trigger_datetime_created", F.lit(int(round(datetime.utcnow().timestamp()))))
            .dropDuplicates(['client_id', 'restaurant_id'])
            .withWatermark('timestamp', '1 minutes')
            )


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    (df
     .withColumn('feedback', F.lit(None).cast(StringType()))
     .write.format('jdbc')
     .option('url', 'jdbc:postgresql://localhost:5432/postgres')
     .option('driver', 'org.postgresql.Driver')
     .option('dbtable', 'public.create_subscribers_feedback')
     .option('user', 'jovyan')
     .option('password', 'jovyan')
     .mode('append')
     .save()
     )

    # создаём df для отправки в Kafka. Сериализация в json
    df_kafka = (df
                .select(F.to_json(F.struct(F.col('*'))).alias('value'))
                .select('value')
                )

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    (df_kafka
     .write.format('kafka')
     .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
     .option('kafka.security.protocol', 'SASL_SSL')
     .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
     .option('kafka.sasl.jaas.config',
             'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";')
     .option('topic', KAFKA_TOPIC_OUT)
     .option('truncate', False)
     .save()
     )

    # очищаем память от df
    df.unpersist()


if __name__ == "__main__":
    main()
