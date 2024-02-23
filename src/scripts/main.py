from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from config import (KAFKA_OPTIONS,
                    KAFKA_TOPIC_IN,
                    KAFKA_TOPIC_OUT,
                    JDBC_OPTIONS_IN,
                    JDBC_OPTIONS_OUT)

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream(spark)
    current_timestamp_utc = get_current_timestamp_utc()
    filtered_data = filter_stream_data(restaurant_read_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data(spark)
    result_df = join_and_transform_data(filtered_data, subscribers_data)

    # запускаем стриминг
    query = (result_df.writeStream
             .foreachBatch(save_to_postgresql_and_kafka)
             .start()
             )
    try:
        query.awaitTermination()
    finally:
        query.stop()


def create_spark_session(name: str = "RestaurantSubscribeStreamingService") -> SparkSession:
    # необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    return (SparkSession.builder
            .appName(name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
            )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (spark.readStream
            .format('kafka')
            .option(**KAFKA_OPTIONS)
            .option("subscribe", KAFKA_TOPIC_IN)
            .load()
            )


def get_current_timestamp_utc() -> int:
    return int(round(datetime.utcnow().timestamp()))


def filter_stream_data(df: DataFrame, current_timestamp_utc: int) -> DataFrame:
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

    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    return (df
            .withColumn('value', F.col('value').cast(StringType()))
            .withColumn('message', F.from_json(F.col("value"), incomming_message_schema))
            .select("message.*")
            .where((F.col('adv_campaign_datetime_start') <= current_timestamp_utc)
                   & (F.col('adv_campaign_datetime_end') <= current_timestamp_utc))
            )


def read_subscribers_data(spark: SparkSession) -> DataFrame:
    # вычитываем всех пользователей с подпиской на рестораны
    return (spark.read
            .format('jdbc')
            .option(**JDBC_OPTIONS_IN)
            .load())


def join_and_transform_data(filtered_data: DataFrame, subscribers_data: DataFrame) -> DataFrame:
    # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
    # Добавляем время создания события.
    return (filtered_data.join(subscribers_data, 'restaurant_id', 'inner')
            .withColumn("trigger_datetime_created", F.lit(int(round(datetime.utcnow().timestamp()))))
            .dropDuplicates(['client_id', 'restaurant_id'])
            )


def save_to_postgresql_and_kafka(df: DataFrame, epoch_id) -> None:
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()

    # записываем df в PostgreSQL с полем feedback
    write_to_postgresql(df.withColumn('feedback', F.lit(None).cast(StringType())))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    write_to_kafka(df)

    # очищаем память от df
    df.unpersist()


def write_to_postgresql(df):
    try:
        (df
         .write
         .option(**JDBC_OPTIONS_OUT)
         .mode('append')
         .save()
         )
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")


def write_to_kafka(df):
    try:
        # создаём df для отправки в Kafka. Сериализация в json
        df_kafka = (df
                    .select(F.to_json(F.struct(F.col('*'))).alias('value'))
                    .select('value')
                    )
        (df_kafka
         .write.format('kafka')
         .option(**KAFKA_OPTIONS)
         .option('topic', KAFKA_TOPIC_OUT)
         .option('truncate', False)
         .save()
         )
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")


if __name__ == "__main__":
    main()
