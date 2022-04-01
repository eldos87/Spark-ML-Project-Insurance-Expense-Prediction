import os

from pyspark.sql.functions import *

from insurance_exception.insurance_exception import InsuranceException as KafkaDataConsumerException
from utility import read_params
from streaming.dbinsertion.prediction_insertion import DBInsertion


class KafkaDataConsumer:
    def __init__(self, schema_string, database_name, collection_name, spark_session, transformers,
                 processing_interval_second=5):
        path = os.path.join("config", "params.yaml")
        self.config = read_params(path)
        self.kafka_topic_name = self.config['kafka']['topic_name']
        self.kafka_bootstrap_server = self.config['kafka']['kafka_bootstrap_server']
        self.schema = schema_string
        self.db_insertion = DBInsertion(database_name=database_name, collection_name=collection_name)
        self.spark_session = spark_session
        self.transformers = transformers
        self.processing_interval_second = processing_interval_second
        self.query = None

    def receive_csv_data_from_kafka_topics(self):
        try:
            dataframe = self.spark_session \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_server) \
                .option("subscribe", self.kafka_topic_name) \
                .option("startingOffsets", "latest") \
                .load()

            dataframe_1 = dataframe.selectExpr("CAST(value as STRING) ", "timestamp")
            dataframe_2 = dataframe_1.select(from_csv(dataframe_1.value, self.schema).alias("records"), "timestamp")
            dataframe_3 = dataframe_2.select("records.*", "timestamp")
            transformed_df = dataframe_3

            for transformer in self.transformers:
                transformed_df = transformer.transform(transformed_df)

            self.query = transformed_df.writeStream.trigger(
                processingTime=f'{self.processing_interval_second} seconds').foreachBatch(
                self.db_insertion.insert_each_record).start()
            self.query.awaitTermination()

        except Exception as e:
            kafka_data_consumer_exception = KafkaDataConsumerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaDataConsumer.__name__,
                           self.receive_csv_data_from_kafka_topics.__name__))
            raise Exception(kafka_data_consumer_exception.error_message_detail(str(e), sys)) from e

