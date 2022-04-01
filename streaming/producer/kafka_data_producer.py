import os
import sys

from kafka import KafkaProducer
from utility import read_params
import time
from insurance_exception.insurance_exception import InsuranceException as KafkaDataProducerException


class KafkaDataProducer:

    def __init__(self, spark_session):
        try:
            path = os.path.join("config", "params.yaml")
            self.config = read_params(config_path=path)
            self.kafka_topic_name = self.config['kafka']['topic_name']
            self.kafka_bootstrap_server = self.config['kafka']['kafka_bootstrap_server']
            self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_server,
                                                value_serializer=lambda x: x.encode('utf-8'))
            self.spark_session = spark_session
        except Exception as e:
            kafka_data_producer_exp = KafkaDataProducerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaDataProducer.__name__,
                           self.__init__.__name__))
            raise Exception(kafka_data_producer_exp.error_message_detail(str(e), sys)) from e

    def send_csv_data_to_kafka_topic(self, directory_path):
        try:
            files = os.listdir(directory_path)
            for file in files:
                if not file.endswith(".csv"):
                    continue
                file_path = os.path.join(directory_path, file)
                df = self.spark_session.read.csv(file_path, header=True, inferSchema=True)

                # sending dataframe to kafka topic iteratively
                for row in df.rdd.toLocalIterator():
                    message = ",".join(map(str, list(row)))
                    print(message)
                    self.kafka_producer.send(self.kafka_topic_name, message)
                    time.sleep(1)   # put a time-delay of 1sec to simulate streaming

        except Exception as e:
            kafka_data_producer_exp = KafkaDataProducerException(
                "Error occurred  in module [{0}] class [{1}] method [{2}] ".
                    format(self.__module__, KafkaDataProducer.__name__,
                           self.__init__.__name__))
            raise Exception(kafka_data_producer_exp.error_message_detail(str(e), sys)) from e