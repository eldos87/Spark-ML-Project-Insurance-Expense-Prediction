from streaming.producer.kafka_data_producer import KafkaDataProducer
from streaming.spark_manager.spark_manager import SparkManager

if __name__ == "__main__":
    try:
        path = "data/streaming_files"
        spark_session = SparkManager().get_spark_session_object()
        kfk_data_producer = KafkaDataProducer(spark_session=spark_session)
        kfk_data_producer.send_csv_data_to_kafka_topic(directory_path=path)
    except Exception as e:
        raise e

"""To execute : spark-submit csv_to_kafka.py"""