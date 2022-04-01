import os

from pyspark.ml import PipelineModel
from pyspark.ml.regression import RandomForestRegressionModel

from streaming.spark_manager.spark_manager import SparkManager
from streaming.consumer.kafka_data_consumer import KafkaDataConsumer


if __name__ == "__main__":
    spark_session = SparkManager().get_spark_session_object()

    schema_string = "age INT,sex STRING,bmi DOUBLE,children INT,smoker STRING,region STRING"
    database_name = "stream_prediction"
    collection_name = "streaming_prediction_output"

    transformer_list = []
    pipeline_model = PipelineModel.load(os.path.join("artifacts", "pipeline", "pipeline_model"))
    random_forest_model = RandomForestRegressionModel.load(os.path.join("artifacts", "model", "random_forest_regressor"))
    transformer_list.append(pipeline_model)
    transformer_list.append(random_forest_model)

    kfk_consumer = KafkaDataConsumer(spark_session=spark_session, schema_string=schema_string,
                                      database_name=database_name, collection_name=collection_name,
                                      transformers=transformer_list
                                      )
    kfk_consumer.receive_csv_data_from_kafka_topics()

""" To execute : spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1  kafka_to_spark_consumer.py"""