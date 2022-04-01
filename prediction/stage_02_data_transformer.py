import os
import sys
import pandas as pd
from utility import read_params, get_logger_object_of_prediction
from dataBase.mongo_db import MongoDBOperation
from insurance_exception.insurance_exception import InsuranceException as GenericException

log_collection_name = "data_transformer"


class DataTransformer:
    def __init__(self, config, logger):
        try:
            self.config = config
            self.logger = logger
            self.good_file_path = self.config["artifacts"]['prediction_data']['good_file_path']
            self.unwanted_column_names = self.config["dataset"]['unwanted_column']
            self.mongo_db = MongoDBOperation()
            self.dataset_database = self.config["database_detail"]["prediction_database_name"]
            self.dataset_collection_name = self.config["database_detail"]["dataset_prediction_collection_name"]
            self.mongo_db.drop_collection(self.dataset_database, self.dataset_collection_name)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataTransformer.__name__,
                            self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def unite_dataset(self):
        try:
            dataset_list = []
            for file in os.listdir(self.good_file_path):
                dataset_list.append(pd.read_csv(os.path.join(self.good_file_path, file)))
            df = pd.concat(dataset_list)
            df = self.remove_unwanted_column(df)
            self.logger.log(f"Inserting dataset into database {self.dataset_database} "
                            f"collection_name: {self.dataset_collection_name}")
            self.mongo_db.insert_dataframe_into_collection(self.dataset_database, self.dataset_collection_name, df)
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataTransformer.__name__,
                            self.unite_dataset.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def remove_unwanted_column(self, df):
        try:
            column_to_remove = list(filter(lambda x: x in df.columns, [self.unwanted_column_names]))
            if len(column_to_remove) > 0:
                df.drop(column_to_remove, axis=1, inplace=True)
                return df
            return df
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataTransformer.__name__,
                            self.remove_unwanted_column.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


def transform_main(config_path):
    try:
        logger = get_logger_object_of_prediction(config_path, log_collection_name)
        config = read_params(config_path)
        data_transformer = DataTransformer(config=config, logger=logger)
        logger.log('Data Transformation started')
        data_transformer.unite_dataset()
        logger.log('Data Transformation completed')

    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(transform_main.__module__,
                        transform_main.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == '__main__':
    path = os.path.join("config", "params.yaml")
    transform_main(config_path=path)
