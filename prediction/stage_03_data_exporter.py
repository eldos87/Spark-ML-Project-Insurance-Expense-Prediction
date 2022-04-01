import sys
from utility import *
from dataBase.mongo_db import MongoDBOperation
from insurance_exception.insurance_exception import InsuranceException as GenericException

log_collection_name = "data_exporter"


class DataExporter:
    def __init__(self, config, logger):
        try:
            self.config = config
            self.logger = logger
            self.mongo_db = MongoDBOperation()
            self.dataset_database = self.config["database_detail"]["prediction_database_name"]
            self.dataset_collection_name = self.config["database_detail"]["dataset_prediction_collection_name"]
            self.prediction_file_from_db = self.config["artifacts"]['prediction_data']['prediction_file_from_db']
            self.master_csv = self.config["artifacts"]['prediction_data']['master_csv']
        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataExporter.__name__,
                            self.__init__.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e

    def export_dataframe_from_database(self):
        try:
            create_directory_path(self.prediction_file_from_db)
            self.logger.log(f"Creating dataframe of data stored db"
                            f"[{self.dataset_database}] and collection[{self.dataset_collection_name}]")
            df = self.mongo_db.get_dataframe_of_collection(db_name=self.dataset_database,
                                                           collection_name=self.dataset_collection_name)
            master_csv_file_path = os.path.join(self.prediction_file_from_db, self.master_csv)
            self.logger.log(f"master csv file will be generated at "
                            f"{master_csv_file_path}.")
            df.to_csv(master_csv_file_path, index=None, header=True)

        except Exception as e:
            generic_exception = GenericException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(self.__module__, DataExporter.__name__,
                            self.export_dataframe_from_database.__name__))
            raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


def export_main(config_path):
    try:
        logger = get_logger_object_of_prediction(config_path, log_collection_name)

        config = read_params(config_path)
        data_exporter = DataExporter(config=config, logger=logger)
        logger.log("Generating csv file from dataset stored in database.")
        data_exporter.export_dataframe_from_database()
        logger.log("Dataset has been successfully exported in directory and exiting export pipeline.")
    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(export_main.__module__,
                        export_main.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == '__main__':
    path = os.path.join("config", "params.yaml")
    export_main(config_path=path)

