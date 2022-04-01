from datetime import datetime
import sys
from insurance_exception.insurance_exception import InsuranceException as AppLoggerException
from dataBase.mongo_db import MongoDBOperation


class AppLogger:
    def __init__(self, log_database, log_collection_name):
        try:
            self.log_database = log_database
            self.log_collection_name = log_collection_name
            self.mongo_db_object = MongoDBOperation()
        except Exception as e:
            app_logger_exception = AppLoggerException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            "__init__"))
            raise Exception(app_logger_exception.error_message_detail(str(e), sys)) from e

    def log(self, log_message):
        try:
            log_data = {
                'log_time': str(datetime.now().date()) + "  "+ datetime.now().strftime("%H:%M:%S"),
                'message': log_message
            }

            self.mongo_db_object.insert_record_in_collection(
                self.log_database, self.log_collection_name, log_data)
        except Exception as e:
            app_logger_exception = AppLoggerException(
                "Error occurred in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.log.__name__))
            raise Exception(app_logger_exception.error_message_detail(str(e), sys)) from e
