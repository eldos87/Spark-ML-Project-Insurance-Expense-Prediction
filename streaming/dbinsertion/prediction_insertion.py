import pandas as pd
import sys

from insurance_exception.insurance_exception import InsuranceException as PredictionInsertionException
from dataBase.mongo_db import MongoDBOperation


class DBInsertion:
    def __init__(self, database_name, collection_name):
        self.database_name = database_name
        self.collection_name = collection_name
        self.mongo_db = MongoDBOperation()

    def insert_each_record(self, dataframe, epoch_id):
        try:
            dataframe = dataframe.toPandas()
            if dataframe.shape[0] > 0:
                dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
                self.mongo_db.insert_dataframe_into_collection(db_name=self.database_name,
                                                               collection_name=self.collection_name,
                                                               data_frame=dataframe)
                dataframe.to_csv("new_data.csv", index=None)
        except Exception as e:
            prediction_insertion_exception = PredictionInsertionException("Error occurred  in module [{0}] class [{1}] "
                                                                    "method [{2}] ".
                                                                    format(self.__module__, PredictionInsertionException.__name__,
                                                                           self.insert_each_record.__name__))
            raise Exception(prediction_insertion_exception.error_message_detail(str(e), sys)) from e
