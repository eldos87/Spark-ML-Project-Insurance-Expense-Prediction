import pymongo
import json
import pandas as pd
import sys
from insurance_exception.insurance_exception import InsuranceException as MongoDbException


class MongoDBOperation:
    def __init__(self, user_name=None, password=None):
        try:
            if user_name is None or password is None:
                credentials = {
                    "user_name": "thomas",
                    "password": "1234"
                }
                self.__user_name = credentials['user_name']
                self.__password = credentials['password']
            else:
                self.__user_name = user_name
                self.__password = password

        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed to instantiate mongo_db_object in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            "__init__"))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def get_database_client_object(self):
        try:
            url = 'mongodb+srv://{0}:{1}@cluster0.ypdvv.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'.format(
                self.__user_name, self.__password)
            client = pymongo.MongoClient(url)
            return client
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed to fetch  data base client object in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.get_database_client_object.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def create_database(self, client, db_name):
        try:
            return client[db_name]
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failure occured duing database creation steps in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.create_database.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def create_collection_in_database(self, database, collection_name):
        try:
            return database[collection_name]
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed during creating collection in database  in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.create_collection_in_database.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def is_collection_present(self, collection_name, database):
        try:
            collection_list = database.list_collection_names()
            if collection_name in collection_list:
                return True
            return False
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed during checking collection  in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.is_collection_present.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def get_collection(self, collection_name, database):
        try:
            collection = self.create_collection_in_database(database, collection_name)
            return collection
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in retrival of collection  in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.get_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def is_record_present(self, db_name, collection_name, record):
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            collection = self.get_collection(collection_name, database)
            record_found = collection.find(record)
            if record_found.count() > 0:
                client.close()
                return True
            else:
                client.close()
                return False
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in fetching record  in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.is_record_present.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def create_record(self, collection, data):
        try:
            collection.insert_one(data)  # insertion of record in collection
            return 1
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in inserting record in module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.create_record.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def insert_record_in_collection(self, db_name, collection_name, record):
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            collection = self.get_collection(collection_name, database)
            self.create_record(collection=collection, data=record)
            client.close()

        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in inserting record  in collection module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.insert_record_in_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def drop_collection(self, db_name, collection_name):
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            if self.is_collection_present(collection_name, database):
                collection_name = self.get_collection(collection_name, database)
                collection_name.drop()
            return True
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in droping collection module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.drop_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def insert_dataframe_into_collection(self, db_name, collection_name, data_frame):
        try:
            data_frame.reset_index(drop=True, inplace=True)
            records = list(json.loads(data_frame.T.to_json()).values())
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            collection = self.get_collection(collection_name, database)
            collection.insert_many(records)
            return len(records)
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in inserting dataframe in collection module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.insert_dataframe_into_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e

    def get_dataframe_of_collection(self, db_name, collection_name, query=None):
        try:
            client = self.get_database_client_object()
            database = self.create_database(client, db_name)
            collection = self.get_collection(collection_name=collection_name, database=database)
            if query is None:
                query = {}
            df = pd.DataFrame(list(collection.find(query)))
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)
            return df.copy()
        except Exception as e:
            mongo_db_exception = MongoDbException(
                "Failed in returning dataframe of collection module [{0}] class [{1}] method [{2}]"
                    .format(MongoDBOperation.__module__.__str__(), MongoDBOperation.__name__,
                            self.get_dataframe_of_collection.__name__))
            raise Exception(mongo_db_exception.error_message_detail(str(e), sys)) from e
