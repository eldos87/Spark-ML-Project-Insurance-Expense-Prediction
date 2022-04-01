import json
from datetime import datetime
import yaml
import os
import shutil
from application_logging.logger import AppLogger


def get_time():
    return datetime.now().strftime("%H:%M:%S").__str__()


def get_date():
    return datetime.now().date().__str__()


def create_directory_path(path, is_recreate=True):
    try:
        if is_recreate:
            if os.path.exists(path):
                shutil.rmtree(path, ignore_errors=False)
        os.makedirs(path, exist_ok=True)
        return True
    except Exception as e:
        raise e


def clean_data_source_dir(path, logger):
    try:
        if not os.path.exists(path):
            os.mkdir(path)
        for file in os.listdir(path):
            logger.log(f"{os.path.join(path, file)} file will be deleted.")
            os.remove(os.path.join(path, file))
            logger.log(f"{os.path.join(path, file)} file has been deleted.")
    except Exception as e:
        raise e


def get_logger_object_of_training(config_path, collection_name):
    config = read_params(config_path)
    database_name = config['database_detail']['training_database_name']
    logger = AppLogger(log_database=database_name, log_collection_name=collection_name)
    return logger


def get_logger_object_of_prediction(config_path, collection_name):
    config = read_params(config_path)
    database_name = config['database_detail']['prediction_database_name']
    logger = AppLogger(log_database=database_name, log_collection_name=collection_name)
    return logger


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


def values_from_schema_function(schema_path):
    try:
        with open(schema_path, 'r') as r:
            dic = json.load(r)
            r.close()

        pattern = dic['SampleFileName']
        length_of_date_stamp_in_file = dic['LengthOfDateStampInFile']
        length_of_time_stamp_in_file = dic['LengthOfTimeStampInFile']
        column_names = dic['ColName']
        number_of_columns = dic['NumberofColumns']
        return pattern, length_of_date_stamp_in_file, length_of_time_stamp_in_file, column_names, number_of_columns
    except ValueError:
        raise ValueError

    except KeyError:
        raise KeyError

    except Exception as e:
        raise e

