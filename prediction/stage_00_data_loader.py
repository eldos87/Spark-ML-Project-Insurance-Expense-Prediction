import sys
from utility import *
from insurance_exception.insurance_exception import InsuranceException as GenericException

log_collection_name = "data_loader"


def loader_main(config_path):
    try:
        logger = get_logger_object_of_prediction(config_path, log_collection_name)
        logger.log("Starting data loading operation.\nReading configuration file.")

        config = read_params(config_path)
        downloader_path = config['data_download']['prediction_file_path']
        download_path = config['data_source']['Prediction_Batch_Files']

        logger.log("Configuration detail has been fetched from configuration file.")
        # removing any existing training files from local
        logger.log(f"Cleaning local directory [{download_path}]  for training.")
        clean_data_source_dir(download_path, logger)

        logger.log(f"Cleaning completed. Directory has been cleared")
        # transferring file from shared location to local system
        logger.log("Data will be transferred from shared storage to local system")

        for file in os.listdir(downloader_path):
            if not file.endswith(".csv"):
                continue
            shutil.copy(os.path.join(downloader_path, file), os.path.join(download_path, file))
            logger.log(f" file: {file} from Source dir: {downloader_path} is copied to dest dir: {download_path}")
        logger.log("Data has been loaded to local system")

    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(loader_main.__module__,
                        loader_main.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == '__main__':
    path = os.path.join("config", "params.yaml")
    loader_main(config_path=path)

