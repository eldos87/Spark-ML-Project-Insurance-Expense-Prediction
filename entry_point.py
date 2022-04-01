import os
import sys

from utility import get_logger_object_of_training, get_logger_object_of_prediction
from training.stage_00_data_loader import loader_main
from training.stage_01_data_validator import validation_main
from training.stage_02_data_transformer import transform_main
from training.stage_03_data_exporter import export_main
from training.stage_04_model_trainer import train_main

from prediction.stage_00_data_loader import loader_main as pred_loader_main
from prediction.stage_01_data_validator import validation_main as pred_validation_main
from prediction.stage_02_data_transformer import transform_main as pred_transform_main
from prediction.stage_03_data_exporter import export_main as pred_export_main
from prediction.stage_04_model_predictor import predict_main
from insurance_exception.insurance_exception import InsuranceException as GenericException

collection_name = "main_pipeline"
path = os.path.join("config", "params.yaml")


def begin_training():
    try:
        logger = get_logger_object_of_training(config_path=path, collection_name=collection_name)

        logger.log("Data loading begin..")
        loader_main(config_path=path)
        logger.log("Data loading completed..")

        logger.log("Data validation begin..")
        validation_main(config_path=path)
        logger.log("Data validation completed..")

        logger.log("Data transformation begin..")
        transform_main(config_path=path)
        logger.log("Data transformation completed..")

        logger.log("Export operation begin..")
        export_main(config_path=path)
        logger.log("Export operation completed..")

        logger.log("Training begin..")
        train_main(config_path=path)
        logger.log(f"Training completed")

        print('Training completed successfully')
    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(begin_training.__module__,
                        begin_training.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


def begin_prediction():
    try:
        logger = get_logger_object_of_prediction(config_path=path, collection_name=collection_name)

        logger.log("Data loading begin..")
        pred_loader_main(config_path=path)
        logger.log("Data loading completed..")

        logger.log("Data validation begin..")
        pred_validation_main(config_path=path)
        logger.log("Data validation completed..")

        logger.log("Data transformation begin..")
        pred_transform_main(config_path=path)
        logger.log("Data transformation completed..")

        logger.log("Export operation begin..")
        pred_export_main(config_path=path)
        logger.log("Export operation completed..")

        logger.log("Prediction begin..")
        predict_main(config_path=path)
        logger.log("Prediction completed")

        print('Prediction completed successfully')
    except Exception as e:
        generic_exception = GenericException(
            "Error occurred in module [{0}] method [{1}]"
                .format(begin_prediction.__module__,
                        begin_prediction.__name__))
        raise Exception(generic_exception.error_message_detail(str(e), sys)) from e


if __name__ == "__main__":
    # begin_training()
    begin_prediction()


