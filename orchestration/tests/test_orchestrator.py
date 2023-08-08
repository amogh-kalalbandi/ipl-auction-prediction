"""Unit test file for orchestrator."""
from mlflow import pyfunc

from orchestration import training_orchestrator
from orchestration import prediction_orchestrator

# from orchestration.constants import MLFLOW_MODEL_NAME


def test_pull_files_from_s3():
    """Testing pulling from files from S3 bucket."""
    expected_filename_list = ["tmp/training_data/training_data_2021.csv"]

    actual_filename_list = training_orchestrator.get_input_files_from_s3.fn()

    expected_filename_list.sort()
    actual_filename_list.sort()

    assert actual_filename_list == expected_filename_list


def test_dictionary_vectorizer_method():
    """Test dictionary vectorizers generated."""
    training_number_of_columns = 193
    filename_list = ["tmp/training_data/training_data_2021.csv"]

    dict_vectorizer = training_orchestrator.prepare_dictionary_vectorizers.fn(
        filename_list
    )

    assert training_number_of_columns == dict_vectorizer["train"].num_row()


def test_model_pulling_from_s3():
    """Test pulling of latest model from registry."""
    logged_model = 'runs:/a7f841c99cf7458c84c28800b5d6112e/models_mlflow'

    client = prediction_orchestrator.prepare_mlflow()
    expected_model = pyfunc.load_model(logged_model)
    actual_model = prediction_orchestrator.get_model_from_mlflow_registry(client)

    assert expected_model.metadata.run_id == actual_model.metadata.run_id


def test_pull_prediction_data_from_s3():
    """Test pulling prediction data from s3 bucket."""
    expected_filename_list = ['prediction_data/prediction_data_2022.csv']

    actual_filename_list = prediction_orchestrator.pull_prediction_data_from_s3()

    expected_filename_list.sort()
    actual_filename_list.sort()
    assert expected_filename_list == actual_filename_list


def test_predict_auction_amount():
    """Test prediction value generation method."""
