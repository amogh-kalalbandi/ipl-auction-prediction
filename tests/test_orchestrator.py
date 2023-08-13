"""Unit test file for orchestrator."""
from mlflow import xgboost

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
    logged_model = 'runs:/240ae65d48814f8091d5ce9039928edf/models_mlflow'

    client = prediction_orchestrator.prepare_mlflow.fn()
    expected_model = xgboost.load_model(logged_model)
    actual_model = prediction_orchestrator.get_model_from_mlflow_registry.fn(client)

    assert expected_model.attributes()['best_score'] == actual_model.attributes()['best_score']


def test_pull_prediction_data_from_s3():
    """Test pulling prediction data from s3 bucket."""
    expected_filename_list = ['tmp/prediction_data/prediction_data_2022.csv']

    actual_filename_list = prediction_orchestrator.pull_prediction_data_from_s3.fn()

    expected_filename_list.sort()
    actual_filename_list.sort()
    assert expected_filename_list == actual_filename_list


def test_predict_auction_amount():
    """Test prediction value generation method."""
    actual_filename_list = prediction_orchestrator.pull_prediction_data_from_s3.fn()
    client = prediction_orchestrator.prepare_mlflow.fn()
    actual_model = prediction_orchestrator.get_model_from_mlflow_registry.fn(client)
    actual_prediction_df, _ = prediction_orchestrator.predict_auction_amount.fn(actual_filename_list, actual_model)

    expected_rows_in_preds = 173

    assert actual_prediction_df.shape[0] == expected_rows_in_preds
