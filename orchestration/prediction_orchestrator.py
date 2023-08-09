"""Prefect Prediction pipeline."""
import pandas as pd
import xgboost as xgb
from mlflow import xgboost, MlflowClient, set_tracking_uri

from prefect import variables

from sklearn.feature_extraction import DictVectorizer

from orchestration.utils import (  # pylint: disable=import-error
    pull_file_from_s3,
    resolve_s3_location,
    get_all_files_from_s3
)

from orchestration.constants import (
    MLFLOW_TRACKING_URI,
    StatusEnum
)

pd.set_option('display.float_format', lambda x: '%.5f' % x)  # pylint: disable=consider-using-f-string


# @task(log_prints=True, name="Prepare MLflow client")
def prepare_mlflow():
    """Setup MLFlow with experiment and tracking URL."""
    set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    return client


def get_model_from_mlflow_registry(mlflow_client):
    """Pull latest model file from model registry."""
    registry_list = mlflow_client.search_registered_models()

    for each_registry in registry_list:
        for each_model in each_registry.latest_versions:
            if each_model.current_stage == StatusEnum.PRODUCTION:
                model_run_id = each_model.run_id
                model = xgboost.load_model(f"runs:/{model_run_id}/models_mlflow")

    mlflow_client.download_artifacts(run_id=model_run_id, path='preprocessor', dst_path='.')

    return model


def pull_prediction_data_from_s3():
    """Pull prediction data from s3 bucket."""
    s3_path = variables.get(
        "PREDICTION_S3_PATH",
        default="s3://mlflow-artifacts-remote-amogh/prediction_data/"
    )
    prefix = 'prediction_data'
    is_environment_local = bool(variables.get("IS_ENVIRONMENT_LOCAL", default="true"))

    s3_tuple = resolve_s3_location(s3_path)

    s3_filename_list = get_all_files_from_s3(
        s3_tuple.bucket,
        prefix,
        is_environment_local
    )

    s3_filename_list.sort(reverse=True)
    local_filename_list = []
    print(f'Filename list in S3 bucket = {s3_filename_list}')
    if s3_filename_list:
        training_file_local_path = f"tmp/{s3_filename_list[0]}"
        pull_file_from_s3(
            s3_tuple.bucket,
            s3_filename_list[0],
            training_file_local_path,
            is_environment_local
        )
        local_filename_list.append(training_file_local_path)

    return local_filename_list


def predict_auction_amount(filename_list, booster):
    """Predict auction amount"""
    print(f'filename list = {filename_list}')
    prediction_df = pd.DataFrame()
    for each_filename in filename_list:
        temp_df = pd.read_csv(each_filename)
        prediction_df = pd.concat([prediction_df, temp_df])

    categorical = [
        'role',
        'player_origin'
    ]

    numerical = [
        'tot_runs',
        'highest_score',
        'number_of_hundreds',
        'number_of_fifties',
        'number_of_fours',
        'number_of_sixes',
        'batting_average',
        'batting_strike_rate',
        'wickets',
        'number_of_four_fers',
        'number_of_five_fers',
        'max_number_of_wickets_per_match',
        'bowling_average',
        'bowling_strike_rate',
        'total_matches',
    ]

    print(prediction_df.shape)
    # import ipdb; ipdb.set_trace()

    dictionary_vector = DictVectorizer()
    pred_dicts = prediction_df[categorical + numerical].to_dict(orient='records')
    X_test = dictionary_vector.fit_transform(pred_dicts)  # pylint: disable=invalid-name

    prediction_matrix = xgb.DMatrix(X_test)
    predictions = booster.predict(prediction_matrix)

    auction_result_df = pd.read_csv('../data/actual_auction_amount_data.csv')
    auction_result_df['predicted_auction_amount'] = predictions
    auction_result_df['predicted_auction_amount'] = auction_result_df['predicted_auction_amount'] * 10000000
    auction_result_df.fillna(0, inplace=True)
    auction_result_df['prediction_result'] = abs(auction_result_df['difference']).between(50000, 20000000)

    auction_result_df.to_csv('auction_result_data.csv', index=False)
    return auction_result_df


def update_rmse_value():
    """Calculate RMSE value and store it."""
    print('update_rmse_value')


def prediction_flow():
    """Flow of prediction pipeline."""
    print('prediction_flow')
    mlflow_client = prepare_mlflow()
    booster = get_model_from_mlflow_registry(mlflow_client)
    filename_list = pull_prediction_data_from_s3()
    predict_auction_amount(filename_list, booster)
    update_rmse_value()


if __name__ == '__main__':
    prediction_flow()
