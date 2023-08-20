"""Prefect Prediction pipeline."""
import os
import pickle
import pandas as pd
import xgboost as xgb
import mlflow

from mlflow import xgboost, MlflowClient, set_tracking_uri
from prefect import variables, flow, task
from prefect_email import EmailServerCredentials, email_send_message
from prefect.context import get_run_context

from sklearn.metrics import mean_squared_error

from orchestration.utils import (  # pylint: disable=import-error
    pull_file_from_s3,
    resolve_s3_location,
    get_all_files_from_s3
)

from orchestration.constants import (
    MLFLOW_TRACKING_URI,
    StatusEnum,
    MLFLOW_PREDICTION_EXPERIMENT_NAME
)

pd.set_option('display.float_format', lambda x: '%.5f' % x)  # pylint: disable=consider-using-f-string


@task(log_prints=True, name="Prepare MLflow client")
def prepare_mlflow():
    """Setup MLFlow with experiment and tracking URL."""
    set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_PREDICTION_EXPERIMENT_NAME)
    client = MlflowClient()
    return client


@task(log_prints=True, name="Get Model from Model registry")
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


@task(log_prints=True, name="Pull prediction data from s3 bucket")
def pull_prediction_data_from_s3():
    """Pull prediction data from s3 bucket."""
    s3_path = variables.get(
        "PREDICTION_S3_PATH",
        default="s3://mlflow-artifacts-remote-amogh/prediction_data/"
    )
    prefix = 'prediction_data'
    is_environment_local = bool(variables.get("is_environment_local"))

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
        training_file_local_path = f'tmp/{s3_filename_list[0]}'
        print(f'training_file_local_path = {training_file_local_path}')
        print(f'is_environment_local = {is_environment_local}')

        tmp_path = os.path.join(os.getcwd(), 'tmp')
        is_directory_exists = os.path.exists(os.path.join(tmp_path, 'prediction_data'))
        if not is_directory_exists:
            print(f'Directory not found. Creating one.')
            prediction_path = os.path.join(tmp_path, 'prediction_data')
            os.makedirs(prediction_path)

        pull_file_from_s3(
            s3_tuple.bucket,
            s3_filename_list[0],
            training_file_local_path,
            is_environment_local
        )
        local_filename_list.append(training_file_local_path)

    return local_filename_list


@task(log_prints=True, name="Predict auction amount of players")
def predict_auction_amount(filename_list, booster):
    """Predict auction amount."""
    print(f'filename list = {filename_list}')
    with mlflow.start_run() as run:
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

        # dictionary_vector = DictVectorizer()
        dictionary_vector = None
        with open('preprocessor/preprocessor.b', 'rb') as f_in:
            dictionary_vector = pickle.load(f_in)

        pred_dicts = prediction_df[categorical + numerical].to_dict(orient='records')
        X_test = dictionary_vector.transform(pred_dicts)  # pylint: disable=invalid-name

        prediction_matrix = xgb.DMatrix(X_test)
        predictions = booster.predict(prediction_matrix)

        auction_result_df = pd.read_csv('data/actual_auction_amount_data.csv')
        auction_result_df['predicted_auction_amount'] = predictions
        auction_result_df['predicted_auction_amount'] = auction_result_df['predicted_auction_amount'] * 10000000
        auction_result_df.fillna(0, inplace=True)
        auction_result_df['difference'] = auction_result_df['predicted_auction_amount'] - auction_result_df['amount']
        auction_result_df['prediction_result'] = abs(auction_result_df['difference']).between(50000, 20000000)

        auction_result_df.to_csv('auction_result_data.csv', index=False)

        y_test = auction_result_df['amount'] / 10000000
        y_pred = auction_result_df['predicted_auction_amount'] / 10000000
        rmse = mean_squared_error(y_test, y_pred, squared=False)

        mlflow.log_metric("rmse", rmse)

        result_df = pd.read_csv('auction_result_data.csv')
        right_approx_result_list = list(result_df['prediction_result'].value_counts())
        auction_prediction_percentage = round((right_approx_result_list[0] / result_df.shape[0]), 3)

        mlflow.log_metric("auction_prediction_percentage", auction_prediction_percentage)

    return (auction_result_df, run.info.run_id)


@task(log_prints=True, name="Check prediction prediction value.")
def check_auction_prediction_percentage_and_alert(mlflow_client, run_id):
    """Check the prediction percentage metric and send email."""
    filter_string = f"run_id = '{run_id}'"
    print(f'filter string = {filter_string}')
    is_environment_local = bool(variables.get("is_environment_local", default="True"))

    # Finding the prediction experiment name
    experiments = mlflow_client.search_experiments(filter_string=f"name = '{MLFLOW_PREDICTION_EXPERIMENT_NAME}'")

    # Finding the current run
    if is_environment_local:
        runs = mlflow_client.search_runs(experiment_ids=experiments[0].experiment_id, filter_string=filter_string)
        auction_percentage = runs[0].data.metrics['auction_prediction_percentage']
    else:
        auction_percentage = 0.30

    print(f'Checking auction prediction percentage value = {auction_percentage}')

    return auction_percentage


@flow
def prediction_flow():
    """Flow of prediction pipeline."""
    print('prediction_flow')
    mlflow_client = prepare_mlflow()
    booster = get_model_from_mlflow_registry(mlflow_client)
    filename_list = pull_prediction_data_from_s3()
    _ , run_id = predict_auction_amount(filename_list, booster)
    auction_percentage = check_auction_prediction_percentage_and_alert(mlflow_client, run_id)
    context = get_run_context()

    # Check if auction percentage is less than 40.
    if auction_percentage < 0.40:
        email_server_credentials = EmailServerCredentials.load("gmail-server-notification-creds")
        flow_run_name = context.flow_run.name
        email_send_message(
            email_server_credentials=email_server_credentials,
            subject=f"Flow run {flow_run_name} violated the auction percentage",
            msg=f"""
                Flow run {flow_run_name} violated the auction percentage value.
                The percentage recorded = {auction_percentage}
            """,
            email_to=email_server_credentials.username,
        )


if __name__ == '__main__':
    prediction_flow()
