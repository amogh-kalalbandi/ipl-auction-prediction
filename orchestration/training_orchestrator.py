"""Prefect Training pipeline."""
import logging

import pandas as pd
import xgboost as xgb
import mlflow

from mlflow import MlflowClient, set_tracking_uri

from sklearn.feature_extraction import DictVectorizer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

from utils import (  # pylint: disable=import-error
    pull_file_from_s3,
    resolve_s3_location,
    get_all_files_from_s3,
)
from prefect import variables, flow, task

from constants import MLFLOW_EXPERIMENT_NAME, MLFLOW_TRACKING_URI, MLFLOW_MODEL_NAME


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)


@task(log_prints=True, name="Prepare MLflow client")
def prepare_mlflow():
    """Setup MLFlow with experiment and tracking URL."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    experiment = mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    return experiment.experiment_id


@task(log_prints=True, name="Get Input Files from S3 bucket.")
def get_input_files_from_s3():
    """Pull Input files from S3."""
    is_pull_all_files_enabled = variables.get("PULL_ALL_FILES", default="false")
    pull_from_year = int(variables.get("PULL_FROM_YEAR", default="2021"))
    s3_path = variables.get(
        "INPUT_S3_PATH", default="s3://mlflow-artifacts-remote-amogh/training_data/"
    )
    is_environment_local = bool(variables.get("IS_ENVIRONMENT_LOCAL", default="true"))
    prefix = "training_data"
    local_filename_list = []

    s3_tuple = resolve_s3_location(s3_path)

    if is_pull_all_files_enabled == "true":
        print("Pulling all files from S3.")
        filename_list = get_all_files_from_s3(
            s3_tuple.bucket, prefix, is_environment_local
        )

    else:
        print(f"Pulling all files from S3 from this year = {pull_from_year}.")
        filename_list = []
        s3_filename_list = get_all_files_from_s3(
            s3_tuple.bucket, prefix, is_environment_local
        )

        for each_key in s3_filename_list:
            # Getting the year from s3 file
            each_filename = each_key.split("/")[1]
            year_value = int(each_filename.split(".")[0].split("_")[2])
            if year_value >= pull_from_year:
                filename_list.append(each_key)

    # Pull each file from S3 bucket.
    for each_file in filename_list:
        training_file_local_path = f"tmp/{each_file}"
        print(f"each_file = {each_file}")
        print(f"bucket_name = {s3_tuple.bucket}")
        pull_file_from_s3(
            s3_tuple.bucket, each_file, training_file_local_path, is_environment_local
        )
        local_filename_list.append(training_file_local_path)

    return local_filename_list


@task(log_prints=True, name="Prepare Input dictionary vectorizers.")
def prepare_dictionary_vectorizers(filename_list):
    """Prepare dv dictionaries for model training."""

    # Prepare the training dataframe.
    training_df = pd.DataFrame()
    for each_filename in filename_list:
        temp_df = pd.read_csv(each_filename)
        training_df = pd.concat([training_df, temp_df])

    categorical = ["role", "player_origin"]

    numerical = [
        "tot_runs",
        "highest_score",
        "number_of_hundreds",
        "number_of_fifties",
        "number_of_fours",
        "number_of_sixes",
        "batting_average",
        "batting_strike_rate",
        "wickets",
        "number_of_four_fers",
        "number_of_five_fers",
        "max_number_of_wickets_per_match",
        "bowling_average",
        "bowling_strike_rate",
        "total_matches",
    ]

    target = "amount"

    training_df[target] = training_df[target] / 10000000

    # Split the training data into train and test
    df_train, df_val = train_test_split(training_df, test_size=0.2, random_state=1)
    dict_vectorizer = DictVectorizer()

    train_dicts = df_train[categorical + numerical].to_dict(orient="records")
    X_train = dict_vectorizer.fit_transform(train_dicts)  # pylint: disable=invalid-name
    y_train = df_train[target].values

    val_dicts = df_val[categorical + numerical].to_dict(orient="records")
    X_val = dict_vectorizer.transform(val_dicts)  # pylint: disable=invalid-name
    y_val = df_val[target].values

    train = xgb.DMatrix(X_train, label=y_train)
    valid = xgb.DMatrix(X_val, label=y_val)

    return {"train": train, "valid": valid, "y_val": y_val}


@task(log_prints=True, name="Train the ML model")
def train_ml_model(train, valid, y_val):
    """Train ML model with dv dictionaries."""
    # Track the run in mlflow.
    with mlflow.start_run() as run:
        params = {
            "learning_rate": 0.15381188612462107,
            "max_depth": 5,
            "min_child_weight": 10.620379334384285,
            "objective": "reg:linear",
            "reg_alpha": 0.03951664300214618,
            "reg_lambda": 0.058208962170917034,
            "seed": 42,
        }

        mlflow.set_tag("artifact_space", "s3")

        booster = xgb.train(
            params=params,
            dtrain=train,
            num_boost_round=30,
            evals=[(valid, "validation")],
            early_stopping_rounds=15,
        )

        y_pred = booster.predict(valid)

        rmse = mean_squared_error(y_val, y_pred, squared=False)
        mlflow.log_metric("rmse", rmse)

        mlflow.xgboost.log_model(booster, artifact_path="models_mlflow")

        print(f"The new run id generated = {run.info.run_id}")
        return run.info.run_id


@task(log_prints=True, name="Move the latest trained model to production")
def move_latest_model_to_registry(experiment_id, mlflow_run_id):
    """Move latest trained model to MLFlow model registry."""
    client = MlflowClient()
    set_tracking_uri(MLFLOW_TRACKING_URI)
    artifact_path = "model"

    # Move current model in production to Staging.
    registry_list = client.search_registered_models()

    for each_registry in registry_list:
        for each_model in each_registry.latest_versions:
            if each_model.current_stage == "Production":
                client.transition_model_version_stage(
                    name=MLFLOW_MODEL_NAME, version=each_model.version, stage="Staging"
                )

    print("Existing model is moved to Staging.")
    runs = client.search_runs(experiment_ids=experiment_id)

    print(f"Number of runs found in experiment = {len(runs)}")
    print(f"Experiment ID passed = {experiment_id}")

    # Move the new model to Production.
    for each_run in runs:
        print(f"Run Id = {each_run.info.run_id}")
        if each_run.info.run_id == mlflow_run_id:
            model_uri = f"runs:/{mlflow_run_id}/{artifact_path}"
            model_details = mlflow.register_model(
                model_uri=model_uri, name=MLFLOW_MODEL_NAME
            )
            client.set_model_version_tag(
                model_details.name, model_details.version, "model_type", "xgboost"
            )
            client.set_model_version_tag(
                model_details.name, model_details.version, "run_type", "latest"
            )
            client.transition_model_version_stage(
                name=model_details.name,
                version=model_details.version,
                stage="Production",
            )


@flow
def pipeline_flow():
    """Flow of the pipeline"""
    experiment_id = prepare_mlflow()
    filename_list = get_input_files_from_s3()
    dictionary_vectors = prepare_dictionary_vectorizers(filename_list)
    mlflow_run_id = train_ml_model(
        dictionary_vectors["train"],
        dictionary_vectors["valid"],
        dictionary_vectors["y_val"],
    )
    move_latest_model_to_registry(experiment_id, mlflow_run_id)


if __name__ == "__main__":
    pipeline_flow()
