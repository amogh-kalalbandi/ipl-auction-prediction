"""Constants file for prefect pipelines."""
import os

LOCAL_ENDPOINT_URL = "http://localhost:4566/"
MLFLOW_EXPERIMENT_NAME = "xgboost-ipl-auction-prediction-2"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
MLFLOW_MODEL_NAME = "ipl-auction-prediction-model"
MLFLOW_PREDICTION_EXPERIMENT_NAME = "xgboost-ipl-auction-prediction-metrics"

class StatusEnum:
    """Enums for model registry statuses."""
    PRODUCTION = "Production"
    STAGING = "Staging"
