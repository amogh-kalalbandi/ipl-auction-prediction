#!/usr/bin/env bash
exec prefect server start
exec echo "Prefect server started"
exec prefect deploy -n ipl-auction-training && prefect deploy -n ipl-auction-prediction
exec echo "Prefect code deployed"
exec mlflow server --backend-store-uri=sqlite:///mlflow.db --default-artifact-root=s3://mlflow-artifacts-remote-amogh/auction-prediction-experiment/ --host 0.0.0.0
exec echo "ML flow started"
exec prefect worker start -p "job-pool" --type "process"
