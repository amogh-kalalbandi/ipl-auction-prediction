```bash
    -- image: localstack/localstack

    -- S3 command to create bucket in local:

        awslocal s3api create-bucket --bucket mlflow-artifacts-remote-amogh --region us-east-1

    -- Command to copy the files to localstack s3 bucket:

    awslocal s3 cp data/training_data.csv s3://mlflow-artifacts-remote-amogh/training_data/training_data_2022.csv
    awslocal s3 cp data/prediction_data.csv s3://mlflow-artifacts-remote-amogh/prediction_data/prediction_data_2022.csv

    -- prefect command to start the worker pool:

        prefect worker start -p "job-pool" --type "process"

    -- mlflow server start command

    mlflow server --backend-store-uri=sqlite:///mlflow.db --default-artifact-root=s3://mlflow-artifacts-remote-amogh/auction-prediction-experiment/

    -- prefect deploy command

    prefect deploy -n ipl-auction-training && prefect deploy -n ipl-auction-prediction

    - docker command to run mlflow:

        mlflow server --backend-store-uri=sqlite:///mlflow.db --default-artifact-root=s3://mlflow-artifacts-remote-amogh/auction-prediction-experiment/ --host 0.0.0.0

    - s3 bucket name for artifact saving

        player-auction-data

```
