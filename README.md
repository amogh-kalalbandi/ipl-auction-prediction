```bash
    -- image: localstack/localstack

    -- S3 command to create bucket in local:

        awslocal s3api create-bucket --bucket mlflow-artifacts-remote-amogh --region us-east-1

    -- prefect command to start the worker pool:

        prefect worker start -p "job-pool" --type "process"

    -- prefect deploy command

    -- mlflow server start command

    mlflow server --backend-store-uri=sqlite:///mlflow.db --default-artifact-root=s3://mlflow-artifacts-remote-amogh/auction-prediction-experiment/

    -- prefect deploy command

    prefect deploy -n ipl-auction-training
    prefect deploy -n ipl-auction-prediction

```
