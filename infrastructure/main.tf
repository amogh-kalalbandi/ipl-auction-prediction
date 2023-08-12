terraform {
  required_version = ">= 1.0"
  backend "s3" {
    bucket  = "mlflow-artifacts-remote-amogh"
    key     = "ipl-auction-prediction.tfstate"
    region  = "us-east-2"
    encrypt = true
  }
}

provider "aws" {
  region  = var.aws_region
  profile = "default"
}

# model artifacts bucket
module "artifacts_s3_bucket" {
  source      = "./modules/s3"
  bucket_name = "${var.model_bucket_name}-${var.project_id}"
}
