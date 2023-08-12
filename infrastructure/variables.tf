variable "aws_region" {
  description = "AWS region to create resources"
  default     = "us-east-2"
}

variable "project_id" {
  description = "project ID"
  default     = "ipl-auction-prediction"
}

variable "model_bucket_name" {
  description = "s3_bucket"
}
