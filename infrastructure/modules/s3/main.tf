resource "aws_s3_bucket" "ipl_auction_tf_artifacts_bucket" {
  bucket = var.bucket_name
  acl    = "private"
}

output "name" {
  value = aws_s3_bucket.ipl_auction_tf_artifacts_bucket.bucket
}
