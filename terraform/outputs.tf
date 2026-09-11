output "bucket_names" {
  description = "Names of the medallion layer buckets managed by Terraform."
  value       = sort([for b in minio_s3_bucket.layer : b.bucket])
}

output "bucket_urls" {
  description = "Console URLs for each bucket."
  value = {
    for b in minio_s3_bucket.layer :
    b.bucket => "http://${var.minio_endpoint}/${b.bucket}"
  }
}
