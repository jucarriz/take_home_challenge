variable "minio_endpoint" {
  description = "MinIO S3 API endpoint (host:port, no scheme)."
  type        = string
  default     = "localhost:9000"
}

variable "minio_user" {
  description = "MinIO access key."
  type        = string
  default     = "minioadmin"
}

variable "minio_password" {
  description = "MinIO secret key."
  type        = string
  default     = "minioadmin"
  sensitive   = true
}

variable "bucket_names" {
  description = "Medallion layer buckets to create."
  type        = set(string)
  default     = ["bronze", "silver", "gold"]
}
