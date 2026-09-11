# MinIO provider — points at the local MinIO instance from docker-compose.
#
# From the host machine the endpoint is `localhost:9000`. From inside
# another Docker container on the same network it would be `minio:9000`;
# override `minio_endpoint` via -var or a *.tfvars file in that case.

provider "minio" {
  minio_server   = var.minio_endpoint
  minio_user     = var.minio_user
  minio_password = var.minio_password
  minio_ssl      = false
  minio_region   = "us-east-1"
}
