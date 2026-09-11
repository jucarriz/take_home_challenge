# Terraform root config for the Aurelia data platform.
#
# Manages the MinIO buckets (bronze / silver / gold) declaratively so
# their lifecycle is version-controlled and re-runnable, instead of
# relying on the imperative `minio-init` container shell script.
#
# The `minio-init` container in docker-compose.yml is kept as an
# alternative for reviewers who don't want to install Terraform:
# both paths are idempotent and produce the same three buckets.

terraform {
  required_version = ">= 1.5"

  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 3.0, < 4.0"
    }
  }
}
