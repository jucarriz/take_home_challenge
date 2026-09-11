# Terraform — MinIO buckets

Declarative management of the bronze / silver / gold buckets in the
local MinIO instance. Runs against the MinIO that
[`docker-compose.yml`](../docker-compose.yml) brings up on
`localhost:9000`.

`docker-compose.yml` also has a `minio-init` container that creates
the same buckets via a shell one-liner (`mc mb --ignore-existing`).
Both paths are idempotent — `minio-init` keeps zero-config UX for
reviewers who don't have Terraform installed; the code in this folder
demonstrates the IaC discipline for those who prefer declarative
infra.

## Prerequisites

- The MinIO container must be running:
  ```bash
  docker compose up minio -d
  ```
- One of:
  - **Terraform CLI 1.5+** installed on the host, or
  - Docker (to run Terraform in a throwaway container).

## Usage — with Terraform installed locally

```bash
cd terraform
terraform init
terraform plan          # shows the 3 buckets to be created
terraform apply         # -auto-approve if you trust yourself
terraform destroy       # remove the buckets (irreversible)
```

## Usage — via Docker (no local install)

```bash
# From the repo root
docker run --rm -it \
    -v "$(pwd)/terraform:/workspace" \
    -w /workspace \
    --network aurelia_default \
    -e TF_VAR_minio_endpoint=minio:9000 \
    hashicorp/terraform:1.9 init

docker run --rm -it \
    -v "$(pwd)/terraform:/workspace" \
    -w /workspace \
    --network aurelia_default \
    -e TF_VAR_minio_endpoint=minio:9000 \
    hashicorp/terraform:1.9 apply
```

The `TF_VAR_minio_endpoint=minio:9000` override uses the Docker
network DNS name instead of `localhost`, because from another
container `localhost` points at the Terraform container itself.

## What gets created

Three private S3 buckets named `bronze`, `silver`, `gold`. The
pipeline (Airflow / Spark) reads and writes them via the S3A protocol
using the same MinIO credentials.

## State

Local file backend: `terraform.tfstate` lives in this folder and is
**gitignored**. No remote backend (S3, Terraform Cloud, etc.) is
configured — this is a local development stack, not a shared
environment.

## Adjusting

- **Different credentials or endpoint**: copy `terraform.tfvars.example`
  to `terraform.tfvars` and edit.
- **Add a fourth bucket**: add its name to `var.bucket_names` (in
  `variables.tf` or your `.tfvars`) — no other change needed thanks
  to `for_each`.
