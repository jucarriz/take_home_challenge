# One resource per medallion layer bucket. Using `for_each` keeps the
# three declarations identical — adding a fourth bucket is a one-line
# change to `var.bucket_names`.

resource "minio_s3_bucket" "layer" {
  for_each = var.bucket_names

  bucket = each.value
  acl    = "private"
}
