terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.21.0"
    }
  }
}

provider "google" {
  # Configuration options
  project     = var.project
  region      = var.region
  credentials = file(var.credentials_file_path)
}

locals {
  gcs_bucket_name = var.gcs_bucket_name != "" ? var.gcs_bucket_name : "${var.project}-demo-bucket"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = local.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id = var.bq_dataset_name
}