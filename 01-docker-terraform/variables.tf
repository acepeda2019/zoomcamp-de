variable "bq_dataset_name" {
  description = "The name of the BigQuery dataset"
  type        = string
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "The name of the GCS bucket (leave default to use project-based name)"
  type        = string
  default     = ""
}

variable "gcs_storage_class" {
  description = "The storage class of the GCS bucket"
  type        = string
  default     = "STANDARD"
}

variable "location" {
  description = "The standard location of GCS resources"
  type        = string
  default     = "US"
}

variable "region" {
  description = "The standard region of GCS resources"
  type        = string
  default     = "us-central1"
}

variable "project" {
  description = "The standard project id of GCS resources"
  type        = string
  default     = "dtc-de-course-488600"
}

variable "credentials_file_path" {
  description = "The path to the credentials file"
  type        = string
  default     = "keys/my-creds.json"
}