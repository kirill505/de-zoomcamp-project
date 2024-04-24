  terraform {
    required_providers {
      google = {
        source  = "hashicorp/google"
        version = "5.12.0"
      }
    }
  }

  provider "google" {
    # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
    #  credentials = 
    project = var.project_id
    region  = var.region
  }

  resource "google_storage_bucket" "data-lake-bucket" {
    name          = var.gcs_bucket_name
    location      = var.location
    force_destroy = true

    # Optional, but recommended settings:
    storage_class = var.gcs_storage_class
    # uniform_bucket_level_access = true

    lifecycle_rule {
      condition {
        age = 1
      }
      action {
        type = "AbortIncompleteMultipartUpload"
      }
    }

  }

  resource "google_bigquery_dataset" "data-warehouse" {
    dataset_id = var.bq_dataset_name
    location   = var.location
  }