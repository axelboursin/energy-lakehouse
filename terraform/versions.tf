terraform {
  required_version = ">= 1.10"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.90"
    }
  }

  # State lives in HCP Terraform (free tier). Organization and workspace come
  # from TF_CLOUD_ORGANIZATION / TF_WORKSPACE. The workspace uses *local*
  # execution: plans run in GitHub Actions, where Databricks OIDC credentials are.
  cloud {}
}

provider "databricks" {}
