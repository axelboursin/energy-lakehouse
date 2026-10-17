variable "environments" {
  description = "Environments to create, each mapped to its own catalog."
  type        = set(string)
  default     = ["dev", "staging", "prod"]
}

variable "catalog_prefix" {
  type    = string
  default = "energy"
}

variable "storage_root" {
  description = "Optional managed storage location (s3://, abfss://). Empty uses the metastore default."
  type        = string
  default     = ""
}

variable "deployer_application_id" {
  description = "Application ID of the service principal used by CI to deploy and run the bundle."
  type        = string
}

variable "engineers_group" {
  description = "Account-level group of data engineers (synced from the IdP)."
  type        = string
  default     = "data-engineers"
}

variable "analysts_group" {
  description = "Account-level group of analysts, read-only on gold."
  type        = string
  default     = "data-analysts"
}
