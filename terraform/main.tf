# Unity Catalog layout: one catalog per environment, medallion schemas inside.
#
#   energy_<env>
#   ├── bronze   raw tables + volumes (landing files, stream checkpoints)
#   ├── silver   typed, validated, deduplicated + quarantine
#   └── gold     business aggregates
#
# Access model:
#   engineers  full control on dev, read-only on staging/prod
#   deployer   the only principal writing to staging/prod (via CI)
#   analysts   SELECT on prod gold only

locals {
  layers = ["bronze", "silver", "gold"]

  schemas = {
    for pair in setproduct(var.environments, local.layers) :
    "${pair[0]}.${pair[1]}" => { env = pair[0], layer = pair[1] }
  }

  writer_privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE", "READ_VOLUME", "WRITE_VOLUME"]
  reader_privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT", "READ_VOLUME"]
}

resource "databricks_catalog" "env" {
  for_each = var.environments

  name           = "${var.catalog_prefix}_${each.key}"
  comment        = "Energy lakehouse - ${each.key}. Managed by Terraform."
  storage_root   = var.storage_root != "" ? "${var.storage_root}/${each.key}" : null
  isolation_mode = "ISOLATED"

  properties = {
    environment = each.key
    owner_team  = "data-platform"
  }
}

resource "databricks_schema" "layer" {
  for_each = local.schemas

  catalog_name = databricks_catalog.env[each.value.env].name
  name         = each.value.layer
  comment      = "${each.value.layer} layer"
}

resource "databricks_volume" "landing" {
  for_each = var.environments

  catalog_name = databricks_catalog.env[each.key].name
  schema_name  = databricks_schema.layer["${each.key}.bronze"].name
  name         = "landing"
  volume_type  = "MANAGED"
  comment      = "Raw files exactly as received from the source APIs"
}

resource "databricks_volume" "checkpoints" {
  for_each = var.environments

  catalog_name = databricks_catalog.env[each.key].name
  schema_name  = databricks_schema.layer["${each.key}.bronze"].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Auto Loader schemas and Structured Streaming checkpoints"
}

resource "databricks_grants" "catalog" {
  for_each = var.environments

  catalog = databricks_catalog.env[each.key].name

  grant {
    principal  = var.engineers_group
    privileges = each.key == "dev" ? ["ALL_PRIVILEGES"] : local.reader_privileges
  }

  grant {
    principal  = var.deployer_application_id
    privileges = local.writer_privileges
  }

  # Analysts need USE_CATALOG on prod to reach the gold schema, nothing more.
  # Declared here because databricks_grants is authoritative for the catalog.
  dynamic "grant" {
    for_each = each.key == "prod" ? [var.analysts_group] : []
    content {
      principal  = grant.value
      privileges = ["USE_CATALOG"]
    }
  }
}

resource "databricks_grants" "prod_gold" {
  schema = databricks_schema.layer["prod.gold"].id

  grant {
    principal  = var.analysts_group
    privileges = ["USE_SCHEMA", "SELECT"]
  }
}
