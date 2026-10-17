output "catalogs" {
  value = { for env, catalog in databricks_catalog.env : env => catalog.name }
}

output "landing_volumes" {
  value = { for env, volume in databricks_volume.landing : env => "/Volumes/${volume.catalog_name}/${volume.schema_name}/${volume.name}" }
}
