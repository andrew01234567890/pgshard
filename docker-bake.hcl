// Keep the version variables in sync with postgres/versions.env.
variable "PG18_VERSION" { default = "18.6" }
variable "PG18_SHA256" { default = "555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f" }
variable "PG19_VERSION" { default = "19beta3" }
variable "PG19_SHA256" { default = "ea4ad8933121930a58f23c73dc99c26a4184faca26faefa77d15ce0fba7dfe2c" }
variable "PGBACKREST_VERSION" { default = "2.59.1" }
variable "REGISTRY" { default = "ghcr.io/andrew01234567890" }
variable "GIT_SHA" { default = "" }
variable "CI" { default = "" }

group "default" { targets = ["postgres"] }
group "postgres" { targets = ["postgres-18", "postgres-19"] }

function "tags" {
  params = [major, version]
  result = concat(
    ["${REGISTRY}/pgshard-postgres:${major}", "${REGISTRY}/pgshard-postgres:${version}"],
    GIT_SHA != "" ? ["${REGISTRY}/pgshard-postgres:${major}-${GIT_SHA}"] : [],
  )
}

target "_common" {
  context    = "postgres"
  dockerfile = "Dockerfile"
  cache-from = CI == "true" ? ["type=gha"] : []
  cache-to   = CI == "true" ? ["type=gha,mode=max"] : []
  args = { PGBACKREST_VERSION = PGBACKREST_VERSION }
}

target "postgres-18" {
  inherits = ["_common"]
  args = { PG_MAJOR = "18", PG_VERSION = PG18_VERSION, PG_SHA256 = PG18_SHA256 }
  tags = tags("18", PG18_VERSION)
  cache-from = CI == "true" ? ["type=gha,scope=postgres-18"] : []
  cache-to   = CI == "true" ? ["type=gha,scope=postgres-18,mode=max"] : []
}

target "postgres-19" {
  inherits = ["_common"]
  args = { PG_MAJOR = "19", PG_VERSION = PG19_VERSION, PG_SHA256 = PG19_SHA256 }
  tags = tags("19", PG19_VERSION)
  cache-from = CI == "true" ? ["type=gha,scope=postgres-19"] : []
  cache-to   = CI == "true" ? ["type=gha,scope=postgres-19,mode=max"] : []
}
