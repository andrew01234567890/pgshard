// Keep the version variables in sync with postgres/versions.env.
variable "PG18_VERSION" { default = "18.6" }
variable "PG18_SHA256" { default = "555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f" }
variable "PG19_VERSION" { default = "19beta3" }
variable "PG19_SHA256" { default = "ea4ad8933121930a58f23c73dc99c26a4184faca26faefa77d15ce0fba7dfe2c" }
variable "PGBACKREST_VERSION" { default = "2.59.1" }
variable "PGBACKREST_SHA256" { default = "ca1e75c7490989a2fb39b8266c0f3c518dd3c873ddc0c3a346ca9b5dccc16455" }
variable "REGISTRY" { default = "ghcr.io/andrew01234567890" }
variable "GIT_SHA" { default = "" }
variable "CI" { default = "" }

// Stamped into the control-plane binaries so a deployed image can say what
// it is. Dockerfile.control already threads these into ldflags; nothing was
// passing them, so every published binary reported "dev (none, unknown)".
variable "VERSION" { default = "" }
variable "BUILD_DATE" { default = "" }

// RELEASE_TAG is the version tag a release is cut from, "v1.2.3". Set, it
// adds an immutable tag to every image so a consumer has something to pin
// that a later build cannot move; empty, nothing changes and the moving
// tags are all a branch push produces.
variable "RELEASE_TAG" { default = "" }

group "default" { targets = ["postgres"] }
group "postgres" { targets = ["postgres-18", "postgres-19"] }

// The images docs/guide/getting-started.md tells a reader to deploy. They
// were built only inside the e2e workflow with docker build, so the tags
// the operator defaults to -- pgshard-operator:latest and the router and
// admin images it renders -- had never been published at all, and a first
// deployment stopped at ImagePullBackOff before anything else happened.
group "control" { targets = ["operator", "router", "admin", "controller"] }

// controlTags names a control-plane image. There is no upstream version to
// carry, so the moving tag and the commit are what a deployment can pin to.
function "controlTags" {
  params = [name]
  result = concat(
    ["${REGISTRY}/pgshard-${name}:latest"],
    GIT_SHA != "" ? ["${REGISTRY}/pgshard-${name}:${GIT_SHA}"] : [],
    RELEASE_TAG != "" ? ["${REGISTRY}/pgshard-${name}:${RELEASE_TAG}"] : [],
  )
}

function "tags" {
  params = [major, version]
  result = concat(
    ["${REGISTRY}/pgshard-postgres:${major}", "${REGISTRY}/pgshard-postgres:${version}"],
    GIT_SHA != "" ? ["${REGISTRY}/pgshard-postgres:${major}-${GIT_SHA}"] : [],
    RELEASE_TAG != "" ? ["${REGISTRY}/pgshard-postgres:${major}-${RELEASE_TAG}"] : [],
  )
}

target "_common" {
  context    = "."
  dockerfile = "postgres/Dockerfile"
  cache-from = CI == "true" ? ["type=gha"] : []
  cache-to   = CI == "true" ? ["type=gha,mode=max"] : []
  args = {
    PGBACKREST_VERSION = PGBACKREST_VERSION, PGBACKREST_SHA256 = PGBACKREST_SHA256,
    // The agent and the pooler ship inside this image and are as much a
    // deployed binary as the control plane's are. Built without these they
    // answered --version with "dev (none, unknown)", which is no answer at
    // all when the question is which commit is running on a member.
    VERSION = VERSION != "" ? VERSION : "dev",
    COMMIT  = GIT_SHA != "" ? GIT_SHA : "none",
    DATE    = BUILD_DATE != "" ? BUILD_DATE : "unknown",
  }
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

target "_control" {
  context    = "."
  dockerfile = "Dockerfile.control"
  args = {
    VERSION = VERSION != "" ? VERSION : "dev",
    COMMIT  = GIT_SHA != "" ? GIT_SHA : "none",
    DATE    = BUILD_DATE != "" ? BUILD_DATE : "unknown",
  }
  cache-from = CI == "true" ? ["type=gha"] : []
  cache-to   = CI == "true" ? ["type=gha,mode=max"] : []
}

target "operator" {
  inherits = ["_control"]
  args     = { CMD = "pgshard-operator" }
  tags     = controlTags("operator")
}

target "controller" {
  inherits = ["_control"]
  args     = { CMD = "pgshard-controller" }
  tags     = controlTags("controller")
}

target "admin" {
  inherits = ["_control"]
  args     = { CMD = "pgshard-admin" }
  tags     = controlTags("admin")
}

target "router" {
  inherits   = ["_control"]
  dockerfile = "Dockerfile.router"
  tags       = controlTags("router")
}
