variable "PGSHARD_BUILD_VERSION" {
  default = ""
  validation {
    condition     = PGSHARD_BUILD_VERSION != ""
    error_message = "PGSHARD_BUILD_VERSION must identify this build."
  }
}

variable "PGSHARD_GIT_SHA" {
  default = ""
  validation {
    condition = (
      can(regex("^[0-9a-fA-F]{40}$", PGSHARD_GIT_SHA)) &&
      PGSHARD_GIT_SHA != "0000000000000000000000000000000000000000"
    )
    error_message = "PGSHARD_GIT_SHA must be a non-zero 40-character Git object ID."
  }
}

variable "PGSHARD_IMAGE_OUTPUT" {
  default = "artifacts/images"
}

variable "PGSHARD_IMAGE_TAG" {
  default = "dev"
}

group "ci" {
  targets = ["agent", "operator", "orchestrator", "pooler", "postgres-agent"]
}

# Empty by default so a local build is unchanged and never reads or writes a
# cache it did not ask for. CI points this at a directory it restores between
# runs: every image here inherits one Rust compile, and without a cache each
# job repeats that compile from scratch inside Docker.
variable "PGSHARD_IMAGE_CACHE" {
  default = ""
}

target "rust-runtime" {
  context    = "."
  dockerfile = "deploy/images/rust.Dockerfile"
  platforms  = ["linux/amd64"]
  cache-from = PGSHARD_IMAGE_CACHE == "" ? [] : ["type=local,src=${PGSHARD_IMAGE_CACHE}/rust"]
  cache-to   = PGSHARD_IMAGE_CACHE == "" ? [] : ["type=local,dest=${PGSHARD_IMAGE_CACHE}/rust,mode=max"]
  args = {
    PGSHARD_BUILD_VERSION = PGSHARD_BUILD_VERSION
    PGSHARD_GIT_SHA       = PGSHARD_GIT_SHA
  }
}

target "agent" {
  inherits = ["rust-runtime"]
  target   = "agent"
  tags     = ["pgshard/agent:${PGSHARD_IMAGE_TAG}"]
  output   = ["type=docker,dest=${PGSHARD_IMAGE_OUTPUT}/pgshard-agent.tar"]
}

target "orchestrator" {
  inherits = ["rust-runtime"]
  target   = "orchestrator"
  tags     = ["pgshard/orchestrator:${PGSHARD_IMAGE_TAG}"]
  output   = ["type=docker,dest=${PGSHARD_IMAGE_OUTPUT}/pgshard-orchestrator.tar"]
}

target "pooler" {
  inherits = ["rust-runtime"]
  target   = "pooler"
  tags     = ["pgshard/pooler:${PGSHARD_IMAGE_TAG}"]
  output   = ["type=docker,dest=${PGSHARD_IMAGE_OUTPUT}/pgshard-pooler.tar"]
}

target "postgres-agent" {
  inherits = ["rust-runtime"]
  target   = "postgres-agent"
  tags     = ["pgshard/postgres-agent:${PGSHARD_IMAGE_TAG}"]
  output   = ["type=docker,dest=${PGSHARD_IMAGE_OUTPUT}/pgshard-postgres-agent.tar"]
}

target "operator" {
  context    = "."
  dockerfile = "deploy/images/operator.Dockerfile"
  platforms  = ["linux/amd64"]
  cache-from = PGSHARD_IMAGE_CACHE == "" ? [] : ["type=local,src=${PGSHARD_IMAGE_CACHE}/operator"]
  cache-to   = PGSHARD_IMAGE_CACHE == "" ? [] : ["type=local,dest=${PGSHARD_IMAGE_CACHE}/operator,mode=max"]
  tags       = ["pgshard/operator:${PGSHARD_IMAGE_TAG}"]
  args = {
    PGSHARD_BUILD_VERSION = PGSHARD_BUILD_VERSION
    PGSHARD_GIT_SHA       = PGSHARD_GIT_SHA
  }
  output = ["type=docker,dest=${PGSHARD_IMAGE_OUTPUT}/pgshard-operator.tar"]
}
