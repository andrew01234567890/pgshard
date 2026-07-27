//! Runtime foundations for the `PostgreSQL` instance agent.

pub mod boottime;
pub mod catalog_activation;
pub mod catalog_activation_consumer;
pub mod catalog_activation_runtime;
pub mod catalog_activation_static_inputs;
pub mod catalog_activation_tls;
#[allow(
    dead_code,
    reason = "sealed identity checks for the next catalog materialization stage; the \
              compatibility bootstrap shell remains the producer until it is retired"
)]
pub(crate) mod catalog_identity;
pub(crate) mod catalog_materialization_program;
pub mod catalog_materialization_stage;
pub(crate) mod catalog_materializer;
#[allow(
    dead_code,
    reason = "sealed material checks for the next catalog materialization stage; the \
              compatibility bootstrap shell remains the producer until it is retired"
)]
pub(crate) mod catalog_secret_material;
pub mod config;
pub mod coordination;
pub mod domain;
pub mod genesis_authority;
pub mod http;
pub(crate) mod kube_transport;
pub mod postgres;
pub mod postgres_fence;
pub mod postgres_generation;
pub(crate) mod postgres_recovery;
pub(crate) mod postgres_replication;
pub mod serving_activation;
pub mod serving_activation_consumer;
pub(crate) mod serving_input_compiler;
pub mod telemetry;
pub mod writable;
