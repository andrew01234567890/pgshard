//! Runtime foundations for the `PostgreSQL` instance agent.

pub mod boottime;
pub mod catalog_activation;
pub mod catalog_activation_consumer;
pub mod catalog_activation_runtime;
pub mod catalog_activation_static_inputs;
pub mod catalog_activation_tls;
pub(crate) mod catalog_materialization_program;
pub mod catalog_materialization_stage;
pub(crate) mod catalog_materializer;
pub mod config;
pub mod coordination;
pub mod domain;
pub mod genesis_authority;
pub mod http;
pub mod postgres;
pub mod postgres_fence;
pub mod postgres_generation;
pub(crate) mod postgres_recovery;
pub(crate) mod postgres_replication;
pub(crate) mod serving_input_compiler;
pub mod telemetry;
pub mod writable;
