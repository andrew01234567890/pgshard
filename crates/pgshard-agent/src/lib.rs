//! Runtime foundations for the `PostgreSQL` instance agent.

pub mod boottime;
pub mod catalog_activation;
pub mod catalog_activation_consumer;
pub mod config;
pub mod coordination;
pub mod domain;
pub mod http;
pub mod postgres;
pub mod postgres_fence;
pub mod postgres_generation;
pub(crate) mod postgres_recovery;
pub(crate) mod postgres_replication;
pub mod telemetry;
pub mod writable;
