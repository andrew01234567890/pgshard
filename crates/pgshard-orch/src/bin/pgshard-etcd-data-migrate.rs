//! One-shot init-container entry point for transitional etcd data migration.

use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pgshard_orch::etcd_data::prepare_etcd_data_dir(Path::new("/var/lib/etcd"))?;
    Ok(())
}
