use ccp_nimbus::{Nimbus, NimbusConfig};
use clap::Parser;
use tracing::info;

fn main() {
    tracing_subscriber::fmt::init();
    let cfg = NimbusConfig::parse();
    let ipc = cfg.ipc.clone();
    let nimbus: Nimbus = cfg.into();

    info!(
        algorithm = "NIMBUS",
        ipc = ?&ipc,
        "starting CCP"
    );

    portus::start!(ipc.as_str(), nimbus).unwrap()
}
