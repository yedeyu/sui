use prometheus::Registry;

use anyhow::Result;
use clap::*;
use log::info;
use sui_security_watchdog::scheduler::SchedulerService;
use sui_security_watchdog::SecurityWatchdogConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    let config = SecurityWatchdogConfig::parse();
    info!("Parsed config: {:#?}", config);
    let registry_service = mysten_metrics::start_prometheus_server(
        format!(
            "{}:{}",
            config.client_metric_host, config.client_metric_port
        )
        .parse()
        .unwrap(),
    );
    let registry: Registry = registry_service.default_registry();
    mysten_metrics::init_metrics(&registry);
    let service = SchedulerService::new(&config, &registry)?;
    service.start().await?;
    Ok(())
}
