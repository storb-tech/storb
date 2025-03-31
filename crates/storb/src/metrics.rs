use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use sysinfo::{Disks, Networks, System};
use tracing::{debug, trace};

use crate::constants::OTEL_EXPORTER_OTLP_ENDPOINT;

const SYSTEM_CPU_LOAD: &str = "system.cpu.load";
const SYSTEM_MEMORY_USAGE: &str = "system.memory.usage";
const SYSTEM_NETWORK_RECEIVED: &str = "system.network.received";
const SYSTEM_NETWORK_TRANSMITTED: &str = "system.network.transmitted";
const SYSTEM_DISK_TOTAL: &str = "system.disk.total";
const SYSTEM_DISK_USED: &str = "system.disk.used";

static SYSTEM_INSTANCE: OnceLock<Mutex<System>> = OnceLock::new();
static NETWORK_INSTANCE: OnceLock<Mutex<Networks>> = OnceLock::new();
static DISK_INSTANCE: OnceLock<Mutex<Disks>> = OnceLock::new();

fn get_system_instance() -> &'static Mutex<System> {
    SYSTEM_INSTANCE.get_or_init(|| Mutex::new(System::new_all()))
}

fn get_network_instance() -> &'static Mutex<Networks> {
    NETWORK_INSTANCE.get_or_init(|| Mutex::new(Networks::new_with_refreshed_list()))
}

fn get_disk_instance() -> &'static Mutex<Disks> {
    DISK_INSTANCE.get_or_init(|| Mutex::new(Disks::new_with_refreshed_list()))
}

/// Refresh the global sysinfo instance so that it accumulates non-zero values.
fn refresh_system() {
    if let Ok(mut system) = get_system_instance().lock() {
        // Refresh all subsystems.
        system.refresh_all();
        // Optionally, refresh CPU separately if needed.
        system.refresh_memory();
    }
    if let Ok(mut network) = get_network_instance().lock() {
        network.refresh(true);
    }
    if let Ok(mut disks) = get_disk_instance().lock() {
        disks.refresh(true);
    }
}

#[allow(dead_code)]
pub struct SystemMetrics {
    cpu: opentelemetry::metrics::ObservableGauge<f64>,
    memory: opentelemetry::metrics::ObservableGauge<u64>,
    network_received: opentelemetry::metrics::ObservableGauge<f64>,
    network_transmitted: opentelemetry::metrics::ObservableGauge<f64>,
    disk_total: opentelemetry::metrics::ObservableGauge<f64>,
    disk_used: opentelemetry::metrics::ObservableGauge<f64>,
}

pub fn init_metrics() -> (SdkMeterProvider, SystemMetrics) {
    let exporter = MetricExporter::builder()
        .with_http()
        .with_endpoint(OTEL_EXPORTER_OTLP_ENDPOINT)
        .build()
        .expect("Failed to create metric exporter");

    let reader = PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(10))
        .build();

    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    let meter = provider.meter("node-system-metrics");

    // CPU load gauge.
    let cpu = meter
        .f64_observable_gauge(SYSTEM_CPU_LOAD)
        .with_description("CPU load percentage")
        .with_unit("%")
        .with_callback(|observer| {
            refresh_system();
            if let Ok(system) = get_system_instance().lock() {
                let cpu_usage = system.global_cpu_usage() as f64;
                observer.observe(cpu_usage, &[]);
                trace!("Reported CPU usage: {}", cpu_usage);
            }
        })
        .build();

    // Memory usage gauge.
    let memory = meter
        .u64_observable_gauge(SYSTEM_MEMORY_USAGE) // Use the constant here
        .with_description("Used memory in bytes")
        .with_unit("bytes")
        .with_callback(|observer| {
            refresh_system();
            if let Ok(system) = get_system_instance().lock() {
                let used_memory = system.used_memory();
                observer.observe(used_memory, &[]);
                trace!("Reported memory usage: {} bytes", used_memory);
            }
        })
        .build();

    // Network received gauge.
    let network_received = meter
        .f64_observable_gauge(SYSTEM_NETWORK_RECEIVED)
        .with_description("Total network received bytes")
        .with_unit("bytes")
        .with_callback(|observer| {
            refresh_system();
            if let Ok(network) = get_network_instance().lock() {
                let received_bytes: u64 = network
                    .iter()
                    .map(|(_iface, data)| data.total_received())
                    .sum();
                observer.observe(received_bytes as f64, &[]);
                trace!("Reported network received: {} bytes", received_bytes);
            }
        })
        .build();

    // Network transmitted gauge.
    let network_transmitted = meter
        .f64_observable_gauge(SYSTEM_NETWORK_TRANSMITTED)
        .with_description("Total network transmitted bytes")
        .with_unit("bytes")
        .with_callback(|observer| {
            refresh_system();
            if let Ok(network) = get_network_instance().lock() {
                let transmitted_bytes: u64 = network
                    .iter()
                    .map(|(_iface, data)| data.total_transmitted())
                    .sum();
                observer.observe(transmitted_bytes as f64, &[]);
                trace!("Reported network transmitted: {} bytes", transmitted_bytes);
            }
        })
        .build();

    // Disk total gauge
    let disk_total = meter
        .f64_observable_gauge(SYSTEM_DISK_TOTAL)
        .with_description("Total disk space in bytes")
        .with_unit("bytes")
        .with_callback(|observer| {
            refresh_system();
            if let Ok(disks) = get_disk_instance().lock() {
                let total_space: u64 = disks.iter().map(|disk| disk.total_space()).sum();
                observer.observe(total_space as f64, &[]);
                trace!("Reported disk total: {} bytes", total_space);
            }
        })
        .build();

    // Disk used gauge
    let disk_used = meter
        .f64_observable_gauge(SYSTEM_DISK_USED)
        .with_description("Used disk space in bytes")
        .with_unit("bytes")
        .with_callback(|observer| {
            refresh_system();
            if let Ok(disks) = get_disk_instance().lock() {
                let used_space: u64 = disks
                    .iter()
                    .map(|disk| disk.total_space() - disk.available_space())
                    .sum();
                observer.observe(used_space as f64, &[]);
                trace!("Reported disk used: {} bytes", used_space);
            }
        })
        .build();

    (
        provider,
        SystemMetrics {
            cpu,
            memory,
            network_received,
            network_transmitted,
            disk_total,
            disk_used,
        },
    )
}

pub fn setup_metrics() -> SystemMetrics {
    let (provider, metrics) = init_metrics();
    std::thread::spawn(move || {
        let _keep_alive = provider;
        loop {
            std::thread::sleep(Duration::from_secs(3600));
        }
    });

    debug!("Metrics system initialized");
    metrics
}
