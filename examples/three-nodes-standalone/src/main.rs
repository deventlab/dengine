use d_engine::StandaloneEngine;
use std::env;
use std::error::Error;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio::sync::watch;
use tokio_metrics::RuntimeMonitor;
use tracing::error;
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let log_dir = env::var("LOG_DIR")
        .map_err(|_| "LOG_DIR environment variable not set")
        .expect("Set log dir successfully.");

    let config_path = env::var("CONFIG_PATH")
        .map(|path| format!("{path}.toml"))
        .unwrap_or_else(|_| "d-engine.toml".to_string());

    let data_dir = env::var("DB_PATH")
        .map_err(|_| "DB_PATH environment variable not set")
        .expect("Set data dir successfully.");

    let metrics_port: u16 = env::var("METRICS_PORT")
        .map(|v| v.parse::<u16>().expect("METRICS_PORT must be a valid port"))
        .unwrap_or(9000); // default 9000 if not set

    let _log_guard = if env::var("TOKIO_CONSOLE").is_ok() {
        let tokio_console_port: u16 = env::var("TOKIO_CONSOLE_PORT")
            .map(|v| v.parse::<u16>().expect("TOKIO_CONSOLE_PORT must be a valid port"))
            .unwrap_or(6669);

        println!("Tokio Console port: {tokio_console_port}");

        console_subscriber::Builder::default()
            .server_addr(([127, 0, 0, 1], tokio_console_port))
            .init();

        // Your application code here
        println!("Application started with Tokio Console monitoring");
        None
    } else {
        // Initialize the log system
        Some(init_observability(log_dir).expect("Failed to initialize logging"))
    };

    // Initializing Shutdown Signal
    let (graceful_tx, graceful_rx) = watch::channel(());

    // Start the server (wait for its initialization to complete)
    let server_handler = tokio::spawn(start_dengine_server(
        data_dir,
        config_path,
        graceful_rx.clone(),
    ));

    // Wait for the server to initialize (adjust the waiting time according to the actual logic)
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Start Prometheus metrics server ---
    let metrics_handle = tokio::spawn(start_metrics_server(metrics_port));

    // Monitor shutdown signals
    let shutdown_handler = tokio::spawn(graceful_shutdown(graceful_tx));

    // Wait for all tasks to complete (or error)
    if env::var("TOKIO_CONSOLE").is_ok() {
        // Initialize Tokio metrics monitoring
        let handle = tokio::runtime::Handle::current();
        let runtime_monitor = RuntimeMonitor::new(&handle);
        // Start the Tokio metrics collection task
        let tokio_metrics_handle =
            tokio::spawn(collect_tokio_metrics(runtime_monitor, graceful_rx.clone()));
        let (_server_result, _metrics_result, _shutdown_result, _tokio_metrics_result) = tokio::join!(
            server_handler,
            metrics_handle,
            shutdown_handler,
            tokio_metrics_handle
        );
    } else {
        let (_server_result, _metrics_result, _shutdown_result) =
            tokio::join!(server_handler, metrics_handle, shutdown_handler);
    }
}

async fn start_dengine_server(
    data_dir: String,
    config_path: String,
    graceful_rx: watch::Receiver<()>,
) {
    println!("╔════════════════════════════════════════╗");
    println!("║  d-engine Node Starting...             ║");
    println!("║  Config: {config_path:<28} ║");
    println!("╚════════════════════════════════════════╝");

    // StandaloneEngine with explicit data_dir and config path
    // Blocks until shutdown signal received
    if let Err(e) = StandaloneEngine::run_with(&data_dir, &config_path, graceful_rx).await {
        error!("Server stopped with error: {:?}", e);
    } else {
        info!("Server stopped gracefully");
    }

    println!("Exiting program.");
}

async fn graceful_shutdown(graceful_tx: watch::Sender<()>) {
    info!("Monitoring shutdown signal ...");
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    tokio::select! {
        _ = sigint.recv() => {
            info!("SIGINT detected.");
        },
        _ = sigterm.recv() => {
            info!("SIGTERM detected.");
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C detected.");
        }
    }

    graceful_tx.send(()).unwrap();

    info!("Shutdown completed");
}

pub fn init_observability(log_dir: String) -> Result<WorkerGuard, Box<dyn Error + Send>> {
    let log_file = open_file_for_append(Path::new(&log_dir).join("d.log")).unwrap();

    let (non_blocking, guard) = tracing_appender::non_blocking(log_file);
    let base_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry().with(base_subscriber).init();
    Ok(guard)
}

async fn start_metrics_server(port: u16) {
    // Start Prometheus exporter with dynamic port
    println!("Metrics server will start at http://0.0.0.0:{port}/metrics",);

    // Exponential buckets: start 0.1ms, factor 2, 15 steps → covers 0.1ms to ~1638ms.
    // Matches actual d-engine distribution: normal fsyncs ~0.2ms, outliers up to ~1.7s.
    // Linear or default buckets (max 10.0) lose all sub-10ms detail and miss outliers.
    const MS_BUCKETS: &[f64] = &[
        0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6, 51.2, 102.4, 204.8, 409.6, 819.2, 1638.4,
    ];
    const BATCH_BUCKETS: &[f64] = &[1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0];

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_ms".to_string()),
            MS_BUCKETS,
        )
        .expect("failed to configure _ms buckets")
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full("core.raft.fsync.batch_entries".to_string()),
            BATCH_BUCKETS,
        )
        .expect("failed to configure batch_entries buckets")
        .install()
        .expect("failed to start Prometheus metrics exporter");
}

// Tokio metrics collection function
async fn collect_tokio_metrics(
    monitor: RuntimeMonitor,
    mut graceful_rx: watch::Receiver<()>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut intervals = monitor.intervals();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Some(metrics) = intervals.next() {
                    metrics::gauge!("tokio.runtime.workers_count").set(metrics.workers_count as f64);
                    metrics::counter!("tokio.runtime.park_total").absolute(metrics.total_park_count);
                    metrics::gauge!("tokio.runtime.park_max").set(metrics.max_park_count as f64);
                    metrics::gauge!("tokio.runtime.park_min").set(metrics.min_park_count as f64);
                    metrics::histogram!("tokio.runtime.busy_duration_ns")
                        .record(metrics.total_busy_duration.as_nanos() as f64);
                }
            }
            _ = graceful_rx.changed() => {
                info!("Shutting down Tokio metrics collection");
                break;
            }
        }
    }
}

fn open_file_for_append(path: PathBuf) -> Result<std::fs::File, Box<dyn Error>> {
    // Create parent directories if they don't exist
    if let Some(parent) = path.parent()
        && parent != Path::new("")
    {
        std::fs::create_dir_all(parent)?;
    }

    let log_file = OpenOptions::new().append(true).create(true).open(&path)?;

    Ok(log_file)
}
