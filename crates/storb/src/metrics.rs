use opentelemetry_otlp::WithExportConfig;
use tracing_opentelemetry::layer;
use tracing_subscriber::Registry;

fn init_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .with_endpoint("http://localhost:4318")
        .install_simple()?;

    let otel_layer = layer().with_tracer(tracer);
    Registry::default().with(otel_layer).init();
    Ok(())
}
