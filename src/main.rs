use std::{net::SocketAddr, time::Instant};

use axum::{
    extract::MatchedPath,
    http::Request,
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use rabbit_revival::{get_messages, health, initialize_state, replay};
use sysinfo::{CpuExt, System, SystemExt};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "rabbit_revival=debug,tower_http=trace,axum::rejection=trace".into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let enable_metrics = std::env::var("ENABLE_METRICS").unwrap_or("false".to_string());

    if enable_metrics == "true" {
        tracing::info!("metrics enabled");
        let (_main_server, _metrics_server) =
            tokio::join!(start_main_server(), start_metrics_server());
    } else {
        tracing::info!("metrics disabled");
        start_main_server().await;
    }
}

fn metrics_app() -> Router {
    let recorder_handle = setup_metrics_recorder();
    Router::new().route(
        "/metrics",
        get(move || std::future::ready(recorder_handle.render())),
    )
}

fn setup_metrics_recorder() -> PrometheusHandle {
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("http_requests_duration_seconds".to_string()),
            EXPONENTIAL_SECONDS,
        )
        .unwrap()
        .install_recorder()
        .unwrap()
}

async fn main_app() -> Router {
    Router::new()
        .route("/list", get(get_messages))
        .route("/replay", post(replay))
        .route("/health", get(health))
        .layer(TraceLayer::new_for_http())
        .with_state(initialize_state().await)
        .route_layer(middleware::from_fn(track_metrics))
}

async fn start_main_server() {
    let app = main_app().await;

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn start_metrics_server() {
    let app = metrics_app();

    // NOTE: expose metrics endpoint on a different port
    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    tracing::debug!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn track_metrics<B>(req: Request<B>, next: Next<B>) -> impl IntoResponse {
    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let system = System::new_all();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let cpu_usage = system.global_cpu_info().cpu_usage();
    let memory_usage = system.used_memory();

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", status),
    ];

    metrics::gauge!("cpu_usage_percentage", f64::from(cpu_usage), &labels);
    metrics::gauge!("memory_usage_bytes", memory_usage as f64, &labels);

    metrics::increment_counter!("http_requests_total", &labels);
    metrics::histogram!("http_requests_duration_seconds", latency, &labels);

    response
}
