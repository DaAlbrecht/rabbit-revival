use std::sync::Arc;

use anyhow::Context;
use axum::{
    extract::Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::DateTime;
use deadpool_lapin::{PoolConfig, Runtime};
use replay::{fetch_messages, replay_header, replay_time_frame};
pub mod replay;

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
pub enum ReplayMode {
    TimeFrameReplay(TimeFrameReplay),
    HeaderReplay(HeaderReplay),
}

#[derive(serde::Deserialize, Debug)]
pub struct TimeFrameReplay {
    pub queue: String,
    pub from: DateTime<chrono::Utc>,
    pub to: DateTime<chrono::Utc>,
}

#[derive(serde::Deserialize, Debug)]
pub struct HeaderReplay {
    pub queue: String,
    pub header: AMQPHeader,
}

#[derive(serde::Deserialize, Debug)]
pub struct AMQPHeader {
    pub name: String,
    pub value: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct MessageQuery {
    pub queue: String,
    pub from: Option<DateTime<chrono::Utc>>,
    pub to: Option<DateTime<chrono::Utc>>,
}

pub struct AppState {
    pool: deadpool_lapin::Pool,
    message_options: MessageOptions,
    amqp_config: RabbitmqApiConfig,
}

#[derive(Clone)]
pub struct MessageOptions {
    pub transaction_header: Option<String>,
    pub enable_timestamp: bool,
}

#[derive(Debug)]
pub struct RabbitmqApiConfig {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: String,
}

//retrieves messages from the given queue.
//messages can be filtered by time frame, both from and to are optional
pub async fn get_messages(
    app_state: State<Arc<AppState>>,
    Query(message_query): Query<MessageQuery>,
) -> Result<impl IntoResponse, AppError> {
    let messages = fetch_messages(
        &app_state.pool.clone(),
        &app_state.amqp_config,
        &app_state.message_options,
        message_query,
    )
    .await?;
    Ok((StatusCode::OK, Json(messages)))
}

//replays messages based on the given replay mode, either by time frame or by header value
//a time stamp or transaction uuid can be added to the message upon replay
pub async fn replay(
    app_state: State<Arc<AppState>>,
    Json(replay_mode): Json<ReplayMode>,
) -> Result<impl IntoResponse, AppError> {
    let pool = app_state.pool.clone();
    let message_options = app_state.message_options.clone();
    let messages = match replay_mode {
        ReplayMode::TimeFrameReplay(timeframe) => {
            replay_time_frame(&pool, &app_state.amqp_config, timeframe).await?
        }
        ReplayMode::HeaderReplay(header) => {
            replay_header(&pool, &app_state.amqp_config, header).await?
        }
    };
    let replayed_messages = replay::publish_message(&pool, &message_options, messages).await?;
    Ok((StatusCode::CREATED, Json(replayed_messages)))
}

//checks if the service is up and running and can connect to rabbitmq can be established
pub async fn health(app_state: State<Arc<AppState>>) -> Result<impl IntoResponse, AppError> {
    let pool = app_state.pool.clone();
    let connection = pool
        .get()
        .await
        .context("Could not establish a connection to RabbitMQ")?;
    let channel = connection
        .create_channel()
        .await
        .context("Connection established, Could not create a channel")?;
    let status = channel.status().state();

    match status {
        lapin::ChannelState::Connected => Ok((StatusCode::OK, "OK")),
        _ => Err(AppError(anyhow::anyhow!("Chanel created, but not healthy"))),
    }
}

//read out the environment variables and configure the application state accordingly
pub async fn initialize_state() -> Arc<AppState> {
    let pool_size = std::env::var("AMQP_CONNECTION_POOL_SIZE")
        .unwrap_or("5".into())
        .parse::<usize>()
        .unwrap();

    let username = std::env::var("AMQP_USERNAME").unwrap_or("guest".into());
    let password = std::env::var("AMQP_PASSWORD").unwrap_or("guest".into());
    let host = std::env::var("AMQP_HOST").unwrap_or("localhost".into());
    let amqp_port = std::env::var("AMQP_PORT").unwrap_or("5672".into());
    let management_port = std::env::var("AMQP_MANAGEMENT_PORT").unwrap_or("15672".into());

    let transaction_header = std::env::var("AMQP_TRANSACTION_HEADER")
        .ok()
        .filter(|s| !s.is_empty());

    let enable_timestamp = std::env::var("AMQP_ENABLE_TIMESTAMP")
        .unwrap_or("true".into())
        .parse::<bool>()
        .unwrap();

    let publish_options = MessageOptions {
        transaction_header,
        enable_timestamp,
    };

    let amqp_config = RabbitmqApiConfig {
        username: username.clone(),
        password: password.clone(),
        host: host.clone(),
        port: management_port.clone(),
    };

    let cfg = deadpool_lapin::Config {
        url: Some(format!(
            "amqp://{}:{}@{}:{}/%2f",
            username, password, host, amqp_port
        )),
        pool: Some(PoolConfig::new(pool_size)),
        ..Default::default()
    };

    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    Arc::new(AppState {
        pool,
        message_options: publish_options,
        amqp_config,
    })
}
//https://github.com/tokio-rs/axum/blob/main/examples/anyhow-error-response/src/main.rs
// Make our own error that wraps `anyhow::Error`.
pub struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
