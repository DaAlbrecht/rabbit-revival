use chrono::{TimeZone, Utc};
use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use lapin::types::AMQPValue::{self};
use lapin::{
    options::{BasicConsumeOptions, BasicQosOptions},
    types::{FieldTable, ShortString},
};

use anyhow::{anyhow, Result};
use futures_lite::{stream, StreamExt};
use serde::Serialize;

use crate::{HeaderReplay, MessageOptions, MessageQuery, RabbitmqApiConfig, TimeFrameReplay};

#[derive(Serialize, Debug)]
pub struct Message {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    pub transaction: Option<TransactionHeader>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub data: String,
}

#[derive(Serialize, Debug)]
pub struct TransactionHeader {
    pub name: String,
    pub value: String,
}

impl TransactionHeader {
    pub fn from_fieldtable(field_table: FieldTable, header_name: &str) -> Result<Self> {
        let transaction_id = match field_table.inner().get(header_name) {
            Some(AMQPValue::LongString(transaction_id)) => transaction_id.to_string(),
            _ => return Err(anyhow!("Transaction header {} not found", header_name)),
        };
        Ok(Self {
            name: header_name.to_string(),
            value: transaction_id,
        })
    }
}

pub async fn replay_time_frame(
    pool: &deadpool_lapin::Pool,
    rabbitmq_api_config: &RabbitmqApiConfig,
    time_frame: TimeFrameReplay,
) -> Result<Vec<Delivery>> {
    let message_count =
        match get_queue_message_count(rabbitmq_api_config, &time_frame.queue).await? {
            Some(message_count) => message_count,
            None => return Err(anyhow!("Queue not found or empty")),
        };

    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    //set prefetch count to 1000
    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &time_frame.queue,
            "replay",
            BasicConsumeOptions::default(),
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();
    while let Some(Ok(delivery)) = consumer.next().await {
        delivery.ack(BasicAckOptions::default()).await?;
        let headers = match delivery.properties.headers().as_ref() {
            Some(headers) => headers,
            None => return Err(anyhow!("No headers found")),
        };
        let offset = match headers.inner().get("x-stream-offset") {
            Some(AMQPValue::LongLongInt(offset)) => offset,
            _ => return Err(anyhow!("x-stream-offset not found")),
        };
        let timestamp = *delivery.properties.timestamp();

        match is_within_timeframe(timestamp, Some(time_frame.from), Some(time_frame.to)) {
            Some(true) => {
                if *offset >= i64::try_from(message_count - 1)? {
                    messages.push(delivery);
                    break;
                }
                messages.push(delivery);
            }
            _ => {
                if *offset >= i64::try_from(message_count - 1)? {
                    break;
                }
                continue;
            }
        }
    }
    Ok(messages)
}

pub async fn fetch_messages(
    pool: &deadpool_lapin::Pool,
    rabbitmq_api_config: &RabbitmqApiConfig,
    message_options: &MessageOptions,
    message_query: MessageQuery,
) -> Result<Vec<Message>> {
    let message_count =
        match get_queue_message_count(rabbitmq_api_config, message_query.queue.as_str()).await? {
            Some(message_count) => message_count,
            None => {
                return Err(anyhow!("Queue not found or empty"));
            }
        };

    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;

    //set prefetch count to 1000
    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &message_query.queue,
            "fetch_messages",
            BasicConsumeOptions::default(),
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();

    while let Some(Ok(delivery)) = consumer.next().await {
        delivery.ack(BasicAckOptions::default()).await?;

        let headers = match delivery.properties.headers().as_ref() {
            Some(headers) => headers,
            None => return Err(anyhow!("No headers found")),
        };

        let transaction = match message_options.transaction_header.clone() {
            Some(transaction_header) => match headers.inner().get(transaction_header.as_str()) {
                Some(AMQPValue::LongString(transaction_id)) => Some(TransactionHeader {
                    name: transaction_header,
                    value: transaction_id.to_string(),
                }),
                _ => None,
            },
            None => None,
        };

        let offset = match headers.inner().get("x-stream-offset") {
            Some(AMQPValue::LongLongInt(offset)) => offset,
            _ => return Err(anyhow!("x-stream-offset not found")),
        };

        let timestamp = *delivery.properties.timestamp();

        match is_within_timeframe(timestamp, message_query.from, message_query.to) {
            Some(true) => {
                if *offset >= i64::try_from(message_count - 1)? {
                    messages.push(Message {
                        offset: Some(*offset as u64),
                        transaction,
                        timestamp: Some(
                            //unwrap is safe here, because we checked if time stamp is set
                            chrono::Utc
                                .timestamp_millis_opt(timestamp.unwrap() as i64)
                                .unwrap(),
                        ),
                        data: String::from_utf8(delivery.data)?,
                    });
                    break;
                }
                messages.push(Message {
                    offset: Some(*offset as u64),
                    transaction,
                    timestamp: Some(
                        //unwrap is safe here, because we checked if time stamp is set
                        chrono::Utc
                            .timestamp_millis_opt(timestamp.unwrap() as i64)
                            .unwrap(),
                    ),
                    data: String::from_utf8(delivery.data)?,
                });
            }
            Some(false) => {
                if *offset >= i64::try_from(message_count - 1)? {
                    break;
                }
                continue;
            }
            None => {
                if *offset >= i64::try_from(message_count - 1)? {
                    messages.push(Message {
                        offset: Some(*offset as u64),
                        transaction,
                        timestamp: None,
                        data: String::from_utf8(delivery.data)?,
                    });
                    break;
                }
                messages.push(Message {
                    offset: Some(*offset as u64),
                    transaction,
                    timestamp: None,
                    data: String::from_utf8(delivery.data)?,
                });
            }
        }
    }
    Ok(messages)
}

pub async fn replay_header(
    pool: &deadpool_lapin::Pool,
    rabbitmq_api_config: &RabbitmqApiConfig,
    header_replay: HeaderReplay,
) -> Result<Vec<Delivery>> {
    let message_count =
        match get_queue_message_count(rabbitmq_api_config, &header_replay.queue).await? {
            Some(message_count) => message_count,
            None => return Err(anyhow!("Queue not found or empty")),
        };

    let connection = pool.get().await?;

    let channel = connection.create_channel().await?;

    //set prefetch count to 1000
    channel
        .basic_qos(1000u16, BasicQosOptions { global: false })
        .await?;

    let mut consumer = channel
        .basic_consume(
            &header_replay.queue,
            "replay",
            BasicConsumeOptions::default(),
            stream_consume_args(AMQPValue::LongString("first".into())),
        )
        .await?;

    let mut messages = Vec::new();

    while let Some(Ok(delivery)) = consumer.next().await {
        delivery.ack(BasicAckOptions::default()).await?;
        let headers = match delivery.properties.headers().as_ref() {
            Some(headers) => headers,
            None => return Err(anyhow!("No headers found")),
        };

        let target_header = headers.inner().get(header_replay.header.name.as_str());
        let offset = match headers.inner().get("x-stream-offset") {
            Some(AMQPValue::LongLongInt(offset)) => offset,
            _ => return Err(anyhow!("Queue is not a stream")),
        };

        if *offset >= i64::try_from(message_count - 1)? {
            if let Some(AMQPValue::LongString(header)) = target_header {
                if *header.to_string() == header_replay.header.value {
                    messages.push(delivery);
                }
            }
            break;
        }

        if let Some(AMQPValue::LongString(header)) = target_header {
            if *header.to_string() == header_replay.header.value {
                messages.push(delivery);
            }
        }
    }
    Ok(messages)
}

async fn get_queue_message_count(
    rabitmq_api_config: &RabbitmqApiConfig,
    name: &str,
) -> Result<Option<u64>> {
    //AMQP does not provide a way to get meta data about a queue thus the management HTTP API is used.
    let client = reqwest::Client::new();

    let url = format!(
        "http://{}:{}/api/queues/%2f/{}",
        rabitmq_api_config.host, rabitmq_api_config.port, name
    );

    let res = client
        .get(url)
        .basic_auth(
            rabitmq_api_config.username.clone(),
            Some(rabitmq_api_config.password.clone()),
        )
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    if let Some(res) = res.get("type") {
        if res != "stream" {
            return Err(anyhow!("Queue is not a stream"));
        }
    }

    let message_count = res.get("messages");

    match message_count {
        Some(message_count) => Ok(Some(message_count.as_u64().unwrap())),
        None => Ok(None),
    }
}

//publishes the given messages, messages can be published with or without
//transaction- and timestamp headers depending on the environment variables set.
pub async fn publish_message(
    pool: &deadpool_lapin::Pool,
    message_options: &MessageOptions,
    messages: Vec<Delivery>,
) -> Result<Vec<Message>> {
    let connection = pool.get().await?;
    let channel = connection.create_channel().await?;
    let mut s = stream::iter(messages);
    let mut replayed_messages = Vec::new();

    while let Some(message) = s.next().await {
        let mut transaction: Option<TransactionHeader> = None;
        let mut timestamp: Option<chrono::DateTime<chrono::Utc>> = None;
        let basic_props = match (
            message_options.enable_timestamp,
            message_options.transaction_header.clone(),
        ) {
            (true, None) => {
                timestamp = Some(chrono::Utc::now());
                let timestamp_u64 = timestamp.unwrap().timestamp_millis() as u64;
                lapin::BasicProperties::default().with_timestamp(timestamp_u64)
            }
            (true, Some(transaction_header)) => {
                timestamp = Some(chrono::Utc::now());
                let timestamp_u64 = timestamp.unwrap().timestamp_millis() as u64;
                let uuid = uuid::Uuid::new_v4().to_string();
                let mut headers = FieldTable::default();
                headers.insert(
                    ShortString::from(transaction_header.as_str()),
                    AMQPValue::LongString(uuid.as_str().into()),
                );
                transaction = TransactionHeader::from_fieldtable(
                    headers.clone(),
                    transaction_header.as_str(),
                )
                .ok();
                lapin::BasicProperties::default()
                    .with_headers(headers)
                    .with_timestamp(timestamp_u64)
            }
            (false, None) => lapin::BasicProperties::default(),
            (false, Some(transaction_header)) => {
                let uuid = uuid::Uuid::new_v4().to_string();
                let mut headers = FieldTable::default();
                headers.insert(
                    ShortString::from(transaction_header.as_str()),
                    AMQPValue::LongString(uuid.as_str().into()),
                );
                transaction = TransactionHeader::from_fieldtable(
                    headers.clone(),
                    transaction_header.as_str(),
                )
                .ok();
                lapin::BasicProperties::default().with_headers(headers)
            }
        };

        channel
            .basic_publish(
                message.exchange.as_str(),
                message.routing_key.as_str(),
                lapin::options::BasicPublishOptions::default(),
                message.data.as_slice(),
                basic_props,
            )
            .await?;

        replayed_messages.push(Message {
            offset: None,
            transaction,
            timestamp,
            data: String::from_utf8(message.data)?,
        });
    }
    Ok(replayed_messages)
}

fn stream_consume_args(stream_offset: AMQPValue) -> FieldTable {
    let mut args = FieldTable::default();
    args.insert(ShortString::from("x-stream-offset"), stream_offset);
    args
}

fn is_within_timeframe(
    date: Option<u64>,
    from: Option<chrono::DateTime<chrono::Utc>>,
    to: Option<chrono::DateTime<chrono::Utc>>,
) -> Option<bool> {
    match date {
        Some(date) => {
            let date = Utc.timestamp_millis_opt(date as i64).unwrap();
            match (from, to) {
                (Some(from), Some(to)) => Some(date >= from && date <= to),
                (Some(from), None) => Some(date >= from),
                (None, Some(to)) => Some(date <= to),
                (None, None) => Some(true),
            }
        }
        None => match (from, to) {
            (None, None) => None,
            _ => Some(false),
        },
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    #[tokio::test]
    async fn test_is_within_timeframe() {
        let tests = vec![
            (
                Some(Utc.with_ymd_and_hms(2021, 10, 13, 0, 0, 0).unwrap()), // October 13, 2021 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(false),
            ),
            (
                Some(Utc.with_ymd_and_hms(2022, 3, 13, 0, 0, 0).unwrap()), // March 13, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(true),
            ),
            (
                Some(Utc.with_ymd_and_hms(2022, 8, 13, 0, 0, 0).unwrap()), // August 13, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(true),
            ),
            (
                Some(Utc.with_ymd_and_hms(2023, 1, 13, 0, 0, 0).unwrap()), // January 13, 2023 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(false),
            ),
            (
                Some(Utc.with_ymd_and_hms(2023, 6, 13, 0, 0, 0).unwrap()), // June 13, 2023 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(false),
            ),
            (
                None,
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(false),
            ),
            (None, None, None, None),
            (
                None,
                None,
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(false),
            ),
            (
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                None,
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                Some(true),
            ),
            (
                Some(Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                None,                                                     // No To date
                Some(false),
            ),
            (
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2023 00:00:00 UTC
                None,                                                     // No To date
                Some(true),
            ),
            (
                Some(Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap()), // January 1, 2022 00:00:00 UTC
                None,                                                     // No From date
                None,                                                     // No To date
                Some(true),
            ),
        ];

        for (date, from, to, expected) in tests {
            assert_eq!(
                expected,
                super::is_within_timeframe(
                    date.map(|date| date.timestamp_millis() as u64),
                    from,
                    to
                )
            );
        }
    }
}
