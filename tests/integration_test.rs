use anyhow::Result;
use chrono::{TimeZone, Utc};
use deadpool_lapin::{Config, PoolConfig, Runtime};
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions, QueueDeleteOptions},
    protocol::basic::AMQPProperties,
    types::{AMQPValue, FieldTable, ShortString},
    Connection, ConnectionProperties,
};
use rabbit_revival::{
    replay::{fetch_messages, replay_time_frame, Message, TransactionHeader},
    HeaderReplay, MessageQuery, RabbitmqApiConfig, TimeFrameReplay,
};
use testcontainers::{clients, GenericImage};

#[tokio::test]
#[ignore]
async fn local_data() {
    let message_count = 500;
    let queue_name = "replay";
    let _ = create_dummy_data(5672, message_count, queue_name)
        .await
        .unwrap();
}

async fn create_dummy_data(
    port: u16,
    message_count: i64,
    queue_name: &str,
) -> Result<Vec<Message>> {
    let connection_string = format!("amqp://guest:guest@127.0.0.1:{port}");
    let connection =
        Connection::connect(&connection_string, ConnectionProperties::default()).await?;

    let channel = connection.create_channel().await?;

    let _ = channel
        .queue_delete(queue_name, QueueDeleteOptions::default())
        .await;

    let mut queue_args = FieldTable::default();
    queue_args.insert(
        ShortString::from("x-queue-type"),
        AMQPValue::LongString("stream".into()),
    );

    channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions {
                durable: true,
                auto_delete: false,
                ..Default::default()
            },
            queue_args,
        )
        .await?;
    let mut messages = Vec::new();
    for i in 0..message_count {
        let data = b"test";
        let timestamp = Utc::now().timestamp_millis() as u64;
        let transaction_id = format!("transaction_{i}");
        let mut headers = FieldTable::default();
        headers.insert(
            ShortString::from("x-stream-transaction-id"),
            AMQPValue::LongString(transaction_id.clone().into()),
        );

        channel
            .basic_publish(
                "",
                queue_name,
                BasicPublishOptions::default(),
                data,
                AMQPProperties::default()
                    .with_headers(headers.clone())
                    .with_timestamp(timestamp),
            )
            .await?;
        messages.push(Message {
            offset: Some(i as u64),
            transaction: Some(TransactionHeader::from_fieldtable(
                &headers,
                "x-stream-transaction-id",
            )?),
            data: String::from_utf8(data.to_vec())?,
            timestamp: Some(chrono::Utc.timestamp_millis_opt(timestamp as i64).unwrap()),
        });
        tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;
    }
    Ok(messages)
}
#[tokio::test]
async fn i_test_setup() -> Result<()> {
    let docker = clients::Cli::default();
    let image = GenericImage::new("rabbitmq", "3.12-management").with_wait_for(
        testcontainers::core::WaitFor::message_on_stdout("started TCP listener on [::]:5672"),
    );
    let image = image.with_exposed_port(5672).with_exposed_port(15672);
    let node = docker.run(image);
    let amqp_port = node.get_host_port_ipv4(5672);
    let management_port = node.get_host_port_ipv4(15672);

    let message_count = 500;
    let queue_name = "replay";
    let messages = create_dummy_data(amqp_port, message_count, queue_name).await?;

    let client = reqwest::Client::new();

    loop {
        let res = client
            .get(format!(
                "http://localhost:{management_port}/api/queues/%2f/{queue_name}"
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        match res.get("messages") {
            Some(m) => {
                match res.get("type") {
                    Some(t) => assert_eq!(t.as_str().unwrap(), "stream"),
                    None => panic!("type not found"),
                }
                assert_eq!(m.as_i64().unwrap(), message_count);
                break;
            }
            None => continue,
        }
    }

    assert_eq!(messages.len(), message_count as usize);

    Ok(())
}

#[tokio::test]
async fn i_test_fetch_messsages() -> Result<()> {
    let docker = clients::Cli::default();
    let image = GenericImage::new("rabbitmq", "3.12-management").with_wait_for(
        testcontainers::core::WaitFor::message_on_stdout("started TCP listener on [::]:5672"),
    );
    let image = image.with_exposed_port(5672).with_exposed_port(15672);
    let node = docker.run(image);
    let amqp_port = node.get_host_port_ipv4(5672);
    let management_port = node.get_host_port_ipv4(15672);

    let message_count = 500;
    let queue_name = "replay";
    let published_messages = create_dummy_data(amqp_port, message_count, queue_name).await?;
    let client = reqwest::Client::new();
    loop {
        let res = client
            .get(format!(
                "http://localhost:{management_port}/api/queues/%2f/{queue_name}"
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        match res.get("messages") {
            Some(m) => {
                match res.get("type") {
                    Some(t) => assert_eq!(t.as_str().unwrap(), "stream"),
                    None => panic!("type not found"),
                }
                assert_eq!(m.as_i64().unwrap(), message_count);
                break;
            }
            None => continue,
        }
    }

    let mut cfg = Config {
        url: Some(format!("amqp://guest:guest@127.0.0.1:{amqp_port}/%2f")),
        ..Default::default()
    };

    cfg.pool = Some(PoolConfig::new(1));

    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let rabbitmq_config = RabbitmqApiConfig {
        username: "guest".to_string(),
        password: "guest".to_string(),
        host: "localhost".to_string(),
        port: management_port.to_string(),
    };

    let message_options = rabbit_revival::MessageOptions {
        transaction_header: Some("x-stream-transaction-id".to_string()),
        enable_timestamp: true,
    };

    let message_query = MessageQuery {
        queue: queue_name.to_string(),
        from: None,
        to: None,
    };

    let messages = fetch_messages(&pool, &rabbitmq_config, &message_options, message_query).await?;

    assert_eq!(messages.len(), message_count as usize);

    messages.iter().enumerate().for_each(|(i, m)| {
        assert_eq!(m.data, published_messages[i].data);
        assert_eq!(m.offset, published_messages[i].offset);
        assert_eq!(m.timestamp, published_messages[i].timestamp);
        assert_eq!(
            m.transaction.as_ref().unwrap().name,
            published_messages[i].transaction.as_ref().unwrap().name
        );
        assert_eq!(
            m.transaction.as_ref().unwrap().value,
            published_messages[i].transaction.as_ref().unwrap().value
        );
    });

    Ok(())
}

#[tokio::test]
async fn i_test_replay_time_frame() -> Result<()> {
    let docker = clients::Cli::default();
    let image = GenericImage::new("rabbitmq", "3.12-management").with_wait_for(
        testcontainers::core::WaitFor::message_on_stdout("started TCP listener on [::]:5672"),
    );
    let image = image.with_exposed_port(5672).with_exposed_port(15672);
    let node = docker.run(image);
    let amqp_port = node.get_host_port_ipv4(5672);
    let management_port = node.get_host_port_ipv4(15672);

    let message_count = 500;
    let queue_name = "replay";
    let published_messages = create_dummy_data(amqp_port, message_count, queue_name).await?;
    let client = reqwest::Client::new();
    loop {
        let res = client
            .get(format!(
                "http://localhost:{management_port}/api/queues/%2f/{queue_name}"
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        match res.get("messages") {
            Some(m) => {
                match res.get("type") {
                    Some(t) => assert_eq!(t.as_str().unwrap(), "stream"),
                    None => panic!("type not found"),
                }
                assert_eq!(m.as_i64().unwrap(), message_count);
                break;
            }
            None => continue,
        }
    }

    let cfg = Config {
        url: Some(format!("amqp://guest:guest@localhost:{amqp_port}/%2f")),
        ..Default::default()
    };

    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let rabbitmq_config = RabbitmqApiConfig {
        username: "guest".to_string(),
        password: "guest".to_string(),
        host: "localhost".to_string(),
        port: management_port.to_string(),
    };

    let time_frame_replay = TimeFrameReplay {
        queue: queue_name.to_string(),
        from: published_messages.first().unwrap().timestamp.unwrap(),
        to: published_messages.last().unwrap().timestamp.unwrap(),
    };

    let replayed_messages = replay_time_frame(&pool, &rabbitmq_config, time_frame_replay).await?;

    assert_eq!(replayed_messages.len(), published_messages.len());

    replayed_messages.iter().enumerate().for_each(|(i, m)| {
        assert_eq!(
            String::from_utf8(m.data.clone()).unwrap(),
            published_messages[i].data
        );
        let headers = m.properties.headers().clone().unwrap();
        let offset = headers.inner().get("x-stream-offset").unwrap();
        let offset = match offset {
            AMQPValue::LongLongInt(i) => i,
            _ => panic!("offset not found"),
        };
        let timestamp = m.properties.timestamp().unwrap();
        let timestamp = Utc.timestamp_millis_opt(timestamp as i64).unwrap();
        assert_eq!(*offset as u64, published_messages[i].offset.unwrap());
        assert_eq!(timestamp, published_messages[i].timestamp.unwrap());
    });

    let time_frame_replay = TimeFrameReplay {
        queue: queue_name.to_string(),
        from: published_messages.last().unwrap().timestamp.unwrap(),
        to: published_messages.last().unwrap().timestamp.unwrap(),
    };
    let replayed_messages = replay_time_frame(&pool, &rabbitmq_config, time_frame_replay).await?;
    assert_eq!(replayed_messages.len(), 1);

    assert_eq!(
        String::from_utf8(replayed_messages[0].data.clone()).unwrap(),
        published_messages.last().unwrap().data
    );

    Ok(())
}

#[tokio::test]
async fn i_test_replay_header() -> Result<()> {
    let docker = clients::Cli::default();
    let image = GenericImage::new("rabbitmq", "3.12-management").with_wait_for(
        testcontainers::core::WaitFor::message_on_stdout("started TCP listener on [::]:5672"),
    );
    let image = image.with_exposed_port(5672).with_exposed_port(15672);
    let node = docker.run(image);
    let amqp_port = node.get_host_port_ipv4(5672);
    let management_port = node.get_host_port_ipv4(15672);

    let message_count = 500;
    let queue_name = "replay";
    let published_messages = create_dummy_data(amqp_port, message_count, queue_name).await?;
    let client = reqwest::Client::new();
    loop {
        let res = client
            .get(format!(
                "http://localhost:{management_port}/api/queues/%2f/{queue_name}"
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;
        match res.get("messages") {
            Some(m) => {
                match res.get("type") {
                    Some(t) => assert_eq!(t.as_str().unwrap(), "stream"),
                    None => panic!("type not found"),
                }
                assert_eq!(m.as_i64().unwrap(), message_count);
                break;
            }
            None => continue,
        }
    }

    let mut cfg = Config {
        url: Some(format!("amqp://guest:guest@localhost:{amqp_port}/%2f")),
        ..Default::default()
    };
    cfg.pool = Some(PoolConfig::new(1));

    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let rabbitmq_config = RabbitmqApiConfig {
        username: "guest".to_string(),
        password: "guest".to_string(),
        host: "localhost".to_string(),
        port: management_port.to_string(),
    };

    for m in published_messages {
        let header_replay = HeaderReplay {
            queue: queue_name.to_string(),
            header: rabbit_revival::AMQPHeader {
                name: "x-stream-transaction-id".to_string(),
                value: m.transaction.unwrap().value,
            },
        };
        let replayed_messages =
            rabbit_revival::replay::replay_header(&pool, &rabbitmq_config, header_replay).await?;
        assert_eq!(replayed_messages.len(), 1);
    }

    Ok(())
}
