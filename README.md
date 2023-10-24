# rabbit-revival

rabbit-revival is a microservice that enables the replaying of RabbitMQ stream messages. 

## Configuration

| Variable                  | Description                                          | Default   |
|---------------------------|------------------------------------------------------|-----------|
| AMQP_CONNECTION_POOL_SIZE | Number of connections to the AMQP server.            | 5         |
| AMQP_USERNAME             | Username to use when connecting to the AMQP server.  | guest     |
| AMQP_PASSWORD             | Password to use when connecting to the AMQP server.  | guest     |
| AMQP_HOST                 | Hostname of the AMQP server.                         | localhost |
| AMQP_PORT                 | AMQP Port                                            | 5672      |
| AMQP_MANAGEMENT_PORT      | AMQP management Port.                                | 15672     |
| AMQP_TRANSACTION_HEADER   | Name of the header that contains the transaction ID. | None      |
| AMQP_ENABLE_TIMESTAMP     | Whether the AMQP messages have timestamps or not.    | true      |
| ENABLE_METRICS            | Whether to enable metrics or not.                    | false     |


# Usage

```bash
git clone https://github.com/DaAlbrecht/rabbit-revival.git
cd rabbit-revival
```

Run RabbitMQ server

```bash
docker run -it --rm --name rabbitmq -p 5552:5552 -p 5672:5672 -p 15672:15672 rabbitmq:3.12-management 
```

To generate some local dummy data run `cargo test -- --ignored`. 
This will generate messages in the queue `replay` with time stamps and a transaction header with the name `x-stream-transaction-id`.

## Start the server

```bash
 export AMQP_TRANSACTION_HEADER=x-stream-transaction-id
 cargo run
```

## List messages 

```bash
curl 'localhost:3000/list?queue=replay'  | jq
```

## Replay messages 

```bash
curl localhost:3000/replay -H 'Content-Type: application/json'  -d '{"queue":"replay", "header":{"name":"x-stream-transaction-id","value":"transaction_499"}}' | jq
```

## Contributing

Contributions to the project are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request on the project's repository.

## License

The Project is licensed under MIT license. Feel free to use, modify, and distribute it according to the terms of this license.
