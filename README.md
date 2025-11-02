# mqtt-influx-scrapper

Demo app for InfluxDB - MQTT presentation

A small Java service that subscribes to MQTT topics, parses incoming messages and writes measurements to InfluxDB. This repository contains the demo implementation and a Dockerfile to run the service in a container.

---

## Features

- Connects to an MQTT broker and subscribes to configurable topic(s)
- Parses simple message payloads and converts them into InfluxDB line protocol points
- Writes measurements to InfluxDB (v1/v2-compatible usage can be described in configuration)
- Configurable via environment variables or properties file
- Lightweight Java implementation suitable for demos and experiments
- Dockerfile included for easy containerized runs

---

## Table of Contents

- Requirements
- Quick start (Docker)
- Running locally (Gradle)
- Configuration
- Message format / Example
- InfluxDB compatibility notes
- Logs & troubleshooting
- Development
- Contributing

---

## Requirements

- Java 11+ (or the version the project targets)
- Gradle if running locally
- Docker (optional, to run the provided Dockerfile)
- An MQTT broker (e.g., Mosquitto)
- An InfluxDB instance (v1.x or v2 with token; adjust configuration accordingly)

---

## Quick start (Docker)

1. Build the Docker image (from project root):

       docker build -t mqtt-influx-scrapper:latest .

2. Run the container with environment variables to point to your MQTT broker and InfluxDB:

       docker run -d \
       --name mqtt-influx-scrapper \
       -e MQTT_BROKER_URL=tcp://broker.example.com:1883 \
       -e MQTT_CLIENT_ID=mqtt-influx-demo \
       -e MQTT_TOPIC=home/+/sensors \
       -e INFLUX_URL=http://influx.example.com:8086 \
       -e INFLUX_ORG=my-org \
       -e INFLUX_BUCKET=my-bucket \
       -e INFLUX_TOKEN=token_here \
       mqtt-influx-scrapper:latest

Adjust environment variables below according to your setup.

---

## Running locally

1. Clone the repository:

       git clone https://github.com/Path-Variable/mqtt-influx-scrapper.git
       cd mqtt-influx-scrapper

2. Build:

       ./gradlew build

3. Run:

       java -jar target/mqtt-influx-scrapper-<version>.jar \
       --mqtt.broker=tcp://broker.example.com:1883 \
       --mqtt.clientId=mqtt-influx-demo \
       --mqtt.topic=home/+/sensors \
       --influx.url=http://influx.example.com:8086 \
       --influx.org=my-org \
       --influx.bucket=my-bucket \
       --influx.token=token_here

Alternatively set the same properties via environment variables (see Configuration).

---

## Configuration

The app reads settings from environment variables and/or application properties. Use whichever is most convenient. Example variables/properties (names used here are examples — verify actual property keys in the code):

- MQTT settings
  - MQTT_BROKER_URL / mqtt.broker — MQTT broker URI (e.g., tcp://broker:1883)
  - MQTT_CLIENT_ID / mqtt.clientId — Client ID for MQTT connection
  - MQTT_TOPIC / mqtt.topic — Topic filter(s) to subscribe to (supports wildcards)
  - MQTT_QOS / mqtt.qos — QoS level (0, 1, or 2). Default: 0
  - MQTT_USERNAME / mqtt.username — (optional) username for broker auth
  - MQTT_PASSWORD / mqtt.password — (optional) password for broker auth

- InfluxDB settings
  - INFLUX_URL / influx.url — InfluxDB HTTP API URL (e.g., http://localhost:8086)
  - INFLUX_TOKEN / influx.token — (for InfluxDB v2) authorization token
  - INFLUX_ORG / influx.org — (for v2) organization name or id
  - INFLUX_BUCKET / influx.bucket — (for v2) bucket name
  - INFLUX_DB / influx.db — (for v1) database name
  - INFLUX_USER / influx.user — (for v1) username
  - INFLUX_PASS / influx.pass — (for v1) password
  - INFLUX_WRITE_TIMEOUT_MS / influx.timeout — write timeout

- Parsing & behavior
  - PARSE_STRATEGY / parse.strategy — Strategy used to convert MQTT payload to measurement (e.g., json, csv, raw)
  - MEASUREMENT_NAME / measurement.name — default measurement name if not derived from topic/payload
  - TOPIC_TAGS / topic.tags — mapping rules to convert topic path segments into measurement tags

Note: confirm exact property names by checking the project's configuration class or application.properties in the repo. The property names above are common conventions used in such demos.

---

## Message format / Examples

This demo can be adapted to a few message formats. Example payloads and how they might map to InfluxDB:

- JSON payload example:
  
      Topic: home/livingroom/sensors/temperature
      Payload:
      {"sensor":"temp-1","value":22.7,"unit":"C","battery":98}
    
      Example InfluxDB point:
      measurement: temperature
      tags: sensor=temp-1, location=livingroom
      fields: value=22.7, unit="C", battery=98
      timestamp: (now or included in payload)

Topic-to-tag mapping:
- If the topic is home/kitchen/sensor1/temperature, a mapping rule can extract "kitchen" as location and "sensor1" as sensor tag.

Adjust parsing rules in configuration or code to match your MQTT message format.

---

## InfluxDB compatibility notes

- For InfluxDB v2, use INFLUX_TOKEN + ORG + BUCKET.
- For InfluxDB v1, use INFLUX_DB and credentials; or use the v1 compatibility API if enabled.
- If timestamps are included in payloads, ensure they are converted to nanoseconds (or the precision expected) before writing.
- Batch writes: the client may buffer points and write in batches — tune batch size and flush interval for throughput/latency trade-offs.

---

## Logs & troubleshooting

- Logs are written to standard output. Use docker logs or your process manager to view them.
- Common issues:
  - Cannot connect to MQTT broker: check broker URL, network, credentials.
  - Cannot write to InfluxDB: verify URL, token/credentials, bucket/database names, and network access.
  - Parse errors: inspect the incoming payloads; enable debug logs to see parsing details.

Enable debug logging (example via Java system property or logback/log4j config) to troubleshoot parsing and connection flows.

---

## Development

- Code is primarily Java. Follow the project's build system (Maven or Gradle).
- To add a new parser for a message type, implement the parser interface (look for classes named Parser, MessageHandler, or similar).
- To add more flexible topic-to-tag mapping, modify the topic parsing utilities in the source.

Run unit tests:

    ./gradlew test

---

## Contributing

Contributions are welcome. Suggested steps:
1. Fork the repository.
2. Create a feature branch (feature/your-feature).
3. Implement your change and add tests.
4. Open a pull request describing the change.

Please follow the existing code style and include unit tests for new functionality.

---

## Example: Full environment example for docker-compose

    version: '3.8'
    services:
      mqtt:
        image: eclipse-mosquitto:2
        ports:
          - "1883:1883"
      influx:
        image: influxdb:2.6
        environment:
          - INFLUXDB_ADMIN_USER=admin
          - INFLUXDB_ADMIN_PASSWORD=adminpass
        ports:
          - "8086:8086"
      scrapper:
        image: mqtt-influx-scrapper:latest
        environment:
          - MQTT_BROKER_URL=tcp://mqtt:1883
          - MQTT_TOPIC=home/+/sensors/#
          - INFLUX_URL=http://influx:8086
          - INFLUX_TOKEN=your-token
          - INFLUX_ORG=your-org
          - INFLUX_BUCKET=your-bucket
        depends_on:
          - mqtt
          - influx
