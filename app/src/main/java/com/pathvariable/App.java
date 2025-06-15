package com.pathvariable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.WriteParameters;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static com.influxdb.client.domain.WritePrecision.S;
import static com.influxdb.client.write.Point.measurement;

public class App {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static InfluxDBClient client;


    public static void main(String[] args) throws MqttException {
        System.out.println("Starting application");

        String url = System.getenv("INFLUX_URL");
        String token = System.getenv("INFLUX_TOKEN");
        String mqttUrl = System.getenv("MQTT_ADDR");
        String org = System.getenv("INFLUX_ORG");
        String bucket = System.getenv("INFLUX_BUCKET");

        System.out.printf("Url is %s%n", url);
        System.out.println("Bucket is " + bucket);
        System.out.println("Org is " + org);
        client = InfluxDBClientFactory.create(url,token.toCharArray());
        System.out.println(client.ready());

        String publisherId = UUID.randomUUID().toString();
        System.out.printf("Publisher id is %s%n", publisherId);

        try (IMqttClient subscriber = new MqttClient(mqttUrl, publisherId)) {
            subscriber.connect(getOptions());
            subscriber.subscribe(System.getenv("MQTT_TOPIC"), (topic, msg) -> {
                System.out.printf("Topic:%s Message:%s\n", topic, msg.toString());
                String sensorName = parseSensorName(topic);
                System.out.printf("Sensor name:%s%n", sensorName);
                Map<String, Object> message = parseMessage(msg.getPayload());
                System.out.printf("Parsed message:%s%n", message.toString());
                publishValues(sensorName, message, org, bucket);
            });
        }

        System.out.println("here");
    }

    private static void publishValues(String sensorName, Map<String, Object> message, String org, String bucket) {
        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        final var now = Instant.now();
        final var points = message.keySet()
                .stream()
                .map(k -> measurement(sensorName)
                                .addField(k, (Number) message.get(k))
                                .time(now, S))
                .toList();
        System.out.println(points);
        writeApi.writePoints(points, new WriteParameters(bucket, org, S));
    }

    private static MqttConnectOptions getOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(false);
        options.setConnectionTimeout(10);


        return options;
    }

    private static Map<String, Object> parseMessage(byte[] message) {
        try {
            return OBJECT_MAPPER.readValue(new String(message), new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            System.out.println("Invalid message format");
            return Map.of();
        }
    }

    private static String parseSensorName(String topic) {
        String[] split = topic.split("/");
        return split.length > 0 ? split[split.length - 1] : topic;
    }

}
