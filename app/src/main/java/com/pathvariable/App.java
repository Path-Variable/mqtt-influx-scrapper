package com.pathvariable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class App {

    private static final String BUCKET = "smart_garden";
    private static final String ORG = "Path Variable";

    private static final String TEMPERATURE = "temperature";
    private static final String SOIL_MOISTURE = "soil_moisture";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static InfluxDBClient client;


    public static void main(String[] args) throws MqttException {
        System.out.println("Starting application");

        String url = System.getProperty("INFLUX_URL");
        String token = System.getProperty("INFLUX_TOKEN");
        String mqttUrl = System.getProperty("MQTT_URL");

        System.out.println("Url is %s".formatted(url));
        client = InfluxDBClientFactory.create(url,token.toCharArray());

        String publisherId = UUID.randomUUID().toString();
        System.out.println("Publisher id is %s".formatted(publisherId));

        try (IMqttClient subscriber = new MqttClient(mqttUrl, publisherId)) {
            subscriber.connect(getOptions());
            subscriber.subscribe("zigbee2mqtt/garden/#", (topic, msg) -> {
                System.out.printf("Topic:%s Message:%s\n", topic, msg.toString());
                String sensorName = parseSensorName(topic);
                System.out.println("Sensor name:%s".formatted(sensorName));
                Map<String, Object> message = parseMessage(msg.getPayload());
                System.out.println("Parsed message:%s".formatted(message.toString()));
                publishValues(sensorName, getNumber(message, TEMPERATURE),
                    getNumber(message, SOIL_MOISTURE));
            });
        }
        System.out.println("here");
    }

    private static MqttConnectOptions getOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
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

    private static Number getNumber(Map<String,Object> message, String key) {
        Object value = message.get(key);
        System.out.println(value);
        return value instanceof Number ? (Number) value : null;
    }


    private static void publishValues(String sensorName, Number temperature, Number soilMoisture) {
        if (temperature == null || soilMoisture == null) {
            System.out.println("Invalid message. Will not publish");
            return;
        }
        String temperatureLine = "temperature,sensor=%s value_in_celsius=%f";
        String soilMoistureLine = "soil_moisture,sensor=%s percentage=%f";

        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        writeApi.writeRecord(BUCKET, ORG, WritePrecision.NS, temperatureLine.formatted(sensorName,temperature.doubleValue()));
        writeApi.writeRecord(BUCKET, ORG, WritePrecision.NS, soilMoistureLine.formatted(sensorName,soilMoisture.doubleValue()));
        System.out.println("Published value");
    }

}
