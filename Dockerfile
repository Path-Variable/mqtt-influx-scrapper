FROM eclipse-temurin:21-jdk-jammy

ENV MQTT_ADDR=tcp://mosquitto:1883
ENV INFLUX_URL=http://localhost:8086
ENV INFLUX_TOKEN=MyInitialAdminToken0==

WORKDIR /app

COPY . .

RUN chmod +x ./gradlew
RUN ./gradlew build

CMD ["./gradlew", "run"]
