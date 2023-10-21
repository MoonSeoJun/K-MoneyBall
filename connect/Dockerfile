FROM confluentinc/cp-kafka-connect:7.2.5

#If you want to run a local build of the connector, uncomment the COPY command and make sure the JAR file is in the directory path
#COPY mongo-kafka-connect-<<INSERT BUILD HERE>>3-all.jar /usr/share/confluent-hub-components
RUN confluent-hub install --no-prompt --verbose mongodb/kafka-connect-mongodb:latest

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

ENV KAFKA_JMX_PORT 35000
ENV KAFKA_JMX_HOSTNAME: localhost
ENV CONNECT_BOOTSTRAP_SERVERS: "broker:29092"
ENV CONNECT_REST_ADVERTISED_HOST_NAME: connect
ENV CONNECT_REST_PORT: 8083
ENV CONNECT_GROUP_ID: connect-cluster-group
ENV CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
ENV CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
ENV CONNECT_OFFSET_FLUSH_INTERVAL_MS: 10000
ENV CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
ENV CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
ENV CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
ENV CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
ENV CONNECT_ZOOKEEPER_CONNECT: "zookeeper:2181"
ENV CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components"
ENV CONNECT_CONNECTIONS_MAX_IDLE_MS: 180000
ENV CONNECT_METADATA_MAX_AGE_MS: 180000
ENV CONNECT_AUTO_CREATE_TOPICS_ENABLE: "true"
ENV CONNECT_KEY_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"
ENV CONNECT_VALUE_CONVERTER: "org.apache.kafka.connect.json.JsonConverter"

ADD connectors /etc/kafka-connect/connectors

WORKDIR /etc/kafka-connect/connectors

# RUN chmod +x ./register_connectors.sh

CMD ["./register_connectors.sh"]