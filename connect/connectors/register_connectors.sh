#!/bin/sh

/etc/confluent/docker/run &
echo -e "Waiting for Kafka Connect to start listening on localhost"
while [ $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) -ne 200 ] ; do 
  echo -e $(date) " Kafka Connect listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors) " (waiting for 200)"
  sleep 10 
done
echo -e $(date) "Kafka Connect is ready! Listener HTTP state: " $(curl -s -o /dev/null -w %{http_code} http://localhost:8083/connectors)


for connector in *.json; do
  curl -X POST -H "Content-Type: application/json" -d @$connector http://localhost:8083/connectors -w "\n"
  sleep 5
done

# curl -X "POST" "http://localhost:8083/connectors" -H "Content-Type: application/json" -d ./simple_source.json

sleep infinity