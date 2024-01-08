#!/bin/sh

argument=$1

if [ $argument = "up" ]; then
    echo "Creating infrastructure..."
    docker-compose up -d mongo
    sleep 5
    docker exec -it mongo /usr/local/bin/init.sh
    sleep 3
    docker-compose up -d
elif [ $argument = "build" ]; then
    echo "Creating infrastructure..."
    docker-compose build
    docker-compose up -d mongo
    sleep 5
    docker exec -it mongo /usr/local/bin/init.sh
    sleep 3
    docker-compose up -d
elif [ $argument = "rewrite" ]; then
    echo "Rewrite DAGs..."
    docker-compose stop airflow-scheduler
    docker-compose stop airflow-worker
    sleep 3
    docker-compose build
    sleep 3
    docker-compose up -d
elif [ $argument = "stop" ]; then
    echo "Stopping infrastructure..."
    docker-compose stop
elif [ $argument = "down" ]; then
    echo "Deleting infrastructure..."
    docker-compose down
else
  echo "Unknown argumnet! Options: up, stop, down"
fi