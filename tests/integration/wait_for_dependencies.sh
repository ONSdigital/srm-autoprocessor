#!/usr/bin/env bash
POSTGRES_CONTAINER=postgres-autoprocessor-its
echo "Waiting for [$POSTGRES_CONTAINER] to be ready"

while true; do
    response=$(docker inspect $POSTGRES_CONTAINER -f "{{ .State.Health.Status }}")
    if [[ "$response" == "healthy" ]]; then
        echo "[$POSTGRES_CONTAINER] is ready"
        break
    fi

    echo "[$POSTGRES_CONTAINER] not ready ([$response] is its current state)"
    ((attempt++))
    if [[ attempt -eq 30 ]]; then
        echo "[$POSTGRES_CONTAINER] failed to start"
        exit 1
    fi
    sleep 2

done

echo "Containers running and alive"
