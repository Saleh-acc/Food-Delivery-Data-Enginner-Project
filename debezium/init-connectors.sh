#!/bin/bash
# Tells Linux to run this script using the bash shell

echo "Waiting for Kafka Connect REST API..."

# Keep checking until Kafka Connect API becomes fully available
# curl -f fails if HTTP status is not 200-level
# > /dev/null hides normal output
# 2>&1 hides error output
until curl -f http://localhost:8083/connectors > /dev/null 2>&1; do

  # Wait 5 seconds before retrying
  sleep 5

done

echo "Kafka Connect is ready. Registering connectors..."

for file in /connectors/*.json; do

  echo "Creating connector: $file"
  # Print current connector file being processed

  # Send HTTP request to Kafka Connect REST API
  # and store only the HTTP status code in "response"

  # HTTP POST request
    # Used to create a new connector
    # Tell API that request body is JSON
    # Read JSON content from current connector file
  response=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @"$file")

  # If connector created successfully
  # 201 = Created
  # 409 = Connector already exists
  if [ "$response" = "201" ] || [ "$response" = "409" ]; then

    echo "Connector registered successfully"

  else

    # Any other status means failure
    echo "Failed with status: $response"

  fi

done

echo "All connectors processed!"
# Final completion message