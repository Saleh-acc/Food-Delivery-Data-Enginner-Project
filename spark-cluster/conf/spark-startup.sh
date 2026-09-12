#!/bin/bash
set -e

SERVICE_NAME=${SPARK_SERVICE_NAME:-Unknown}
HOSTNAME=$(hostname)
HOST_DISPLAY=${HOST_DISPLAY:-localhost}

echo "=================================================="
echo "Spark ${SERVICE_NAME} starting"
echo "Host: ${HOSTNAME}"
echo "Time: $(date)"
echo "=================================================="

case "$SERVICE_NAME" in
  Master)
    echo "UI: http://${HOST_DISPLAY}:8080"
    ;;
  Worker-1)
    echo "Worker UI: http://${HOST_DISPLAY}:8081"
    ;;
  Worker-2)
    echo "Worker UI: http://${HOST_DISPLAY}:8082"
    ;;
  History)
    echo "History UI: http://${HOST_DISPLAY}:18080"
    ;;
  *)
    echo "Unknown service: $SERVICE_NAME"
    ;;
esac

if [ $# -eq 0 ]; then
   echo "No command supplied."
   exit 1
fi

exec "$@"