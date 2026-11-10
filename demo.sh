#!/usr/bin/env bash
# Ejecutar desde cualquier carpeta con: bash /ruta/demo.sh publicar
set -euo pipefail
cd -- "$(dirname -- "$0")"
TOPIC=ventas-m2l9

case "${1:-}" in
  publicar)
    # Si el topic ya existe, no se publican otros seis mensajes por error.
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --create --topic "$TOPIC" \
      --partitions 1 --replication-factor 1
    docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 --topic "$TOPIC" < ventas_eventos.csv
    ;;
  guardar)
    lote=$(mktemp)
    trap 'rm -f -- "$lote"' EXIT
    # Leer seis eventos es una extracción finita: este puente NO corre continuamente.
    docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server kafka:9092 --topic "$TOPIC" --from-beginning \
      --max-messages 6 --timeout-ms 20000 > "$lote"
    if [[ $(wc -l < "$lote") -ne 6 ]]; then
      echo "Se esperaban seis mensajes. No se escribirá un lote incompleto." >&2
      exit 1
    fi
    docker compose exec -T namenode /opt/hadoop/bin/hdfs dfs -mkdir -p /laboratorio
    docker compose exec -T namenode /opt/hadoop/bin/hdfs dfs \
      -put -f - /laboratorio/ventas.csv < "$lote"
    docker compose exec -T namenode /opt/hadoop/bin/hdfs dfs -cat /laboratorio/ventas.csv
    ;;
  procesar)
    docker compose run --rm --no-deps spark
    ;;
  *)
    echo "Uso: bash demo.sh {publicar|guardar|procesar}" >&2
    exit 2
    ;;
esac
