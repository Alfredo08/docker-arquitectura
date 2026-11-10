# Kafka, HDFS y Spark con Docker Compose

Mismo caso M2L9: seis mensajes de ventas incluyen un reintento, una cantidad inválida y una venta tardía. No se resuelve la consigna. No necesitas PostgreSQL, Snowflake ni Python instalado en tu equipo para correr Spark: el contenedor incluye Python.

```text
CSV local → Kafka → consumidor de consola → archivo en HDFS → Spark → reporte
```

El puente hacia HDFS lo disparas tú: lee exactamente seis mensajes y los guarda como un archivo. El trabajo Spark es **batch**. Hay un broker Kafka, un NameNode y un DataNode de HDFS, y un trabajo Spark local de dos hilos. No se levanta YARN, MapReduce ni un clúster Spark. Este recorrido tampoco implementa Lambda, Kappa ni Spark Structured Streaming; sirve para ver los componentes que podrían participar en esos diseños.

## Ensayo antes de la clase

1. Abre Docker Desktop y espera a que esté listo. Descomprime el paquete, abre una terminal en la carpeta que contiene compose.yaml y ejecuta los comandos desde allí. Los ejemplos usan Bash (macOS o Linux; en Windows, una terminal WSL integrada con Docker Desktop).
2. Descarga las imágenes antes de proyectar. Hadoop es una imagen grande y la primera descarga tarda:

   ```sh
   docker compose --profile trabajos pull
   ```

   El Hadoop de este laboratorio está publicado para linux/amd64. En Apple Silicon Compose pide esa plataforma explícitamente; necesitas emulación amd64 en Docker Desktop y el arranque puede ser más lento. Si el equipo tiene poca memoria, cierra aplicaciones antes de ensayar. Un margen de 6–8 GB asignado a Docker es una referencia, no un requisito garantizado.
3. Levanta Kafka y los dos procesos HDFS y espera hasta que estén listos:

   ```sh
   docker compose up -d --wait --wait-timeout 240 kafka namenode datanode
   docker compose ps
   ```

   Kafka y NameNode deben aparecer saludables; DataNode, en ejecución. La comprobación del NameNode espera un DataNode vivo y que termine el modo seguro. Si se vence la espera, revisa `docker compose logs --tail=80 kafka namenode datanode`. En particular, si HDFS no tiene DataNodes vivos, no avances a guardar el archivo.
4. Abre [el panel local de HDFS](http://localhost:19870/). En el NameNode puedes ver que hay un DataNode vivo. Kafka no publica puertos a tu computadora: sus clientes se ejecutan dentro de la red de Compose. Si el puerto 19870 está ocupado, cambia solo ese puerto de la izquierda en compose.yaml y usa el nuevo número en el navegador.
5. Ejecuta los tres pasos de abajo y comprueba 950 y 1400. Para repetir desde cero en clase, usa la limpieza de ensayo indicada al final. Si conservas el topic del ensayo, omite publicar y empieza en guardar; la extracción se puede repetir.

## En vivo: tres pasos

### 1. Publicar en Kafka

```sh
bash demo.sh publicar
```

El script crea el topic `ventas-m2l9` con una partición y publica cada línea de ventas_eventos.csv como un mensaje. Por diseño se ejecuta una vez por topic: si ya existe, se detiene antes de volver a publicar.

Puedes verificar el topic con:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

**Frase:** «Kafka recibe los eventos y permite leerlos después. El archivo local es nuestro productor simulado».

### 2. Guardar en Hadoop HDFS

```sh
bash demo.sh guardar
```

El consumidor lee los seis mensajes desde el inicio. El script comprueba el número de filas, escribe `/laboratorio/ventas.csv` en HDFS y muestra su contenido. El NameNode mantiene metadatos; el DataNode almacena los bloques. Si vuelves a ejecutar guardar con el mismo topic, vuelve a escribir únicamente ese archivo de práctica.

**Frase:** «Los seis registros siguen disponibles para reprocesar. El duplicado y la fila inválida se conservan en bruto; Spark decidirá qué contar».

### 3. Ejecutar Spark

```sh
bash demo.sh procesar
```

Compose inicia un contenedor de trabajo que lee directamente `hdfs://namenode:8020/laboratorio/ventas.csv` y ejecuta el reporte. Verás:

| Consulta | Ventas válidas | Total |
|---|---:|---:|
| Llegadas hasta las 09:07 | 3 | 950 |
| Todas las llegadas | 4 | 1400 |

El reporte filtra cantidades o precios no positivos, elimina reintentos idénticos por id y datos de importe, y suma cantidad × precio. El evento de 450 llega después del corte; por eso cambia el segundo resultado. El contenedor Spark se retira al terminar. El resultado aparece en consola; no se escribe una tabla nueva.

**Frase:** «Spark procesa un lote que ya quedó guardado. Si repito el trabajo, reconstruyo el reporte; no vuelvo a sumar sobre un resultado anterior».


## Qué se verificó

Se comprobó que Compose interpreta el archivo, que Bash y los archivos de configuración son válidos y que los seis eventos producen 950 y 1400. El acceso al servicio Docker está bloqueado en este entorno, así que **no se arrancaron los contenedores aquí**. Haz el ensayo completo en Docker Desktop antes de usarlo en clase.

- Kafka: https://kafka.apache.org/40/getting-started/docker/
- Hadoop HDFS: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SingleCluster.html
- Imágenes Hadoop: https://hub.docker.com/r/apache/hadoop/tags
- Imágenes Spark: https://hub.docker.com/r/apache/spark/tags
- Lectura CSV en Spark: https://spark.apache.org/docs/latest/sql-data-sources-csv.html
