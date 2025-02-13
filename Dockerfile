# Verwenden Sie ein offizielles PySpark-Basisimage
FROM bitnami/spark:3.5.0

# Setzen Sie den Arbeitsverzeichnis im Container
WORKDIR /app

# Kopieren Sie das Skript und alle erforderlichen Dateien in den Container
COPY . /app

# Installieren Sie die erforderlichen Python-Pakete
RUN pip install azure-storage-file-datalake azure-identity

# Installieren Sie die Hadoop-Azure-Bibliothek
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar && \
    mv hadoop-azure-3.3.4.jar /opt/bitnami/spark/jars/

RUN curl -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar && \
    mv azure-storage-8.6.6.jar /opt/bitnami/spark/jars/

# Setzen Sie den Befehl, der beim Start des Containers ausgeführt wird
CMD ["spark-submit", "crime_weather_gold.py"]