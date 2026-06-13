# Imagen base del sistema operativo
FROM ubuntu:22.04

# Desactivar interacción durante la instalación de paquetes
ENV DEBIAN_FRONTEND=noninteractive

# Instalación de dependencias del sistema operativo
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    openjdk-11-jre-headless \
    curl \
    procps \
    bc \
    git \
    build-essential \
    libpq-dev \
    tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configuración del entorno de Python
RUN ln -s /usr/bin/python3.10 /usr/bin/python

# Definición del entorno de ejecución de Java (Requerido por Apache Spark)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Parámetros de recolección de basura y gestión de memoria de la JVM
ENV SPARK_DRIVER_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"

# Variables de entorno globales del orquestador y sistema
ENV AIRFLOW_HOME=/tfm/airflow
ENV TZ=UTC

# Directorio de trabajo del contenedor
WORKDIR /tfm

# Instalación de Apache Airflow mediante restricciones de resolución de dependencias
ARG AIRFLOW_VERSION=2.7.1
ARG PYTHON_VERSION=3.10
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Instalación de dependencias del entorno de datos y machine learning
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Creación de la topología de directorios del proyecto
RUN mkdir -p /tfm/airflow /tfm/python_notebooks /tfm/delta_lake /tfm/spark-warehouse /tfm/local_docker_ia

# Directorio de persistencia de metadatos para Spark
ENV SPARK_WAREHOUSE_DIR=/tfm/spark-warehouse

# Copia de los archivos de configuración y scripts del entorno
COPY . /tfm/local_docker_ia/

# Asignación de permisos de ejecución al punto de entrada
RUN cp /tfm/local_docker_ia/start.sh /usr/local/bin/start.sh && \
    chmod +x /usr/local/bin/start.sh

# Exposición de puertos de las interfaces web (JupyterLab y Airflow)
EXPOSE 8888 8080

# Definición del proceso de inicialización principal
ENTRYPOINT ["/usr/local/bin/start.sh"]
