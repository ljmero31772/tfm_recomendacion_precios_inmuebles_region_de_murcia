#!/bin/bash

# =============================================================================
# Script de inicialización del entorno de trabajo
# Servicios desplegados: Airflow Scheduler, Airflow Webserver, JupyterLab
# =============================================================================

# Forzar zona horaria UTC
export TZ=UTC
export AIRFLOW_HOME=/tfm/airflow

echo "================================================"
echo " Iniciando entorno de trabajo..."
echo "================================================"

# A. Inicialización del esquema de base de datos y usuarios administradores
echo "[1/4] Preparando entorno..."
airflow db init > /dev/null 2>&1
airflow users list | grep -q "^admin" || airflow users create \
    --username admin --firstname Admin --lastname User \
    --role Admin --email admin@example.com --password admin > /dev/null 2>&1

echo "      Base de datos y usuario admin inicializados."

# B. Definición de subrutinas de ejecución de servicios en segundo plano
start_scheduler() {
    echo "      [Watchdog] Arrancando Airflow Scheduler..."
    airflow scheduler > /tfm/airflow/scheduler.log 2>&1 &
    SCHEDULER_PID=$!
}

start_webserver() {
    echo "      [Watchdog] Arrancando Airflow Webserver..."
    airflow webserver --port 8080 > /tfm/airflow/webserver.log 2>&1 &
    WEBSERVER_PID=$!
}

start_jupyter() {
    echo "      [Watchdog] Arrancando JupyterLab..."
    jupyter lab \
        --ip=0.0.0.0 \
        --port=8888 \
        --no-browser \
        --allow-root \
        --NotebookApp.token='' \
        --NotebookApp.password='' \
        --ServerApp.iopub_data_rate_limit=1.0e10 \
        > /tfm/python_notebooks/jupyter.log 2>&1 &
    JUPYTER_PID=$!
}

# C. Disparo inicial de servicios
echo "[3/4] Lanzando servicios principales..."
start_scheduler
start_webserver
start_jupyter
echo "      Todos los servicios desplegados y activos."

# D. Bucle principal: Monitorización de disponibilidad (Watchdog) y control de inactividad
echo "[4/4] Entrando en modo vigilancia activa..."
echo "      - Watchdog: Recuperación automática ante caídas de procesos."
echo "      - Idle Monitor: Desconexión automática tras 60 minutos de inactividad de Python."
echo ""

IDLE_TIME=0
TIMEOUT=3600  # 60 minutos (ajustable)
SHUTTING_DOWN=false

while true; do
    # 1. Watchdog: comprobar si los procesos siguen vivos
    if ! kill -0 $SCHEDULER_PID > /dev/null 2>&1 && [ "$SHUTTING_DOWN" = false ]; then
        echo "      [!] Detectada caída de Scheduler. Reiniciando..."
        start_scheduler
    fi

    if ! kill -0 $WEBSERVER_PID > /dev/null 2>&1 && [ "$SHUTTING_DOWN" = false ]; then
        echo "      [!] Detectada caída de Webserver. Reiniciando..."
        start_webserver
    fi

    if ! kill -0 $JUPYTER_PID > /dev/null 2>&1 && [ "$SHUTTING_DOWN" = false ]; then
        echo "      [!] Detectada caída de JupyterLab. Reiniciando..."
        start_jupyter
    fi

    # 2. Idle Monitor: contar procesos Python activos (excluye servidores base)
    ACTIVE=$(ps aux | grep python | grep -vE "(airflow|jupyter-lab|grep|start.sh)" | wc -l)

    if [ "$ACTIVE" -eq 0 ]; then
        IDLE_TIME=$((IDLE_TIME + 60))
    else
        IDLE_TIME=0
    fi

    # Notificar inactividad cada 10 min (opcional, para logs)
    # if [ $((IDLE_TIME % 600)) -eq 0 ] && [ "$IDLE_TIME" -gt 0 ]; then
    #    echo "      [Info] Inactividad: $((IDLE_TIME / 60)) / $((TIMEOUT / 60)) min"
    # fi

    # 3. Comprobar timeout
    if [ "$IDLE_TIME" -ge "$TIMEOUT" ]; then
        echo "================================================"
        echo " 60 minutos de inactividad detectados."
        echo " Cerrando contenedor para ahorrar recursos..."
        echo "================================================"
        SHUTTING_DOWN=true
        pkill -f airflow
        pkill -f jupyter
        exit 0
    fi

    sleep 60
done
