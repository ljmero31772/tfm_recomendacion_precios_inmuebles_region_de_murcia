"""
DAG: full_idealista
===================
Orquestador global del proyecto.
Ejecuta de forma paralela los DAGs de scraping y refinado de CREM e Idealista,
y una vez completados con éxito, lanza el DAG de entrenamiento de Machine Learning.
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

# ============================================================
# CONFIGURACIÓN Y PARÁMETROS
# ============================================================

DAG_ID = 'full_idealista'

# ============================================================
# ARGUMENTOS POR DEFECTO
# ============================================================

default_args = {
    'owner': 'admin',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# ============================================================
# CUERPO DEL DAG PRINCIPAL
# ============================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Orquestación completa: Scraping (CREM + Idealista) -> ML Training.',
    schedule_interval=None,
    catchup=False,
    tags=['global', 'orchestrator', 'optimized'],
    max_active_runs=1,
) as dag:

    # 1. Trigger para el DAG de CREM (esperando a que termine)
    t_trigger_crem = TriggerDagRunOperator(
        task_id='trigger_scrapear_crem',
        trigger_dag_id='00_sscrapear_crem',
        wait_for_completion=True,
    )

    # 2. Trigger para el DAG de Idealista (esperando a que termine)
    t_trigger_idealista = TriggerDagRunOperator(
        task_id='trigger_scrapear_idealista',
        trigger_dag_id='00_scrapear_idealista',
        wait_for_completion=True,
    )

    # 3. Trigger para el DAG de Entrenamiento de ML (esperando a que termine)
    t_trigger_ml = TriggerDagRunOperator(
        task_id='trigger_entrenamiento_ml',
        trigger_dag_id='00_entrenamiento_ml',
        wait_for_completion=True,
    )

    # Flujo: Los dos primeros se ejecutan en paralelo y después va el de ML
    [t_trigger_crem, t_trigger_idealista] >> t_trigger_ml
