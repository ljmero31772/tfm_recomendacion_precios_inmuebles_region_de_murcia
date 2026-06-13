"""
DAG: 00_sscrapear_crem
=======================
Orquestación de los notebooks de CREM.
Ejecuta en paralelo todos los notebooks crem_01 y al finalizar, ejecuta crem_02.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ============================================================
# CONFIGURACIÓN Y PARÁMETROS
# ============================================================

DAG_ID = '00_sscrapear_crem'

NOTEBOOKS_PATH = '/tfm/python_notebooks/crem'
OUTPUT_PATH = '/tfm/airflow/logs/notebooks'

# ============================================================
# ARGUMENTOS POR DEFECTO
# ============================================================

default_args = {
    'owner': 'admin',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

# ============================================================
# FUNCIÓN AUXILIAR: Comando papermill
# ============================================================

def papermill_cmd(notebook_name: str) -> str:
    """Genera el comando papermill sin parámetros de provincia."""
    input_nb  = f'{NOTEBOOKS_PATH}/{notebook_name}.ipynb'
    output_nb = f'{OUTPUT_PATH}/{notebook_name}_{{{{ ts_nodash }}}}.ipynb'

    return (
        f'mkdir -p {OUTPUT_PATH} && '
        f'papermill {input_nb} {output_nb}'
    )

# ============================================================
# CUERPO DEL DAG PRINCIPAL
# ============================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Orquestación de notebooks de CREM (Murcia).',
    schedule_interval=None,
    catchup=False,
    tags=['crem', 'medallion', 'optimized'],
    max_active_tasks=2,  # Concurrencia controlada para evitar sobrecargar recursos de Spark
    max_active_runs=1,
) as dag:

    crem_01_notebooks = [
        'crem_01_edad_media_poblacion',
        'crem_01_indice_gini_y_distribucion_renta_p80_p20',
        'crem_01_porcentaje_poblacion_65_o_mas',
        'crem_01_porcentaje_poblacion_espanola',
        'crem_01_porcentaje_poblacion_menos_de_18',
        'crem_01_renta_neta_y_bruta_media_por_persona',
        'crem_01_tamano_medio_hogar_y_pct_hogares_unipersonales',
        'crem_01_tamano_poblacion',
        'crem_01_transacciones_inmobiliarias',
    ]

    # Crear las tareas crem_01 de forma dinámica
    tasks_crem_01 = []
    for nb in crem_01_notebooks:
        t = BashOperator(
            task_id=nb,
            bash_command=papermill_cmd(nb),
        )
        tasks_crem_01.append(t)

    # Crear la tarea final crem_02
    t_crem_02 = BashOperator(
        task_id='crem_02_silver_full_data',
        bash_command=papermill_cmd('crem_02_silver_full_data'),
    )

    # Definir dependencias: todas las crem_01 terminan antes de lanzar crem_02
    tasks_crem_01 >> t_crem_02
