"""
DAG: 00_entrenamiento_ml
========================
Orquestación de los notebooks de ML (salvo ml_05_entrenamiento_redes).
Ejecuta secuencialmente los notebooks ml_01 a ml_04.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ============================================================
# CONFIGURACIÓN Y PARÁMETROS
# ============================================================

DAG_ID = '00_entrenamiento_ml'

NOTEBOOKS_PATH = '/tfm/python_notebooks/ml'
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
    """Genera el comando papermill sin parámetros."""
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
    description='Orquestación de notebooks de Machine Learning (Murcia).',
    schedule_interval=None,
    catchup=False,
    tags=['ml', 'training', 'optimized'],
    max_active_tasks=1,  # Ejecución secuencial controlada para no saturar recursos de Spark
    max_active_runs=1,
) as dag:

    # 1. Preparación del dataset
    t_create_dataset = BashOperator(
        task_id='ml_01_create_dataset',
        bash_command=papermill_cmd('ml_01_create_dataset'),
    )

    # 2. Reducción de variables y normalización
    t_reduccion_variables = BashOperator(
        task_id='ml_02_reduccion_de_variables_y_normalizacion',
        bash_command=papermill_cmd('ml_02_reduccion_de_variables_y_normalizacion'),
    )

    # 3. Pequeño análisis EDA
    t_pequeno_analisis = BashOperator(
        task_id='ml_03_pequno_analisis_eda',
        bash_command=papermill_cmd('ml_03_pequno_analisis_eda'),
    )

    # 4. Entrenamiento de modelos
    t_entrenamiento = BashOperator(
        task_id='ml_04_entrenamiento',
        bash_command=papermill_cmd('ml_04_entrenamiento'),
    )

    # Definir dependencias secuenciales
    t_create_dataset >> t_reduccion_variables >> t_pequeno_analisis >> t_entrenamiento
