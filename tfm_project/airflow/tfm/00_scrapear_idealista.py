"""
DAG: 00_scrapear_idealista
===========================
Pipeline dinámico de Idealista.
Cambios:
- Logs con Timestamp (ts_nodash) para desarrollo continuo.
- Tarea Silver por provincia integrada en TaskGroups.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# ============================================================
# CONFIGURACIÓN Y PARÁMETROS
# ============================================================

DAG_ID = '00_scrapear_idealista'

PROVINCIAS = [
    'murcia',
    #'alicante',
    #'valencia',
    #'almeria',
]

# Rutas dentro del contenedor
NOTEBOOKS_PATH = '/tfm/python_notebooks/idealista'
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

def papermill_cmd(notebook_name: str, provincia: str) -> str:
    """Genera el comando con Timestamp para no sobrescribir ejecuciones."""
    input_nb  = f'{NOTEBOOKS_PATH}/{notebook_name}.ipynb'
    # ts_nodash incluye YYYYMMDDTHHMMSS
    output_nb = f'{OUTPUT_PATH}/{notebook_name}_{provincia}_{{{{ ts_nodash }}}}.ipynb'

    return (
        f'mkdir -p {OUTPUT_PATH} && '
        f'papermill {input_nb} {output_nb} '
        f'-p provincia {provincia} '
    )

# ============================================================
# CUERPO DEL DAG PRINCIPAL
# ============================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Scraping e Idealista Silver por provincia.',
    schedule_interval=None,
    catchup=False,
    tags=['idealista', 'scraping', 'medallion', 'taskgroups', 'optimized'],
    max_active_tasks=2, 
    max_active_runs=1,
) as dag:

    # Lista para almacenar los TaskGroups de cada provincia
    task_groups = []
    
    for provincia in PROVINCIAS:
        
        with TaskGroup(group_id=f'provincia_{provincia}') as active_group:
            
            t_municipios = BashOperator(
                task_id='1_municipios',
                bash_command=papermill_cmd('idealista_01_municipios', provincia),
            )
            
            t_revision_ofertas = BashOperator(
                task_id='2_revision_ofertas',
                bash_command=papermill_cmd('idealista_02_revision_ofertas', provincia),
            )
            
            t_detalle_propiedades = BashOperator(
                task_id='3_detalle_propiedades',
                bash_command=papermill_cmd('idealista_03_detalle_propiedades', provincia),
            )
            
            t_silver_explore = BashOperator(
                task_id='4_silver_explore_info_fireworks_ai',
                bash_command=papermill_cmd('idealista_04_silver_explore_info_fireworks_ai', provincia),
            )
            
            # Flujo secuencial ordenado por sus nombres
            t_municipios >> t_revision_ofertas >> t_detalle_propiedades >> t_silver_explore
            
        task_groups.append(active_group)

    # Las provincias corren según el max_active_tasks (concurrencia controlada)
    # Al final, los TaskGroups simplemente mueren al terminar su última tarea interna.
    task_groups 
