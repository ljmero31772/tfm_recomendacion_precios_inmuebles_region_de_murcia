"""
tfm_lib/utils.py
Utilidades genéricas de Spark y Delta Lake con soporte para particionado dinámico.
Optimizado para el entorno Docker de TFM.
"""

import sys
import contextlib
import os
from typing import List, Optional, Dict
from loguru import logger as log
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DataType, DateType, NumericType, TimestampType, StringType
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from deltalake import DeltaTable, write_deltalake
from delta.tables import DeltaTable as DT
import unicodedata
import re



DELTA_BASE_PATH = "/tfm/delta_lake"
SPARK_WAREHOUSE = "/tfm/spark-warehouse"
ENV = os.getenv("ENV", "local")


def get_spark_session(app_name: str = "Idealista_TFM", env_vars_to_import:List[str]=None) -> SparkSession:
    """
    SparkSession configurada para Delta Lake local.
    Reutiliza la sesión existente SOLO si tiene Delta Lake correctamente configurado.
    Si no, la detiene y crea una nueva con configure_spark_with_delta_pip.
    Optimizada para trabajos de larga duración (hasta 1 hora).
    """
    # Reutilizar la sesión existente SOLO si Delta Lake está disponible en ella
    existing = SparkSession.getActiveSession()
    if existing is not None:
        try:
            # Prueba mínima: intentar leer el catálogo Delta
            existing.conf.get("spark.sql.extensions")
            ext = existing.conf.get("spark.sql.extensions", "")
            if "DeltaSparkSessionExtension" in ext:
                existing.sparkContext.setLogLevel("ERROR")
                return existing
        except Exception:
            pass
        # La sesión existente no tiene Delta → pararla para crear una limpia
        existing.stop()
    
    # Configuración mejorada para trabajos de larga duración
    builder = (
        SparkSession.builder
        .appName(f"{app_name}_{ENV}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE)
        # --- MEMORIA: aumentada para trabajos pesados de scraping ---
        # Desglose aprox: Airflow ~1GB + Jupyter ~0.5GB + Spark driver 2GB + overhead = ~4.5GB
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.3")
        # --- TIMEOUTS PARA TRABAJOS LARGOS (hasta 2 hora) ---
        .config("spark.network.timeout", "7200s")
        .config("spark.executor.heartbeatInterval", "60s")  # Heartbeat más frecuente
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Mejora pandas interop
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # --- OPTIMIZACIONES PARA MÁQUINAS LOCALES ---
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.autoBroadcastJoinThreshold", "10m")  # Evita broadcasts grandes
        .config("spark.databricks.delta.optimize.maxFileSize", "33554432")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Modo de sobreescritura dinámica: solo sobrescribe las particiones procesadas
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.microsoft.delta.merge.lowShuffle.enabled", "true")
    )
    
    #Añadimos las variables de sistema deseadas
    if env_vars_to_import and len(env_vars_to_import)>0:
        for env_var in env_vars_to_import:
            builder=builder.config(f"spark.executorEnv.{env_var}", f"{os.environ.get('env_var')}")


    # Usamos redirección de bajo nivel (File Descriptors) para silenciar el chatter de la JVM/Ivy
    # que contextlib.redirect_stderr no puede atrapar.
    devnull = os.open(os.devnull, os.O_WRONLY)
    old_stdout_fd = os.dup(1)
    old_stderr_fd = os.dup(2)
    
    try:
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    finally:
        os.dup2(old_stdout_fd, 1)
        os.dup2(old_stderr_fd, 2)
        os.close(devnull)
        os.close(old_stdout_fd)
        os.close(old_stderr_fd)
            
    # Reducimos logs de la sesión al mínimo para no ocultar trazas de Python
    spark.sparkContext.setLogLevel("ERROR") 
    return spark


def display(df: DataFrame, n: int = 10) -> None:
    """
    Sirve para hacer un display de un dataFrame de PySpark, de forma que veamos de 
    forma estructurada las primeras n filas del dataFrame y no como en un show().
    """
    return df.limit(n).toPandas().head(n)



def normalize_column_name(name: str) -> str:
    """
    Normaliza un nombre de columna:
    - Pasa a minúsculas
    - Elimina acentos
    - Reemplaza espacios y símbolos por guiones bajos
    - Elimina cualquier carácter no alfanumérico (excepto _)
    """
    starts_with_underscore = name.startswith('_')
    # Pasa a minúsculas
    name = name.lower()
    # Elimina acentos
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    # Reemplaza espacios y símbolos comunes por "_"
    name = re.sub(r'[^\w]+', '_', name)
    # Quita guiones bajos al principio y final
    name = name.strip('_')
    
    if starts_with_underscore: name="_"+name

    return name


def normalize_df(df_to_normalize: DataFrame):
    for col in df_to_normalize.columns:
        df_to_normalize=df_to_normalize.withColumnRenamed(col, normalize_column_name(col))

    columns=df_to_normalize.columns

    sorted_columns=sorted(columns, key=lambda x: (x.startswith("_"), x.lower()))

    df_to_normalize=df_to_normalize.select(sorted_columns)

    return df_to_normalize


def normalize_df_pandas(df_to_normalize):
    # Renombrado de columnas
    df_to_normalize = df_to_normalize.rename(
        columns={col: normalize_column_name(col) for col in df_to_normalize.columns}
    )

    # Reordenado de las columnas
    columns = df_to_normalize.columns.tolist()
    sorted_columns = sorted(columns, key=lambda x: (x.startswith("_"), x.lower()))
    df_to_normalize = df_to_normalize[sorted_columns]

    return df_to_normalize



def write_pandas_df_in_delta(df, table_path, partition_cols, mode="append"):
    # Normalizar los nombres de las columnas de partición igual que normalize_df_pandas
    # para garantizar que coincidan con los nombres reales del DataFrame
    partition_cols_norm = [normalize_column_name(c) for c in partition_cols]

    # Validar que todas las columnas existen antes de operar
    missing = [c for c in partition_cols_norm if c not in df.columns]
    if missing:
        raise ValueError(
            f"Columnas de partición no encontradas en el DataFrame: {missing}\n"
            f"Columnas disponibles: {df.columns.tolist()}"
        )

    # Identificar las combinaciones de partición afectadas
    affected_partitions = (
        df[partition_cols_norm]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    try:
        # Si la tabla existe, eliminamos solo las particiones afectadas.
        # dt.delete() requiere un predicado SQL string (API de deltalake/Rust),
        # NO un dict. Ejemplo: "provincia = 'Madrid' AND municipio = 'Alcala'"
        dt = DeltaTable(table_path)
        for part in affected_partitions:
            predicate = " AND ".join(
                f"{col} = '{val}'" for col, val in part.items()
            )
            log.debug(f"Eliminando partición: {predicate}")
            dt.delete(predicate)
    except Exception as e:
        # La tabla aún no existe → se creará en el write_deltalake
        log.debug(f"No se eliminaron particiones previas (tabla nueva o error): {e}")

    # Escribir los datos en la tabla Delta
    write_deltalake(
        table_path,
        df,
        mode="append",
        partition_by=partition_cols_norm
    )

    print(f"Tabla guardada en {table_path}. Particiones actualizadas: {affected_partitions}")



def full_table_path(table_name:str):
        if len(table_name.split("."))==3:
            layer=table_name.split(".")[0]
            project=table_name.split(".")[1]
            table_name=table_name.split(".")[2]
        
        else:
            raise Exception(f"Nombre de la tabla erroneo: {table_name}")
        
        full_path="/".join([DELTA_BASE_PATH,layer,project,table_name]).lower()

        return full_path


def read_from_delta(
    spark: SparkSession,
    table_name: str
) -> DataFrame:
    """
    Lee una tabla Delta. Si la tabla no existe, devuelve un DataFrame vacío en lugar de romper.
    """
    table_path = full_table_path(table_name)
    df = spark.read.format("delta").load(table_path)
    df=normalize_df(df)

    return df


def get_delta_table(
    spark: SparkSession,
    table_name: str
) -> DT:
    """
    Lee una tabla Delta. Si la tabla no existe, devuelve un DataFrame vacío en lugar de romper.
    """
    table_path = full_table_path(table_name)
    dt=DT.forPath(spark, table_path)

    return dt


def write_deltalake_pandas_df(df, table_name, mode="append"):
    table_path=full_table_path(table_name)

    write_deltalake(
        table_path,
        df,
        mode=mode
    )

    print(f"Datos guardados en {table_path} con mode {mode}")



def update_delta_table(spark, table_name, condition, set):
    """
    Actualiza el metadato _scraped de la tabla given_table para el municipio y provincia dados.
    """
    delta_table=get_delta_table(spark=spark, table_name=table_name)
    delta_table.update(
        condition = condition,
        set = set
    )

    print(f"Tabla {table_name} actualizada correctamente")