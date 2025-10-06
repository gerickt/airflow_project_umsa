"""
DAG de ejemplo para BigQuery
Demuestra cómo ejecutar queries y transferir datos a BigQuery
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# CONFIGURACIÓN - Cambiar estos valores
GCP_PROJECT_ID = 'project_id_here'  # Tu Project ID de GCP
GCS_BUCKET = 'tu-bucket-airflow'     # ← Cambiar por tu bucket de GCS (si usas GCS)
BQ_DATASET = 'tiktok'      # Dataset de BigQuery
BQ_TABLE = 'silver_posts' # Tabla de BigQuery

dag = DAG(
    'ejemplo_bigquery',
    default_args=default_args,
    description='Ejemplo de integración con BigQuery',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['bigquery', 'ejemplo', 'gcp'],
)


# Tarea 1: Crear dataset en BigQuery (si no existe)
crear_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='crear_dataset_bigquery',
    dataset_id=BQ_DATASET,
    project_id=GCP_PROJECT_ID,
    location='US',  # O 'southamerica-east1' para Brasil (más cerca de Bolivia)
    gcp_conn_id='google_cloud_default',
    exists_ok=True,  # No falla si ya existe
    dag=dag,
)


# Tarea 2: Ejecutar query de ejemplo
ejecutar_query = BigQueryInsertJobOperator(
    task_id='ejecutar_query_ejemplo',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}` AS
                SELECT
                    CURRENT_DATE() as fecha,
                    'Ejemplo' as tipo_dato,
                    RAND() * 100 as valor
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)


# Tarea 3: Verificar que la tabla existe
verificar_tabla = BigQueryCheckOperator(
    task_id='verificar_tabla_existe',
    gcp_conn_id='google_cloud_default',
    sql=f"""
        SELECT COUNT(*)
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    """,
    use_legacy_sql=False,
    dag=dag,
)


# Tarea 4: Query de lectura (ejemplo)
leer_datos = BigQueryInsertJobOperator(
    task_id='leer_datos_bigquery',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": f"""
                SELECT *
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
                LIMIT 10
            """,
            "useLegacySql": False,
        }
    },
    dag=dag,
)


# Definir dependencias
crear_dataset >> ejecutar_query >> verificar_tabla >> leer_datos


# ============================================================================
# EJEMPLO AVANZADO: Cargar Parquet a BigQuery vía GCS
# ============================================================================
#
# Descomentar para usar (requiere configurar GCS bucket)
#
# # Paso 1: Subir archivo Parquet a Google Cloud Storage
# subir_a_gcs = LocalFilesystemToGCSOperator(
#     task_id='subir_parquet_a_gcs',
#     src='/opt/airflow/output/social_listening_usdt_*.parquet',
#     dst=f'data/tiktok_usdt/archivo.parquet',
#     bucket=GCS_BUCKET,
#     gcp_conn_id='google_cloud_default',
#     dag=dag,
# )
#
# # Paso 2: Cargar desde GCS a BigQuery
# cargar_a_bigquery = GCSToBigQueryOperator(
#     task_id='cargar_gcs_a_bigquery',
#     bucket=GCS_BUCKET,
#     source_objects=['data/tiktok_usdt/archivo.parquet'],
#     destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}',
#     source_format='PARQUET',
#     write_disposition='WRITE_TRUNCATE',  # Sobrescribir tabla
#     autodetect=True,  # Auto-detectar schema
#     gcp_conn_id='google_cloud_default',
#     dag=dag,
# )
#
# leer_datos >> subir_a_gcs >> cargar_a_bigquery
