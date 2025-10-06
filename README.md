# Airflow Pipeline para Análisis de Social Listening vs Tipo de Cambio USDT/BOB

## Descripción General

Este proyecto académico implementa un pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow para analizar la relación entre el tipo de cambio USDT/BOB y la actividad en redes sociales, específicamente en TikTok, en el contexto económico de Bolivia. El pipeline extrae datos de TikTok desde BigQuery y los consolida con datos de tipo de cambio USDT/BOB para el tercer trimestre de 2025.

## Objetivo del Proyecto

El objetivo principal es evaluar si existe alguna correlación entre las fluctuaciones del tipo de cambio USDT/BOB y la actividad en redes sociales relacionada con temas económicos y políticos en Bolivia. El análisis se centra en el Q3 2025 (julio-septiembre), utilizando datos de TikTok como indicador de opinión pública y datos de tipo de cambio para evaluar posibles correlaciones.

## Grupo de Desarrollo

Este proyecto académico fue desarrollado por:

- CAMILA NATALIA PELAEZ SALAZAR
- CECILIA ELSA CHOQUE BERDEJA
- GERICK TORO RODRIGUEZ


## Componentes del Sistema

### 1. Apache Airflow
- **Versión**: 3.1.0
- **Executor**: CeleryExecutor
- **Autenticación**: FabAuthManager
- **Configuración**: Basada en el archivo `config/airflow.cfg`

### 2. Docker y Docker Compose
- **Entorno**: Contenedorizado para desarrollo y pruebas
- **Servicios**:
  - `postgres`: Base de datos para metadata de Airflow
  - `redis`: Broker de mensajes para Celery
  - `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-worker`, `airflow-triggerer`: Componentes principales de Airflow
  - `airflow-init`: Inicialización del entorno

### 3. Google Cloud Platform (BigQuery)
- **Proyecto**: `project_id_here`
- **Dataset**: `tiktok`
- **Tabla**: `silver_posts`
- **Autenticación**: Service Account con archivo JSON

## Configuración del Entorno

### Requisitos Previos
- Docker y Docker Compose
- Credenciales de Google Cloud Platform para BigQuery
- Archivo de credenciales `gcp-service-account.json` en la carpeta `credentials/`

### Variables de Entorno
El archivo `.env.sample` define:
- `AIRFLOW_UID`: ID de usuario para ejecución de Airflow (1000)

### Dependencias
Las dependencias se especifican en `requirements.txt`:
- `apache-airflow-providers-postgres`: Conexiones y operadores para PostgreSQL
- `apache-airflow-providers-google`: Conexiones y operadores para Google Cloud
- `pandas`, `numpy`: Procesamiento de datos
- `openpyxl`: Lectura de archivos Excel
- `pyarrow`: Formato Parquet
- `psycopg2-binary`: Adaptador PostgreSQL

## Pipeline Principal

### DAG: `pipeline_social_listening_usdt_bigquery`

El pipeline principal, definido en `dags/pipeline_social_listening_usdt_v3_bigquery.py`, consta de las siguientes tareas:

1. **Extracción de datos TikTok desde BigQuery**
   - Extrae datos del dataset `project_id_here.tiktok.silver_posts`
   - Filtra datos del Q3 2025 (julio-septiembre)
   - Convierte horarios de UTC a GMT-4 (Bolivia)

2. **Extracción de datos USDT/BOB**
   - Lee datos de tipo de cambio desde archivo Excel `usdt_Q3.xlsx`
   - Datos en horario GMT-4

3. **Transformación y consolidación avanzada**
   - Aplica filtrado semántico usando keywords económicas/políticas
   - Realiza agregaciones para métricas generales y económicas
   - Combina datos de TikTok con datos de tipo de cambio

4. **Cálculo de correlaciones**
   - Calcula correlaciones de Pearson entre precio USDT/BOB y métricas de TikTok
   - Realiza análisis de correlación general, económico y filtrado

5. **Carga de datos en formato Parquet**
   - Guarda datos consolidados en formato Parquet

6. **Generación de reporte ejecutivo**
   - Genera reporte con análisis de tendencia del precio
   - Muestra estadísticas de actividad en TikTok
   - Presenta análisis de correlación de Pearson
   - Incluye hallazgos clave y archivos generados

## Análisis de Correlación

El pipeline implementa un análisis de correlación de Pearson en tres niveles:

1. **Correlaciones Generales**: Entre el precio USDT/BOB y todas las métricas de TikTok
2. **Correlaciones Económicas**: Entre el precio USDT/BOB y métricas de posts filtrados como económicos/políticos
3. **Correlaciones Filtradas**: Solo en días donde hubo actividad económica en TikTok

## Resultados y Archivos Generados

El pipeline genera los siguientes archivos en la carpeta `/opt/airflow/output/`:

- Archivo Parquet con datos consolidados
- Reporte ejecutivo en formato de texto
- Archivo JSON con correlaciones

## Seguridad y Buenas Prácticas

- El archivo de credenciales de GCP no se incluye en el control de versiones
- Se utiliza acceso read-only para el directorio de credenciales
- Se implementa el principio de mínimo privilegio para las credenciales de GCP

## Consideraciones Académicas

Este proyecto forma parte de un ejercicio académico de la materia de **Adquisión y Comprensión de Datos** y demuestra la implementación de un pipeline ETL completo para análisis de datos. El enfoque en el análisis de social listening en relación con temas económicos en Bolivia es particularmente relevante para comprender las percepciones públicas en el contexto del tipo de cambio.

## Instrucciones de Ejecución

Para ejecutar el proyecto:

1. Asegúrate de tener Docker y Docker Compose instalados
2. Configura las credenciales de GCP en la carpeta `credentials/`
3. Ejecuta:
   ```bash
   docker-compose build
   docker-compose up -d
   ```
4. Accede a la UI de Airflow en `http://localhost:8080`
5. Activa y ejecuta el DAG `pipeline_social_listening_usdt_bigquery`

## Tecnologías Utilizadas

- **Airflow**: Orquestación de pipelines
- **BigQuery**: Almacenamiento y procesamiento de datos
- **Python**: Lógica de procesamiento
- **Docker**: Contenerización
- **PostgreSQL**: Base de datos para metadata
- **Redis**: Broker de mensajes
- **Pandas**: Procesamiento de datos
- **PyArrow**: Manipulación de datos en formato Parquet