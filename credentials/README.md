# Credenciales de Google Cloud Platform

## Instrucciones para configurar BigQuery

### 1. Crear Service Account en Google Cloud

1. Ve a [Google Cloud Console](https://console.cloud.google.com)
2. Selecciona tu proyecto (o créalo)
3. Navega a: **IAM & Admin** → **Service Accounts**
4. Click en **+ CREATE SERVICE ACCOUNT**
5. Completa:
   - **Service account name**: `airflow-bigquery`
   - **Description**: `Service account para Airflow con acceso a BigQuery`
6. Click **CREATE AND CONTINUE**

### 2. Asignar permisos

Agrega los siguientes roles:
- **BigQuery Admin** (para administrar datasets y tablas)
- O roles más específicos:
  - **BigQuery Data Editor** (lectura/escritura)
  - **BigQuery Job User** (ejecutar queries)
  - **BigQuery Read Session User** (leer datos)

### 3. Crear y descargar la clave JSON

1. En la lista de Service Accounts, encuentra la que creaste
2. Click en **Actions** (⋮) → **Manage keys**
3. Click **ADD KEY** → **Create new key**
4. Selecciona **JSON** como tipo de clave
5. Click **CREATE** - se descargará automáticamente

### 4. Colocar el archivo de credenciales

**⚠️ IMPORTANTE: NO subir este archivo a Git**

1. Renombra el archivo descargado a: `gcp-service-account.json`
2. Cópialo a esta carpeta: `/home/gerick/Docker/airflow/credentials/`
3. Verifica que esté en `.gitignore`

```bash
# Renombrar y copiar
mv ~/Downloads/proyecto-123456-abcdef.json /home/gerick/Docker/airflow/credentials/gcp-service-account.json

# Verificar
ls -la /home/gerick/Docker/airflow/credentials/
```

### 5. Reconstruir la imagen de Airflow

```bash
cd /home/gerick/Docker/airflow
docker-compose down
docker-compose build
docker-compose up -d
```

### 6. Crear conexión en Airflow UI

1. Abre Airflow UI: http://localhost:8080
2. Ve a **Admin** → **Connections** → **+**
3. Completa:
   - **Connection Id**: `google_cloud_default`
   - **Connection Type**: `Google Cloud`
   - **Project Id**: Tu project ID de GCP (ej: `mi-proyecto-123456`)
   - **Keyfile Path**: `/opt/airflow/credentials/gcp-service-account.json`
4. Click **Test** → **Save**

## Estructura de archivos

```
credentials/
├── README.md                      # Este archivo
├── gcp-service-account.json       # Tu archivo de credenciales (NO incluir en Git)
└── .gitkeep                       # Para mantener la carpeta en Git
```

## Variables de entorno configuradas

El `docker-compose.yaml` ya está configurado con:

```yaml
environment:
  GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/credentials/gcp-service-account.json

volumes:
  - ./credentials:/opt/airflow/credentials:ro  # read-only por seguridad
```

## Ejemplo de uso en DAG

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": "SELECT * FROM `proyecto.dataset.tabla` LIMIT 100",
            "useLegacySql": False,
        }
    },
)
```

## Seguridad

⚠️ **NUNCA** commitear el archivo `gcp-service-account.json` a Git
⚠️ Agregar `credentials/*.json` al `.gitignore`
⚠️ Rotar credenciales periódicamente
⚠️ Usar permisos mínimos necesarios (principio de least privilege)
