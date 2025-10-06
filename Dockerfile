# Extender la imagen oficial de Apache Airflow 3.1.0
FROM apache/airflow:3.1.0

# Cambiar a usuario root para instalar dependencias del sistema (si es necesario)
USER root

# Instalar dependencias del sistema si son necesarias
# Por ejemplo: herramientas de compilación, librerías, etc.
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     && apt-get clean

# Volver al usuario airflow (requerido para ejecutar Airflow)
USER airflow

# Copiar archivo de requirements para dependencias de Python
COPY requirements.txt /requirements.txt

# Instalar dependencias de Python
# - Providers de Airflow
# - Librerías para procesamiento de datos (pandas, openpyxl, pyarrow)
# - Librerías para bases de datos (psycopg2-binary)
RUN pip install --no-cache-dir -r /requirements.txt

# Verificar instalación de providers críticos
RUN airflow providers list | grep -E "postgres|standard"

# Metadata de la imagen
LABEL maintainer="data_engineer"
LABEL description="Apache Airflow 3.1.0 con providers PostgreSQL y librerías de análisis de datos"
LABEL version="1.0"
