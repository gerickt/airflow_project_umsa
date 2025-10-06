from datetime import datetime, timedelta
from airflow import DAG
from airflow .operators.python import PythonOperator
import pandas as pd
import json

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'pipeline_etl_ventas',
    default_args=default_args,
    description='Pipeline ETL para procesar datos de ventas',
    schedule='@daily',  # Se ejecuta todos los días a medianoche
    catchup=False,
)


def extraer_datos(**context):
    """
    Extrae datos de ventas (simulados)
    En un caso real, esto podría ser de una API o base de datos
    """
    print("Extrayendo datos de ventas...")
    # Datos simulados
    ventas = [
        {"producto": "Laptop", "cantidad": 5, "precio": 1200},
        {"producto": "Mouse", "cantidad": 15, "precio": 25},
        {"producto": "Teclado", "cantidad": 10, "precio": 75},
        {"producto": "Monitor", "cantidad": 7, "precio": 300},
    ]
    # Guardar en XCom para compartir con otras tareas
    context['ti'].xcom_push(key='datos_crudos', value=ventas)
    print(f"Extraídos {len(ventas)} registros")
    return ventas


def transformar_datos(**context):
    """
    Transforma y limpia los datos
    """
    print("Transformando datos...")
    # Obtener datos de la tarea anterior
    ventas = context['ti'].xcom_pull(key='datos_crudos', task_ids='extraer')
    # Convertir a DataFrame para facilitar el procesamiento
    df = pd.DataFrame(ventas)
    # Calcular el total por producto
    df['total'] = df['cantidad'] * df['precio']
    # Agregar fecha de procesamiento
    df['fecha_proceso'] = datetime.now().strftime('%Y-%m-%d')
    # Convertir de vuelta a diccionario
    datos_transformados = df.to_dict('records')
    # Guardar resultado transformado
    context['ti'].xcom_push(key='datos_transformados',
                            value=datos_transformados)
    print(f"Datos transformados: {len(datos_transformados)} registros")
    print(f"Total de ventas: ${df['total'].sum()}")
    return datos_transformados


def cargar_datos(**context):
    """
    Carga los datos procesados
    En un caso real, esto guardaría en una base de datos
    """
    print("Cargando datos...")
    # Obtener datos transformados
    datos = context['ti'].xcom_pull(
        key='datos_transformados', task_ids='transformar')
    # Simular guardado (en realidad guardaríamos en DB)
    archivo_salida = '/tmp/ventas_procesadas.json'
    with open(archivo_salida, 'w') as f:
        json.dump(datos, f, indent=2)
        print(f"Datos guardados en {archivo_salida}")
        print(f"Total de registros cargados: {len(datos)}")
    return archivo_salida


def generar_reporte(**context):
    """
    Genera un reporte resumen
    """
    print("Generando reporte...")
    datos = context['ti'].xcom_pull(
        key='datos_transformados', task_ids='transformar')
    df = pd.DataFrame(datos)
    reporte = f"""
    ========================================
    REPORTE DE VENTAS - {datetime.now().strftime('%Y-%m-%d')}
    ========================================
    Total de productos: {len(df)}
    Cantidad total vendida: {df['cantidad'].sum()}
    Ingresos totales: ${df['total'].sum():,.2f}
    Top 3 productos por ingresos:
    {df.nlargest(3, 'total')[['producto', 'total']].to_string(index=False)}
    ========================================
    """
    print(reporte)
    # Guardar reporte
    with open('/tmp/reporte_ventas.txt', 'w') as f:
        f.write(reporte)
    return "Reporte generado exitosamente"


# Crear las tareas
task_extraer = PythonOperator(
    task_id='extraer',
    python_callable=extraer_datos,
    dag=dag,
)
task_transformar = PythonOperator(
    task_id='transformar',
    python_callable=transformar_datos,
)
dag = dag,
task_cargar = PythonOperator(
    task_id='cargar',
    python_callable=cargar_datos,
    dag=dag,
)
task_reporte = PythonOperator(
    task_id='generar_reporte',
    python_callable=generar_reporte,
    dag=dag,
)
# Definir dependencias
task_extraer >> task_transformar >> [task_cargar, task_reporte]
# task_cargar y task_reporte se ejecutan en paralelo después de transformar
