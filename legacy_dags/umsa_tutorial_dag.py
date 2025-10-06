from datetime import datetime, timedelta
from airflow import DAG
from airflow .operators.python import PythonOperator
from airflow .operators.bash import BashOperator
# Definir argumentos por defecto
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Crear el DAG
dag = DAG(
    'umsa_tutorial',
    default_args=default_args,
    description='Un pipeline simple de ejemplo',
    schedule=timedelta(days=1),  # Se ejecuta diariamente
    catchup=False,  # No ejecutar fechas pasadas
)
# Función de Python para la primera tarea


def saludar():
    print("¡Hola! Este es mi primer pipeline de datos")
    return "Saludo completado"
# Función para procesar datos


def procesar_datos():
    print("Procesando datos...")
    datos = [1, 2, 3, 4, 5]
    resultado = sum(datos)
    print(f"La suma de los datos es: {resultado}")
    return resultado


# Crear tareas
tarea_1 = PythonOperator(
    task_id='saludar',
    python_callable=saludar,
    dag=dag,
)

tarea_2 = PythonOperator(
    task_id='procesar_datos',
    python_callable=procesar_datos,
    dag=dag,
)
tarea_3 = BashOperator(
    task_id='crear_archivo',
    bash_command='echo "Pipeline completado en $(date)" > /tmp/pipeline_log.txt',
    dag=dag,
)
