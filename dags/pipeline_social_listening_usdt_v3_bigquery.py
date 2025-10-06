"""
Pipeline ETL v3: Social Listening TikTok (BigQuery) vs USDT/BOB - Q3 2025
Extrae datos de TikTok desde BigQuery y los consolida con tipo de cambio USDT/BOB
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd
import numpy as np
import os
import json

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2025, 7, 1),  # Inicio Q3 2025
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_social_listening_usdt_bigquery',
    default_args=default_args,
    description='Pipeline ETL: Análisis Social Listening TikTok (BigQuery) vs USDT/BOB - Q3 2025',
    schedule='@monthly',
    catchup=False,
    tags=['bigquery', 'social-listening', 'q3-2025'],
)

# CONFIGURACIÓN
GCP_PROJECT_ID = 'project_id_here'
BQ_DATASET = 'tiktok'
BQ_TABLE = 'silver_posts'

# Keywords económicas/políticas para filtrado semántico
KEYWORDS_ECONOMICAS = [
    'dolar', 'dólar', 'economia', 'economía', 'inflacion', 'inflación',
    'bolivia', 'elecciones', 'politica', 'política', 'crisis', 'banco central',
    'tipo de cambio', 'reservas', 'financiero', 'macroeconomia', 'gobierno',
    'usdt', 'cripto', 'tether', 'boliviano', 'bob', 'divisas', 'cambio', 'paralelo'
]


def extraer_tiktok_bigquery(**context):
    """
    Extrae datos de TikTok desde BigQuery (Q3 2025: Julio-Septiembre)
    Convierte de UTC a GMT-4 (Bolivia)
    """
    print("=" * 60)
    print("1. Extrayendo datos de TikTok desde BigQuery...")
    print("=" * 60)

    # Usar credenciales directamente desde archivo (evita problemas de OAuth)
    from google.cloud import bigquery
    from google.oauth2 import service_account

    credentials = service_account.Credentials.from_service_account_file(
        '/opt/airflow/credentials/gcp-service-account.json',
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials
    )

    # Query para Q3 2025 (Julio 1 - Septiembre 30, 2025)
    # create_time está en UTC, convertimos a GMT-4 (Bolivia)
    query = f"""
        SELECT
            index_time,
            create_time,
            DATETIME(create_time, 'America/La_Paz') as create_time_bolivia,  -- UTC a GMT-4
            post_id,
            post_ad,
            post_pin,
            author_id,
            author_name,
            author_username,
            description,
            stickers,
            hashtags,
            play_count,
            like_count,
            comment_count,
            share_count,
            collect_count,
            video_id,
            video_cover,
            video_dynamic_cover,
            audio_id,
            texto,
            row_num,
            url
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE
            -- Filtrar Q3 2025 en hora de Bolivia (GMT-4)
            DATETIME(create_time, 'America/La_Paz') >= '2025-07-01 00:00:00'
            AND DATETIME(create_time, 'America/La_Paz') < '2025-10-01 00:00:00'
        ORDER BY create_time
    """

    print(f"Ejecutando query en BigQuery...")
    print(f"Proyecto: {GCP_PROJECT_ID}")
    print(f"Dataset: {BQ_DATASET}")
    print(f"Tabla: {BQ_TABLE}")
    print(f"Periodo: Q3 2025 (Julio-Septiembre, GMT-4)")

    # Ejecutar query usando el cliente de BigQuery directamente
    query_job = client.query(query)
    df_tiktok = query_job.to_dataframe()

    print(f"\n✓ Total de posts extraídos: {len(df_tiktok):,}")
    print(f"✓ Columnas disponibles: {len(df_tiktok.columns)}")

    if len(df_tiktok) > 0:
        print(f"✓ Rango temporal (Bolivia GMT-4):")
        print(f"  - Inicio: {df_tiktok['create_time_bolivia'].min()}")
        print(f"  - Fin:    {df_tiktok['create_time_bolivia'].max()}")
        print(f"✓ Rango temporal (UTC original):")
        print(f"  - Inicio: {df_tiktok['create_time'].min()}")
        print(f"  - Fin:    {df_tiktok['create_time'].max()}")
    else:
        print("⚠️ ADVERTENCIA: No se encontraron datos en el rango especificado")

    # Guardar en archivo temporal Parquet
    temp_path = '/tmp/tiktok_bigquery_q3.parquet'
    df_tiktok.to_parquet(temp_path, engine='pyarrow', index=False)

    # Pasar solo la ruta por XCom
    context['ti'].xcom_push(key='tiktok_path', value=temp_path)

    print(f"✓ Datos guardados en: {temp_path}")
    return len(df_tiktok)


def extraer_usdt_q3(**context):
    """
    Extrae datos de tipo de cambio USDT/BOB Q3 2025 desde Excel (GMT-4)
    """
    print("=" * 60)
    print("2. Extrayendo datos de USDT/BOB Q3 2025...")
    print("=" * 60)

    df_usdt = pd.read_excel('/opt/airflow/data/usdt_Q3.xlsx')

    print(f"✓ Registros de tipo de cambio: {len(df_usdt)}")
    print(f"✓ Columnas: {df_usdt.columns.tolist()}")

    if len(df_usdt) > 0:
        print(f"✓ Rango de fechas (GMT-4): {df_usdt['Fecha'].min()} a {df_usdt['Fecha'].max()}")

    # Guardar en archivo temporal
    temp_path = '/tmp/usdt_q3.parquet'
    df_usdt.to_parquet(temp_path, engine='pyarrow', index=False)

    # Pasar solo la ruta por XCom
    context['ti'].xcom_push(key='usdt_path', value=temp_path)

    print(f"✓ Datos guardados en: {temp_path}")
    return len(df_usdt)


def transformar_consolidar_avanzado(**context):
    """
    Transformación avanzada con filtrado semántico y agregaciones duales
    """
    print("=" * 60)
    print("3. Transformación Avanzada: Filtrado + Agregaciones Duales")
    print("=" * 60)

    # Obtener rutas de archivos temporales
    tiktok_path = context['ti'].xcom_pull(key='tiktok_path', task_ids='extraer_tiktok_bigquery')
    usdt_path = context['ti'].xcom_pull(key='usdt_path', task_ids='extraer_usdt_q3')

    # Leer desde archivos temporales
    df_tiktok = pd.read_parquet(tiktok_path)
    df_usdt = pd.read_parquet(usdt_path)

    # === TRANSFORMACIÓN TIKTOK ===
    print("\n[A] Procesando datos de TikTok...")

    # Usar create_time_bolivia (ya convertido a GMT-4 en BigQuery)
    df_tiktok['create_time_bolivia'] = pd.to_datetime(df_tiktok['create_time_bolivia'])
    df_tiktok['fecha'] = df_tiktok['create_time_bolivia'].dt.date

    # Convertir métricas a numéricas (BigQuery ya las trae como INT pero por si acaso)
    metricas = ['play_count', 'like_count', 'comment_count', 'share_count', 'collect_count']
    for metrica in metricas:
        df_tiktok[metrica] = pd.to_numeric(df_tiktok[metrica], errors='coerce').fillna(0)

    # --- FILTRADO SEMÁNTICO POR KEYWORDS ---
    print(f"\n[B] Aplicando filtrado semántico ({len(KEYWORDS_ECONOMICAS)} keywords)...")

    keyword_regex = '|'.join(KEYWORDS_ECONOMICAS)

    # Crear campo de texto combinado para búsqueda
    texto_cols = []
    for col in ['hashtags', 'description', 'texto', 'stickers']:
        if col in df_tiktok.columns:
            texto_cols.append(df_tiktok[col].astype(str))

    if texto_cols:
        df_tiktok['texto_analisis'] = pd.concat(texto_cols, axis=1).fillna('').agg(' '.join, axis=1).str.lower()
        df_tiktok['is_economia'] = df_tiktok['texto_analisis'].str.contains(keyword_regex, na=False, case=False)
    else:
        df_tiktok['is_economia'] = False
        print("  ⚠ ADVERTENCIA: No se encontraron campos de texto para filtrar")

    posts_economicos = df_tiktok['is_economia'].sum()
    print(f"  ✓ Posts filtrados como económicos/políticos: {posts_economicos:,} ({posts_economicos/len(df_tiktok)*100:.1f}%)")

    # --- AGREGACIÓN 1: MÉTRICAS GENERALES (todos los posts) ---
    print("\n[C] Agregando métricas GENERALES por día...")

    df_tiktok_general = df_tiktok.groupby('fecha').agg({
        'post_id': 'count',
        'play_count': 'sum',
        'like_count': 'sum',
        'comment_count': 'sum',
        'share_count': 'sum',
        'collect_count': 'sum',
    }).reset_index()

    df_tiktok_general.columns = [
        'fecha', 'total_posts', 'total_views', 'total_likes',
        'total_comments', 'total_shares', 'total_collects'
    ]

    # Calcular engagement total
    df_tiktok_general['total_engagement'] = (
        df_tiktok_general['total_likes'] +
        df_tiktok_general['total_comments'] * 2 +
        df_tiktok_general['total_shares'] * 3 +
        df_tiktok_general['total_collects'] * 2
    )

    print(f"  ✓ Días con posts (general): {len(df_tiktok_general)}")

    # --- AGREGACIÓN 2: MÉTRICAS ECONÓMICAS (solo posts filtrados) ---
    print("\n[D] Agregando métricas ECONÓMICAS por día...")

    df_tiktok_eco = df_tiktok[df_tiktok['is_economia']].copy()

    if len(df_tiktok_eco) > 0:
        df_tiktok_eco_agg = df_tiktok_eco.groupby('fecha').agg({
            'post_id': 'count',
            'play_count': 'sum',
            'like_count': 'sum',
            'comment_count': 'sum',
            'share_count': 'sum',
            'collect_count': 'sum',
        }).reset_index()

        df_tiktok_eco_agg.columns = [
            'fecha', 'eco_posts', 'eco_views', 'eco_likes',
            'eco_comments', 'eco_shares', 'eco_collects'
        ]

        # Calcular engagement económico
        df_tiktok_eco_agg['eco_engagement'] = (
            df_tiktok_eco_agg['eco_likes'] +
            df_tiktok_eco_agg['eco_comments'] * 2 +
            df_tiktok_eco_agg['eco_shares'] * 3 +
            df_tiktok_eco_agg['eco_collects'] * 2
        )

        print(f"  ✓ Días con posts económicos: {len(df_tiktok_eco_agg)}")
    else:
        df_tiktok_eco_agg = pd.DataFrame(columns=[
            'fecha', 'eco_posts', 'eco_views', 'eco_likes',
            'eco_comments', 'eco_shares', 'eco_collects', 'eco_engagement'
        ])
        print("  ⚠ No se encontraron posts económicos")

    # === TRANSFORMACIÓN USDT ===
    print("\n[E] Procesando datos de USDT/BOB Q3...")

    df_usdt['Fecha'] = pd.to_datetime(df_usdt['Fecha']).dt.date
    df_usdt['PrecioCompra'] = pd.to_numeric(df_usdt['PrecioCompra'], errors='coerce')
    df_usdt['PrecioVenta'] = pd.to_numeric(df_usdt['PrecioVenta'], errors='coerce')
    df_usdt['precio_promedio'] = (df_usdt['PrecioCompra'] + df_usdt['PrecioVenta']) / 2

    print(f"  ✓ Registros de precio procesados: {len(df_usdt)}")

    # === CONSOLIDACIÓN ===
    print("\n[F] Consolidando datasets...")

    # Join 1: Datos generales
    df_consolidado = pd.merge(
        df_usdt[['Fecha', 'PrecioCompra', 'PrecioVenta', 'precio_promedio']],
        df_tiktok_general,
        left_on='Fecha',
        right_on='fecha',
        how='outer'
    )

    # Join 2: Datos económicos
    df_consolidado = pd.merge(
        df_consolidado,
        df_tiktok_eco_agg,
        on='fecha',
        how='left'
    )

    # Limpiar y renombrar
    df_consolidado['fecha'] = df_consolidado['fecha'].fillna(df_consolidado['Fecha'])
    df_consolidado = df_consolidado.drop(columns=['Fecha'])

    df_consolidado.rename(columns={
        'PrecioCompra': 'usdt_bob_compra',
        'PrecioVenta': 'usdt_bob_venta',
        'precio_promedio': 'usdt_bob_promedio'
    }, inplace=True)

    # Rellenar NaN
    metricas_cols = [
        'total_posts', 'total_views', 'total_likes', 'total_comments',
        'total_shares', 'total_collects', 'total_engagement',
        'eco_posts', 'eco_views', 'eco_likes', 'eco_comments',
        'eco_shares', 'eco_collects', 'eco_engagement'
    ]
    df_consolidado[metricas_cols] = df_consolidado[metricas_cols].fillna(0)

    # Ordenar por fecha
    df_consolidado = df_consolidado.sort_values('fecha').reset_index(drop=True)

    print(f"\n✓ CONSOLIDACIÓN COMPLETA:")
    print(f"  - Registros totales: {len(df_consolidado)}")
    print(f"  - Rango de fechas: {df_consolidado['fecha'].min()} a {df_consolidado['fecha'].max()}")
    print(f"  - Días con posts generales: {(df_consolidado['total_posts'] > 0).sum()}")
    print(f"  - Días con posts económicos: {(df_consolidado['eco_posts'] > 0).sum()}")

    # Guardar en archivo temporal
    temp_path = '/tmp/datos_consolidados_q3_bq.parquet'
    df_consolidado.to_parquet(temp_path, engine='pyarrow', index=False)

    # Pasar solo la ruta por XCom
    context['ti'].xcom_push(key='consolidado_path', value=temp_path)

    print(f"✓ Datos consolidados guardados en: {temp_path}")
    return len(df_consolidado)


def calcular_correlaciones(**context):
    """
    Calcula correlaciones de Pearson entre precio USDT/BOB y métricas de TikTok
    """
    print("=" * 60)
    print("4. Calculando Correlaciones de Pearson")
    print("=" * 60)

    consolidado_path = context['ti'].xcom_pull(key='consolidado_path', task_ids='transformar_consolidar_avanzado')
    df = pd.read_parquet(consolidado_path)

    # Verificar que haya datos
    if df.empty or len(df) < 2:
        print("⚠ Datos insuficientes para calcular correlaciones")
        context['ti'].xcom_push(key='correlaciones', value={})
        return "Correlaciones no calculadas"

    correlaciones = {}

    # === CORRELACIONES GENERALES ===
    print("\n[A] Correlaciones GENERALES (precio vs toda la actividad):")

    try:
        corr_total_views = df['usdt_bob_promedio'].corr(df['total_views'])
        corr_total_likes = df['usdt_bob_promedio'].corr(df['total_likes'])
        corr_total_shares = df['usdt_bob_promedio'].corr(df['total_shares'])
        corr_total_engagement = df['usdt_bob_promedio'].corr(df['total_engagement'])

        correlaciones['general'] = {
            'precio_vs_total_views': float(corr_total_views) if not pd.isna(corr_total_views) else None,
            'precio_vs_total_likes': float(corr_total_likes) if not pd.isna(corr_total_likes) else None,
            'precio_vs_total_shares': float(corr_total_shares) if not pd.isna(corr_total_shares) else None,
            'precio_vs_total_engagement': float(corr_total_engagement) if not pd.isna(corr_total_engagement) else None,
        }

        print(f"  Precio vs Total Views:      {corr_total_views:.4f}" if not pd.isna(corr_total_views) else "  N/A")
        print(f"  Precio vs Total Likes:      {corr_total_likes:.4f}" if not pd.isna(corr_total_likes) else "  N/A")
        print(f"  Precio vs Total Shares:     {corr_total_shares:.4f}" if not pd.isna(corr_total_shares) else "  N/A")
        print(f"  Precio vs Total Engagement: {corr_total_engagement:.4f}" if not pd.isna(corr_total_engagement) else "  N/A")

    except Exception as e:
        print(f"  ⚠ Error calculando correlaciones generales: {e}")
        correlaciones['general'] = {}

    # === CORRELACIONES ECONÓMICAS ===
    print("\n[B] Correlaciones ECONÓMICAS (precio vs posts económicos):")

    total_eco_posts = df['eco_posts'].sum()

    if total_eco_posts > 0:
        try:
            corr_eco_views = df['usdt_bob_promedio'].corr(df['eco_views'])
            corr_eco_likes = df['usdt_bob_promedio'].corr(df['eco_likes'])
            corr_eco_shares = df['usdt_bob_promedio'].corr(df['eco_shares'])
            corr_eco_engagement = df['usdt_bob_promedio'].corr(df['eco_engagement'])

            correlaciones['economico'] = {
                'precio_vs_eco_views': float(corr_eco_views) if not pd.isna(corr_eco_views) else None,
                'precio_vs_eco_likes': float(corr_eco_likes) if not pd.isna(corr_eco_likes) else None,
                'precio_vs_eco_shares': float(corr_eco_shares) if not pd.isna(corr_eco_shares) else None,
                'precio_vs_eco_engagement': float(corr_eco_engagement) if not pd.isna(corr_eco_engagement) else None,
            }

            print(f"  Precio vs Eco Views:      {corr_eco_views:.4f}" if not pd.isna(corr_eco_views) else "  N/A")
            print(f"  Precio vs Eco Likes:      {corr_eco_likes:.4f}" if not pd.isna(corr_eco_likes) else "  N/A")
            print(f"  Precio vs Eco Shares:     {corr_eco_shares:.4f}" if not pd.isna(corr_eco_shares) else "  N/A")
            print(f"  Precio vs Eco Engagement: {corr_eco_engagement:.4f}" if not pd.isna(corr_eco_engagement) else "  N/A")

        except Exception as e:
            print(f"  ⚠ Error: {e}")
            correlaciones['economico'] = {}
    else:
        print("  ⚠ No hay posts económicos para calcular correlación")
        correlaciones['economico'] = {}

    # === CORRELACIONES FILTRADAS ===
    print("\n[C] Correlaciones FILTRADAS (solo días con posts económicos):")

    df_eco_filtered = df[df['eco_posts'] > 0].copy()

    if len(df_eco_filtered) > 1 and df_eco_filtered['eco_views'].var() > 0:
        try:
            corr_filtered_views = df_eco_filtered['usdt_bob_promedio'].corr(df_eco_filtered['eco_views'])
            corr_filtered_likes = df_eco_filtered['usdt_bob_promedio'].corr(df_eco_filtered['eco_likes'])
            corr_filtered_engagement = df_eco_filtered['usdt_bob_promedio'].corr(df_eco_filtered['eco_engagement'])

            correlaciones['filtrado'] = {
                'precio_vs_eco_views_filtrado': float(corr_filtered_views) if not pd.isna(corr_filtered_views) else None,
                'precio_vs_eco_likes_filtrado': float(corr_filtered_likes) if not pd.isna(corr_filtered_likes) else None,
                'precio_vs_eco_engagement_filtrado': float(corr_filtered_engagement) if not pd.isna(corr_filtered_engagement) else None,
                'dias_analizados': len(df_eco_filtered)
            }

            print(f"  Días analizados: {len(df_eco_filtered)}")
            print(f"  Precio vs Eco Views (filtrado):      {corr_filtered_views:.4f}" if not pd.isna(corr_filtered_views) else "  N/A")
            print(f"  Precio vs Eco Likes (filtrado):      {corr_filtered_likes:.4f}" if not pd.isna(corr_filtered_likes) else "  N/A")
            print(f"  Precio vs Eco Engagement (filtrado): {corr_filtered_engagement:.4f}" if not pd.isna(corr_filtered_engagement) else "  N/A")

        except Exception as e:
            print(f"  ⚠ Error: {e}")
            correlaciones['filtrado'] = {}
    else:
        print("  ⚠ Insuficientes días con actividad económica")
        correlaciones['filtrado'] = {'mensaje': 'Insuficientes datos'}

    # Guardar en XCom
    context['ti'].xcom_push(key='correlaciones', value=correlaciones)

    print(f"\n✓ Correlaciones calculadas y guardadas")

    return correlaciones


def cargar_parquet(**context):
    """
    Carga los datos consolidados en formato Parquet
    """
    print("=" * 60)
    print("5. Cargando datos en formato Parquet")
    print("=" * 60)

    consolidado_path = context['ti'].xcom_pull(key='consolidado_path', task_ids='transformar_consolidar_avanzado')
    df = pd.read_parquet(consolidado_path)

    # Convertir fecha a datetime
    df['fecha'] = pd.to_datetime(df['fecha'])

    # Crear directorio de salida
    output_dir = '/opt/airflow/output'
    os.makedirs(output_dir, exist_ok=True)

    # Generar nombre de archivo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    archivo_salida = f'{output_dir}/social_listening_usdt_q3_bigquery_{timestamp}.parquet'

    # Guardar en Parquet
    df.to_parquet(archivo_salida, engine='pyarrow', index=False)

    print(f"\n✓ Datos guardados exitosamente:")
    print(f"  Archivo: {archivo_salida}")
    print(f"  Registros: {len(df):,}")
    print(f"  Periodo: Q3 2025 (Julio-Septiembre)")
    print(f"  Tamaño: {os.path.getsize(archivo_salida) / 1024:.2f} KB")

    context['ti'].xcom_push(key='archivo_parquet', value=archivo_salida)

    return archivo_salida


def generar_reporte(**context):
    """
    Genera reporte ejecutivo con análisis de correlaciones Q3 2025
    """
    print("=" * 60)
    print("6. Generando Reporte Ejecutivo Q3 2025")
    print("=" * 60)

    consolidado_path = context['ti'].xcom_pull(key='consolidado_path', task_ids='transformar_consolidar_avanzado')
    correlaciones = context['ti'].xcom_pull(key='correlaciones', task_ids='calcular_correlaciones')
    archivo_parquet = context['ti'].xcom_pull(key='archivo_parquet', task_ids='cargar_parquet')

    df = pd.read_parquet(consolidado_path)
    if df['fecha'].dtype != 'object':
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date

    # Análisis de tendencia del precio
    precio_inicial = df['usdt_bob_promedio'].iloc[0]
    precio_final = df['usdt_bob_promedio'].iloc[-1]
    diferencia = precio_final - precio_inicial
    variacion_pct = (diferencia / precio_inicial) * 100

    if diferencia > 0.05:
        tendencia = "📈 SUBIDA SIGNIFICATIVA"
    elif diferencia < -0.05:
        tendencia = "📉 CAÍDA SIGNIFICATIVA"
    else:
        tendencia = "➡️ ESTABLE"

    # Estadísticas
    total_posts_general = df['total_posts'].sum()
    total_posts_eco = df['eco_posts'].sum()
    dias_con_posts = (df['total_posts'] > 0).sum()
    dias_con_posts_eco = (df['eco_posts'] > 0).sum()

    def format_corr(val):
        if val is None:
            return "N/A (sin variación)"
        return f"{val:.4f}"

    # Construir reporte
    reporte = f"""
{'='*80}
REPORTE EJECUTIVO: SOCIAL LISTENING vs USDT/BOB - Q3 2025 (BigQuery)
{'='*80}

OBJETIVO: Evaluar correlación entre tipo de cambio USDT/BOB y actividad TikTok
sobre contenido económico/político en Bolivia durante Q3 2025.

PERIODO ANALIZADO: {df['fecha'].min()} al {df['fecha'].max()} (Q3 2025)
TOTAL DE DÍAS: {len(df)} días
FUENTE TIKTOK: BigQuery (project_id_here.tiktok.silver_posts)
FUENTE USDT: Excel Q3 (GMT-4)

{'='*80}
I. ANÁLISIS DEL TIPO DE CAMBIO USDT/BOB
{'='*80}

Precio Promedio Inicial:  Bs {precio_inicial:.4f}
Precio Promedio Final:    Bs {precio_final:.4f}
Variación Absoluta:       Bs {diferencia:.4f}
Variación Porcentual:     {variacion_pct:+.2f}%
TENDENCIA Q3:             {tendencia}

{'='*80}
II. ACTIVIDAD EN TIKTOK (Q3 2025)
{'='*80}

A) ACTIVIDAD GENERAL:
   - Total de posts:              {int(total_posts_general):,}
   - Total de vistas:             {int(df['total_views'].sum()):,}
   - Total de likes:              {int(df['total_likes'].sum()):,}
   - Engagement total:            {int(df['total_engagement'].sum()):,}
   - Días con posts:              {dias_con_posts}/{len(df)}

B) ACTIVIDAD ECONÓMICA/POLÍTICA:
   - Posts económicos:            {int(total_posts_eco):,} ({total_posts_eco/total_posts_general*100:.1f}% del total)
   - Vistas económicas:           {int(df['eco_views'].sum()):,}
   - Engagement económico:        {int(df['eco_engagement'].sum()):,}
   - Días con actividad económica: {dias_con_posts_eco}/{len(df)}

{'='*80}
III. ANÁLISIS DE CORRELACIÓN DE PEARSON
{'='*80}

A) CORRELACIONES GENERALES:
"""

    if correlaciones.get('general'):
        for key, val in correlaciones['general'].items():
            metrica = key.replace('precio_vs_', '').replace('_', ' ').title()
            reporte += f"   {metrica:.<45} {format_corr(val)}\n"
    else:
        reporte += "   No disponible\n"

    reporte += f"""
B) CORRELACIONES ECONÓMICAS:
"""

    if correlaciones.get('economico'):
        for key, val in correlaciones['economico'].items():
            metrica = key.replace('precio_vs_', '').replace('_', ' ').title()
            reporte += f"   {metrica:.<45} {format_corr(val)}\n"
    else:
        reporte += "   No disponible\n"

    reporte += f"""
C) CORRELACIONES FILTRADAS (solo días con actividad económica):
"""

    if correlaciones.get('filtrado') and 'dias_analizados' in correlaciones['filtrado']:
        reporte += f"   Días analizados: {correlaciones['filtrado']['dias_analizados']}\n"
        for key, val in correlaciones['filtrado'].items():
            if key != 'dias_analizados':
                metrica = key.replace('precio_vs_', '').replace('_filtrado', '').replace('_', ' ').title()
                reporte += f"   {metrica:.<45} {format_corr(val)}\n"
    else:
        reporte += "   Insuficientes datos\n"

    reporte += f"""
{'='*80}
IV. HALLAZGOS CLAVE Q3 2025
{'='*80}

1. COBERTURA TEMPORAL:
   - Análisis completo del Q3 2025 (Julio-Septiembre)
   - Datos de TikTok extraídos desde BigQuery con conversión UTC → GMT-4
   - Sincronización perfecta con datos USDT (GMT-4)

2. ACTIVIDAD ECONÓMICA:
   - El {total_posts_eco/total_posts_general*100:.1f}% de posts están relacionados con economía/política
   - Permite análisis preciso de impacto del tipo de cambio

3. METODOLOGÍA:
   - Triple nivel de correlación (general, económico, filtrado)
   - Filtrado semántico con {len(KEYWORDS_ECONOMICAS)} keywords
   - Zona horaria Bolivia (GMT-4) en todo el análisis

{'='*80}
V. ARCHIVOS GENERADOS
{'='*80}

Dataset Consolidado:  {archivo_parquet}
Reporte Ejecutivo:    {archivo_parquet.replace('.parquet', '_reporte.txt')}
Correlaciones JSON:   {archivo_parquet.replace('.parquet', '_correlaciones.json')}

Fuente TikTok: BigQuery project_id_here.tiktok.silver_posts
Fuente USDT:   Excel Q3 2025 (GMT-4)

{'='*80}
Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Pipeline: pipeline_social_listening_usdt_v3_bigquery
{'='*80}
"""

    # Guardar reporte
    output_dir = '/opt/airflow/output'
    os.makedirs(output_dir, exist_ok=True)

    archivo_reporte = archivo_parquet.replace('.parquet', '_reporte.txt')
    with open(archivo_reporte, 'w', encoding='utf-8') as f:
        f.write(reporte)

    print(f"\n✓ Reporte ejecutivo generado:")
    print(f"  {archivo_reporte}")

    # Guardar correlaciones en JSON
    archivo_json = archivo_parquet.replace('.parquet', '_correlaciones.json')
    with open(archivo_json, 'w', encoding='utf-8') as f:
        json.dump({
            'fecha_generacion': datetime.now().isoformat(),
            'periodo': {
                'inicio': str(df['fecha'].min()),
                'fin': str(df['fecha'].max()),
                'trimestre': 'Q3 2025',
                'dias': len(df)
            },
            'fuentes': {
                'tiktok': f'{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}',
                'usdt': 'usdt_Q3.xlsx'
            },
            'precio': {
                'inicial': float(precio_inicial),
                'final': float(precio_final),
                'variacion': float(diferencia),
                'variacion_pct': float(variacion_pct)
            },
            'actividad': {
                'total_posts': int(total_posts_general),
                'eco_posts': int(total_posts_eco),
                'dias_con_posts_eco': int(dias_con_posts_eco)
            },
            'correlaciones': correlaciones
        }, f, indent=2)

    print(f"  {archivo_json}")

    context['ti'].xcom_push(key='reporte_generado', value=archivo_reporte)

    return "Reporte Q3 2025 generado exitosamente"


# Crear las tareas
task_extraer_tiktok = PythonOperator(
    task_id='extraer_tiktok_bigquery',
    python_callable=extraer_tiktok_bigquery,
    dag=dag,
)

task_extraer_usdt = PythonOperator(
    task_id='extraer_usdt_q3',
    python_callable=extraer_usdt_q3,
    dag=dag,
)

task_transformar = PythonOperator(
    task_id='transformar_consolidar_avanzado',
    python_callable=transformar_consolidar_avanzado,
    dag=dag,
)

task_correlaciones = PythonOperator(
    task_id='calcular_correlaciones',
    python_callable=calcular_correlaciones,
    dag=dag,
)

task_cargar = PythonOperator(
    task_id='cargar_parquet',
    python_callable=cargar_parquet,
    dag=dag,
)

task_reporte = PythonOperator(
    task_id='generar_reporte',
    python_callable=generar_reporte,
    dag=dag,
)

# Definir dependencias
[task_extraer_tiktok, task_extraer_usdt] >> task_transformar >> [task_correlaciones, task_cargar]
task_correlaciones >> task_reporte
task_cargar >> task_reporte
