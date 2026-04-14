import json
import boto3
import requests
import datetime
import geopandas as gpd
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from scipy.interpolate import Rbf
import os

# --- CONFIGURACIÓN ---
S3_BUCKET = "airegpt-storm-data" # DEBES CREAR ESTE BUCKET EN AWS
S3_KEY_LATEST = "latest_model.json"
MIRROR_API_URL = "URL_DE_TU_LAMBDA_A_AQUI" # Reemplazar con el endpoint de SACMEX Mirror
GEOJSON_PATH = '/var/task/zmvm_malla_consolidada.geojson'

UMBRAL_NARANJA = 0.5
UMBRAL_PURPURA = 1.0

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("🚀 Iniciando Motor API Púrpura (Evaluando Estado...)")
    
    # 1. LEER API ESPEJO (SACMEX)
    try:
        req = requests.get(MIRROR_API_URL, timeout=10)
        api_data = req.json()
        estaciones = api_data.get('data', [])
    except Exception as e:
        return {"statusCode": 500, "body": f"Error leyendo API Espejo: {e}"}

    # Filtrar estaciones con lluvia
    lluvia_activa = [s for s in estaciones if float(s['acumulado_actual']) > 0]
    
    # ==========================================
    # ESTADO 0: SLEEP (Sin Lluvia, no modelar)
    # ==========================================
    if len(lluvia_activa) < 2:
        print("💤 ESTADO SLEEP: Menos de 2 estaciones con lluvia. Abortando modelo.")
        return {"statusCode": 200, "body": "SLEEP - Sin lluvia suficiente"}

    # ==========================================
    # ESTADO 1 & 2: ACTIVE / CRITICAL
    # ==========================================
    print(f"⛈️ ESTADO ACTIVO: {len(lluvia_activa)} estaciones con lluvia detectadas.")
    
    # Obtener estado anterior de S3 para la Derivada
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
        estado_previo = json.loads(response['Body'].read().decode('utf-8'))
        max_rain_previo = estado_previo.get('metadata', {}).get('lluvia_max', 0)
        fecha_previa_str = estado_previo.get('timestamp')
        fecha_previa = datetime.datetime.fromisoformat(fecha_previa_str)
    except Exception:
        max_rain_previo = 0
        fecha_previa = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=3)

    # Preparar datos de las estaciones para el modelo
    df_obs = pd.DataFrame([{
        'id': s['id'],
        'nombre': s['nombre'],
        'lat': float(s['latitud']),
        'lon': float(s['longitud']),
        'rain': float(s['acumulado_actual']),
        'alcaldia': s['alcaldia']
    } for s in estaciones if float(s['acumulado_actual']) > 0])

    max_rain_actual = df_obs['rain'].max()
    ahora = datetime.datetime.now(datetime.timezone.utc)
    
    # Calcular Derivada
    delta_rain = max_rain_actual - max_rain_previo
    delta_time_min = (ahora - fecha_previa).total_seconds() / 60.0
    
    derivada = 0.0
    alerta_status = "NORMAL"
    
    if delta_time_min > 0 and delta_rain > 0:
        derivada = delta_rain / delta_time_min
        if derivada >= UMBRAL_NARANJA: alerta_status = "PREVENTIVA_NARANJA"
        if derivada >= UMBRAL_PURPURA: alerta_status = "CRITICA_PURPURA"

    print(f"📈 Derivada: {derivada:.2f} mm/min | Alerta: {alerta_status}")

    # CARGA GEOMETRÍA
    grid = gpd.read_file(GEOJSON_PATH)
    grid['lon'] = grid.geometry.x
    grid['lat'] = grid.geometry.y
    grid['rain_predicted'] = 0.0

    # RBF GAUSSIANO
    try:
        rbf = Rbf(df_obs['lon'], df_obs['lat'], df_obs['rain'], function='gaussian', epsilon=0.03, smooth=0.1)
        grid['rain_predicted'] = np.round(np.maximum(0, rbf(grid['lon'], grid['lat'])), 2)
    except:
        dist, _ = cKDTree(df_obs[['lon', 'lat']].values).query(grid[['lon', 'lat']].values)
        grid.loc[dist < 0.02, 'rain_predicted'] = max_rain_actual

    grid.loc[grid['rain_predicted'] < 0.15, 'rain_predicted'] = 0
    
    # ==========================================
    # GENERAR JSON (MOCKUP AIRE HOMOLOGADO)
    # ==========================================
    output_cells = []
    
    # KDTree para hacer "Snap" de las estaciones reales a las celdas
    tree = cKDTree(grid[['lat', 'lon']].values)
    
    for idx, row in grid.iterrows():
        lat, lon = row['lat'], row['lon']
        rain_val = row['rain_predicted']
        
        celda = {
            "timestamp": ahora.strftime("%Y-%m-%d %H:%M:%S"),
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            # LECTURA DINÁMICA DEL NUEVO GEOJSON CONSOLIDADO
            "col": str(row.get('colonia', 'Zona Federal / Sin Colonia')),
            "mun": str(row.get('municipio', 'CDMX/Edomex')),
            "edo": str(row.get('estado', 'Ciudad de México')),
            "pob": int(row.get('poblacion', 0)),
            "altitude": int(row.get('elevation', 0)),
            "rain_mm_h": float(rain_val),
            "derivative_mm_min": 0.0,
            "risk": "Moderado" if rain_val > 3 else "Ligero",
            "alert_status": "NORMAL",
            "station": None,
            "sources": json.dumps({"rain_mm_h": "Modeled (Gaussian RBF)"})
        }
        output_cells.append(celda)

    # Inyectar (Snap) la data dura de las estaciones en sus celdas correspondientes
    for _, est in df_obs.iterrows():
        _, closest_idx = tree.query([est['lat'], est['lon']])
        
        output_cells[closest_idx].update({
            # Ya NO sobreescribimos 'col' ni 'mun', respetamos la geografía de la celda.
            # Solo actualizamos los datos meteorológicos y el nombre de la estación.
            "rain_mm_h": float(est['rain']),
            "derivative_mm_min": float(round(derivada, 2)) if est['rain'] == max_rain_actual else 0.0,
            "risk": "Crítico" if alerta_status != "NORMAL" else "Moderado",
            "alert_status": alerta_status if est['rain'] == max_rain_actual else "NORMAL",
            "station": f"{est['nombre']} (ID: {est['id']})",
            "sources": json.dumps({"rain_mm_h": "Official SACMEX", "derivative": "Calculated Live"})
        })

    # JSON FINAL
    final_payload = {
        "timestamp": ahora.isoformat(),
        "metadata": {
            "lluvia_max": max_rain_actual,
            "derivada_max": round(derivada, 3),
            "alerta_global": alerta_status
        },
        "values": output_cells
    }

    # Guardar en S3
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY_LATEST,
        Body=json.dumps(final_payload),
        ContentType='application/json',
        CacheControl='max-age=60'
    )

    print("✅ Modelado completado y subido a S3.")
    return {"statusCode": 200, "body": "Model executed successfully"}
