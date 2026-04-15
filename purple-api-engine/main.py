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

# --- 1. CONFIGURACIÓN ---
S3_BUCKET = "airegpt-storm-data"
S3_KEY_LATEST = "latest_model.json"
S3_KEY_FORECAST = "latest_forecast.json" # <--- AJUSTE 1
MIRROR_API_URL = "https://onr6tt7eohxppmqaak3jyapt3e0knhvu.lambda-url.us-east-1.on.aws/"
GEOJSON_PATH = '/var/task/zmvm_malla_consolidada.geojson'

UMBRAL_NARANJA = 0.5
UMBRAL_PURPURA = 1.0

s3_client = boto3.client('s3')

# --- 2. AJUSTE: FUNCIÓN MATEMÁTICA INDEPENDIENTE ---
def ejecutar_interpolacion(df_puntos, malla_base):
    """
    Toma puntos (estaciones o forecast) y los proyecta sobre las 3000 celdas
    """
    try:
        rbf = Rbf(df_puntos['lon'], df_puntos['lat'], df_puntos['rain'], 
                  function='gaussian', epsilon=0.03, smooth=0.1)
        prediccion = np.round(np.maximum(0, rbf(malla_base['lon'], malla_base['lat'])), 2)
        prediccion[prediccion < 0.15] = 0
        return prediccion
    except:
        # Fallback si hay pocos puntos
        tree = cKDTree(df_puntos[['lon', 'lat']].values)
        dist, _ = tree.query(malla_base[['lon', 'lat']].values)
        vals = np.zeros(len(malla_base))
        vals[dist < 0.02] = df_puntos['rain'].max()
        return vals

def lambda_handler(event, context):
    ahora = datetime.datetime.now(datetime.timezone.utc)
    
    # --- 3. AJUSTE: DETECCIÓN DE EVENTO ---
    # Si el evento trae 'run_forecast', es el Cron de 1 hora. Si no, es el de 3 min.
    es_trabajo_pronostico = event.get('action') == 'run_forecast'
    
    # Carga de Geometría (Hack: Bypass de Fiona)
    with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
        malla_json = json.load(f)
        
    grid = gpd.GeoDataFrame.from_features(malla_json['features'])
    grid['lon'] = grid.geometry.x
    grid['lat'] = grid.geometry.y

    # ==========================================
    # CASO PROYECCIÓN (FUTURO / FORECAST)
    # ==========================================
    if es_trabajo_pronostico:
        print("🔮 Iniciando Proyección de Forecast (6 horas)...")
        try:
            # Llamamos a la nueva ruta /forecast que creamos en Lambda A
            req_f = requests.get(f"{MIRROR_API_URL}forecast", timeout=15)
            forecast_raw = req_f.json().get('data', [])
        except Exception as e:
            return {"statusCode": 500, "body": f"Error en API Forecast: {e}"}

        if not forecast_raw:
            return {"statusCode": 200, "body": "Sin datos para forecast"}

        bloque_futuro = {"generated_at": ahora.isoformat(), "time_steps": {}}

        # Modelamos las 6 horas futuras (índices 1 a 5 del JSON de Open-Meteo)
        for i in range(1, 6):
            datos_hora = []
            hora_iso = ""
            for p in forecast_raw:
                hora_iso = p['hourly']['time'][i]
                datos_hora.append({
                    'lat': p['lat'], 
                    'lon': p['lon'], 
                    'rain': p['hourly']['precipitation'][i]
                })
            
            df_h = pd.DataFrame(datos_hora)
            lluvia_proyectada = ejecutar_interpolacion(df_h, grid)
            
            # Guardamos solo celdas con lluvia para que el JSON no pese megas
            bloque_futuro["time_steps"][hora_iso] = [
                {"lat": round(grid.iloc[idx]['lat'], 5), "lon": round(grid.iloc[idx]['lon'], 5), "mm": float(lluvia_proyectada[idx])}
                for idx in range(len(lluvia_proyectada)) if lluvia_proyectada[idx] > 0
            ]

        # Guardar el bloque completo en S3
        s3_client.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY_FORECAST,
            Body=json.dumps(bloque_futuro), ContentType='application/json'
        )
        print("✅ Pronóstico de 6 horas guardado exitosamente.")
        return {"statusCode": 200, "body": "Forecast S3 Updated"}

    # ==========================================
    # CASO MONITOREO (PRESENTE / SACMEX)
    # ==========================================
    else:
        print("🚀 Iniciando Motor de Lluvia Actual...")
        # (Aquí mantienes exactamente tu bloque actual de SACMEX)
        # 1. Leer API Espejo
        # 2. Calcular Derivada
        # 3. Correr ejecutar_interpolacion
        # 4. Generar output_cells y subir a S3_KEY_LATEST
        return {"statusCode": 200, "body": "Present Model Updated"}
