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
S3_KEY_FORECAST = "latest_forecast.json"
MIRROR_API_URL = "https://onr6tt7eohxppmqaak3jyapt3e0knhvu.lambda-url.us-east-1.on.aws/"
GEOJSON_PATH = '/var/task/zmvm_malla_consolidada.geojson'

UMBRAL_NARANJA = 0.5
UMBRAL_PURPURA = 1.0

s3_client = boto3.client('s3')

def ejecutar_interpolacion(df_puntos, malla_base):
    """Interpolación RBF Gaussiana centralizada"""
    try:
        rbf = Rbf(df_puntos['lon'], df_puntos['lat'], df_puntos['rain'], 
                  function='gaussian', epsilon=0.03, smooth=0.1)
        prediccion = np.round(np.maximum(0, rbf(malla_base['lon'], malla_base['lat'])), 2)
        prediccion[prediccion < 0.15] = 0
        return prediccion
    except:
        tree = cKDTree(df_puntos[['lon', 'lat']].values)
        dist, _ = tree.query(malla_base[['lon', 'lat']].values)
        vals = np.zeros(len(malla_base))
        vals[dist < 0.02] = df_puntos['rain'].max()
        return vals

def lambda_handler(event, context):
    
    # ==========================================
    # --- 0. INTERCEPTOR HTTP (API PARA FRONTEND MAPAS) ---
    # ==========================================
    # Si el evento trae 'rawPath', significa que entraron por la Function URL
    ruta_web = event.get('rawPath', event.get('path'))
    
    if ruta_web:
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=60'
        }
        try:
            # Si en la URL escribieron /forecast, damos la malla de 3000 celdas del futuro
            if '/forecast' in ruta_web:
                resp = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_FORECAST)
                return {"statusCode": 200, "headers": headers, "body": resp['Body'].read().decode('utf-8')}
            
            # Si entran a la raíz (/), damos la malla del presente
            else:
                resp = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
                return {"statusCode": 200, "headers": headers, "body": resp['Body'].read().decode('utf-8')}
        
        except Exception as e:
            return {"statusCode": 500, "headers": headers, "body": json.dumps({"error": "Archivo no encontrado en S3", "details": str(e)})}

    # ==========================================
    # --- LÓGICA DEL MOTOR (CRON JOBS DE EVENTBRIDGE) ---
    # ==========================================
    # (Si no hay ruta_web, ignoramos el interceptor y corremos el motor normal)
    
    ahora = datetime.datetime.now(datetime.timezone.utc)
    es_trabajo_pronostico = event.get('action') == 'run_forecast'
    
    # --- CARGA GEOMETRÍA (BYPASS FIONA) ---
    with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
        malla_json = json.load(f)
        
    grid = gpd.GeoDataFrame.from_features(malla_json['features'])
    grid['lon'] = grid.geometry.x
    grid['lat'] = grid.geometry.y

    # ==========================================
    # CASO A: PRONÓSTICO (Cada 1 hora)
    # ==========================================
    if es_trabajo_pronostico:
        print("🔮 Iniciando Proyección de Forecast (6 horas)...")
        try:
            req_f = requests.get(f"{MIRROR_API_URL}forecast", timeout=15)
            forecast_raw = req_f.json().get('data', [])
        except Exception as e:
            return {"statusCode": 500, "body": f"Error API Forecast: {e}"}

        if not forecast_raw:
            return {"statusCode": 200, "body": "Sin datos de forecast"}

        bloque_futuro = {"generated_at": ahora.isoformat(), "time_steps": {}}

        for i in range(1, 6):
            datos_hora = []
            hora_iso = ""
            for p in forecast_raw:
                hora_iso = p['hourly']['time'][i]
                datos_hora.append({'lat': p['lat'], 'lon': p['lon'], 'rain': p['hourly']['precipitation'][i]})
            
            df_h = pd.DataFrame(datos_hora)
            lluvia_proyectada = ejecutar_interpolacion(df_h, grid)
            
            bloque_futuro["time_steps"][hora_iso] = [
                {"lat": round(grid.iloc[idx]['lat'], 5), "lon": round(grid.iloc[idx]['lon'], 5), "mm": float(lluvia_proyectada[idx])}
                for idx in range(len(lluvia_proyectada)) if lluvia_proyectada[idx] > 0
            ]

        s3_client.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY_FORECAST,
            Body=json.dumps(bloque_futuro), ContentType='application/json'
        )
        print("✅ Pronóstico guardado en S3.")
        return {"statusCode": 200, "body": "Forecast Updated"}

    # ==========================================
    # CASO B: PRESENTE (Cada 3 minutos)
    # ==========================================
    else:
        print("🚀 Iniciando Motor de Lluvia Actual...")
        try:
            req = requests.get(MIRROR_API_URL, timeout=10)
            api_data = req.json()
            estaciones = api_data.get('data', [])
        except Exception as e:
            return {"statusCode": 500, "body": f"Error leyendo API Espejo: {e}"}

        lluvia_activa = [s for s in estaciones if float(s['acumulado_actual']) > 0]
        
        # if len(lluvia_activa) < 2:
        #     print("💤 ESTADO SLEEP: Menos de 2 estaciones con lluvia.")
        #     return {"statusCode": 200, "body": "SLEEP - Sin lluvia suficiente"}

        print(f"⛈️ ESTADO ACTIVO: {len(lluvia_activa)} estaciones con lluvia.")
        
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
            estado_previo = json.loads(response['Body'].read().decode('utf-8'))
            max_rain_previo = estado_previo.get('metadata', {}).get('lluvia_max', 0)
            fecha_previa = datetime.datetime.fromisoformat(estado_previo.get('timestamp'))
        except Exception:
            max_rain_previo = 0
            fecha_previa = ahora - datetime.timedelta(minutes=3)

        df_obs = pd.DataFrame([{
            'id': s['id'], 'nombre': s['nombre'], 'lat': float(s['latitud']),
            'lon': float(s['longitud']), 'rain': float(s['acumulado_actual'])
        } for s in lluvia_activa])

        max_rain_actual = df_obs['rain'].max()
        delta_time_min = (ahora - fecha_previa).total_seconds() / 60.0
        delta_rain = max_rain_actual - max_rain_previo
        
        derivada = 0.0
        alerta_status = "NORMAL"
        if delta_time_min > 0 and delta_rain > 0:
            derivada = delta_rain / delta_time_min
            if derivada >= UMBRAL_NARANJA: alerta_status = "PREVENTIVA_NARANJA"
            if derivada >= UMBRAL_PURPURA: alerta_status = "CRITICA_PURPURA"

        print(f"📈 Derivada: {derivada:.2f} mm/min | Alerta: {alerta_status}")

        grid['rain_predicted'] = ejecutar_interpolacion(df_obs, grid)
        
        output_cells = []
        tree = cKDTree(grid[['lat', 'lon']].values)
        
        for idx, row in grid.iterrows():
            rain_val = row['rain_predicted']
            output_cells.append({
                "lat": round(row['lat'], 5), "lon": round(row['lon'], 5),
                "col": str(row.get('colonia', 'Sin Colonia')),
                "mun": str(row.get('municipio', 'CDMX/Edomex')),
                "edo": str(row.get('estado', 'CDMX')),
                "rain_mm_h": float(rain_val),
                "derivative_mm_min": 0.0,
                "risk": "Moderado" if rain_val > 3 else "Ligero",
                "alert_status": "NORMAL", "station": None
            })

        for _, est in df_obs.iterrows():
            _, closest_idx = tree.query([est['lat'], est['lon']])
            output_cells[closest_idx].update({
                "rain_mm_h": float(est['rain']),
                "derivative_mm_min": float(round(derivada, 2)) if est['rain'] == max_rain_actual else 0.0,
                "risk": "Crítico" if alerta_status != "NORMAL" else "Moderado",
                "alert_status": alerta_status if est['rain'] == max_rain_actual else "NORMAL",
                "station": f"{est['nombre']} (ID: {est['id']})"
            })

        final_payload = {
            "timestamp": ahora.isoformat(),
            "metadata": {
                "lluvia_max": max_rain_actual,
                "derivada_max": round(derivada, 3),
                "alerta_global": alerta_status
            },
            "values": output_cells
        }

        s3_client.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY_LATEST,
            Body=json.dumps(final_payload), ContentType='application/json', CacheControl='max-age=60'
        )

        print("✅ Modelado PRESENTE completado y subido a S3.")
        return {"statusCode": 200, "body": "Present Model Executed successfully"}
