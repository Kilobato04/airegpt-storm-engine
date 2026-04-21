import json
import boto3
import requests
import datetime
import time 
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

def fetch_open_meteo():
    """Descarga el pronóstico dividiendo los 100 puntos en lotes para evitar error 502"""
    try:
        print("🌐 Generando malla de 100 puntos...")
        latS, latN, lonW, lonE = 19.155, 19.772, -99.352, -98.867
        steps = 9
        all_coords = []
        for i in range(steps + 1):
            lat = latS + (i * (latN - latS) / steps)
            for j in range(steps + 1):
                lon = lonW + (j * (lonE - lonW) / steps)
                all_coords.append((f"{lat:.4f}", f"{lon:.4f}"))
        
        # 🚨 DIVISIÓN EN LOTES (Batching)
        size = 20 # 20 puntos por petición
        chunks = [all_coords[i:i + size] for i in range(0, len(all_coords), size)]
        full_results = []
        
        horas_futuras = 12
        
        for idx, chunk in enumerate(chunks):
            lats = [c[0] for c in chunk]
            lons = [c[1] for c in chunk]
            url = f"https://api.open-meteo.com/v1/forecast?latitude={','.join(lats)}&longitude={','.join(lons)}&hourly=temperature_2m,relative_humidity_2m,precipitation,surface_pressure,wind_speed_10m,wind_direction_10m&timezone=America%2FMexico_City&forecast_hours={horas_futuras}"
            
            # Reintentos por lote
            exito_lote = False
            for intento in range(2):
                try:
                    print(f"📡 Lote {idx+1}/5 - Intento {intento+1}...")
                    res = requests.get(url, timeout=20)
                    if res.status_code == 200:
                        data = res.json()
                        # Si es un solo punto, Open-Meteo no manda lista, lo normalizamos
                        if not isinstance(data, list): data = [data]
                        
                        for item in data:
                            full_results.append({
                                "lat": item['latitude'], 
                                "lon": item['longitude'], 
                                "hourly": item['hourly']
                            })
                        exito_lote = True
                        break
                    else:
                        print(f"⚠️ Error {res.status_code} en Lote {idx+1}")
                except Exception as e:
                    print(f"⚠️ Fallo en Lote {idx+1}: {e}")
                
                time.sleep(1) # Respiro entre reintentos
            
            if not exito_lote:
                print(f"❌ Abortando: El Lote {idx+1} falló definitivamente.")
                return None
            
            time.sleep(0.5) # Respiro entre lotes para no saturar la API
            
        print(f"✅ Éxito: {len(full_results)} nodos procesados correctamente.")
        return full_results
        
    except Exception as e:
        print(f"❌ Error crítico en fetch_open_meteo: {e}")
        return None


def lambda_handler(event, context):
    # ==========================================
    # --- 0. PROXY S3 (Vía Rápida para el Frontend) ---
    # ==========================================
    ruta_web = event.get('rawPath', event.get('path'))
    
    if ruta_web:
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=60'
        }
        try:
            # Determinamos qué archivo quiere el mapa (Forecast o Presente)
            target_key = S3_KEY_FORECAST if '/forecast' in ruta_web else S3_KEY_LATEST
            
            # Vamos directo al buzón (S3)
            resp = s3_client.get_object(Bucket=S3_BUCKET, Key=target_key)
            return {
                "statusCode": 200, 
                "headers": headers, 
                "body": resp['Body'].read().decode('utf-8')
            }
        except Exception as e:
            print(f"⚠️ Error en Proxy S3: {e}")
            return {"statusCode": 404, "headers": headers, "body": json.dumps({"error": "Data not ready"})}

    # ==========================================
    # --- MOTOR DE CÁLCULO (Sólo Cron/Test) ---
    # ==========================================
    # Si la ejecución sigue aquí, es porque toca procesar la ciencia de datos.
    print("🧠 Iniciando Motor de IA...")
    
    es_trabajo_pronostico = event.get('action') == 'run_forecast'
    ahora = datetime.datetime.now(datetime.timezone.utc)
    
    with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
        malla_json = json.load(f)
        
    grid = gpd.GeoDataFrame.from_features(malla_json['features'])
    grid['lon'] = grid.geometry.x
    grid['lat'] = grid.geometry.y

    # ==========================================
    # CASO A: PRONÓSTICO (Cada 1 hora)
    # ==========================================
    if es_trabajo_pronostico:
        print("🔮 Iniciando Proyección de Forecast...")
        forecast_raw = fetch_open_meteo()

        if not forecast_raw:
            print("🛑 Fallo total al recuperar datos. Abortando forecast.")
            return {"statusCode": 500, "body": "Fallo al obtener datos de Open-Meteo."}

        # 🚨 FIX DE HORA EXACTA (Sincronización con Reloj CDMX) 🚨
        # 1. Truncamos a la hora actual exacta (EJ: 17:35 -> 17:00)
        ahora_cdmx = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=6)
        hora_actual_str = ahora_cdmx.strftime("%Y-%m-%dT%H:00")
        
        # 2. Buscamos el índice de esa hora actual (ej. 17:00) en los datos de Open-Meteo
        tiempos = forecast_raw[0]['hourly']['time']
        idx_start = 0
        for i, t in enumerate(tiempos):
            if t >= hora_actual_str:
                idx_start = i
                break
        
        # 3. Recortamos a 7 horas (Índice 0 = ACTUAL, Índices 1 al 6 = FUTURO)
        for p in forecast_raw:
            for key in p['hourly']:
                p['hourly'][key] = p['hourly'][key][idx_start:idx_start+7]

        # 4. Preparamos el payload
        bloque_futuro = {
            "generated_at": ahora.isoformat(), 
            "time_steps": {},
            "raw_nodes": forecast_raw
        }

        try:
            # 5. El Motor IA calcula desde el Índice 1 (ej. 18:00) hasta el 6 (ej. 23:00)
            # El paso 0 lo saltamos porque es el "Presente" que ya tienes en el otro modelo.
            for i in range(1, 7): 
                datos_hora = []
                hora_iso = ""
                for p in forecast_raw:
                    hora_iso = p['hourly']['time'][i]
                    datos_hora.append({'lat': p['lat'], 'lon': p['lon'], 'rain': p['hourly']['precipitation'][i]})
                
                print(f"⏳ Calculando IA Espacial para: {hora_iso}...")
                df_h = pd.DataFrame(datos_hora)
                lluvia_proyectada = ejecutar_interpolacion(df_h, grid)
                
                bloque_futuro["time_steps"][hora_iso] = [
                    {"lat": round(grid.iloc[idx]['lat'], 5), "lon": round(grid.iloc[idx]['lon'], 5), "mm": float(lluvia_proyectada[idx])}
                    for idx in range(len(lluvia_proyectada)) if lluvia_proyectada[idx] > 0
                ]

            # 6. Subida final a S3
            s3_client.put_object(
                Bucket=S3_BUCKET, Key=S3_KEY_FORECAST,
                Body=json.dumps(bloque_futuro), ContentType='application/json', CacheControl='max-age=300'
            )
            print("✅ Pronóstico guardado en S3 exitosamente.")
            return {"statusCode": 200, "body": "Forecast Updated Successfully"}
            
        except Exception as e:
            print(f"❌ ERROR FATAL en el cálculo del forecast: {str(e)}")
            return {"statusCode": 500, "body": f"Error interno en forecast: {str(e)}"}

    # ==========================================
    # CASO B: PRESENTE (Cada 3 minutos)
    # ==========================================
    else:
        print("🚀 Iniciando Motor de Lluvia Actual (Modo S3-Direct)...")
        try:
            # 🚨 SUSTITUCIÓN: En lugar de requests.get, leemos el buzón de S3
            print("📡 Obteniendo datos crudos desde S3 (latest_sacmex.json)...")
            res_s3 = s3_client.get_object(Bucket=S3_BUCKET, Key="latest_sacmex.json")
            api_data = json.loads(res_s3['Body'].read().decode('utf-8'))
            estaciones = api_data.get('data', [])
            print(f"✅ Datos recuperados exitosamente de S3.")
        except Exception as e:
            print(f"❌ Error crítico leyendo origen en S3: {e}")
            return {"statusCode": 500, "body": f"Error de lectura S3: {str(e)}"}

        lluvia_activa = [s for s in estaciones if float(s['acumulado_actual']) > 0]
        
        # 1. Leemos el modelo previo de S3 para la derivada
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
            estado_previo = json.loads(response['Body'].read().decode('utf-8'))
            max_rain_previo = float(estado_previo.get('metadata', {}).get('lluvia_max', 0.0))
            fecha_previa = datetime.datetime.fromisoformat(estado_previo.get('timestamp'))
        except Exception:
            max_rain_previo = 0.0
            fecha_previa = ahora - datetime.timedelta(minutes=3)

        # 2. Tu Lógica de Reposo (Manteniendo N < 2)
        if len(lluvia_activa) < 2:
            if max_rain_previo > 0.0:
                print("🧹 FIN DE TORMENTA: Limpiando mapa...")
                # Dejamos que el código siga para que escriba la malla en ceros
            else:
                print("💤 ESTADO SLEEP: Mapa ya en cero.")
                return {"statusCode": 200, "body": "SLEEP"}
        else:
            print(f"⛈️ ESTADO ACTIVO: {len(lluvia_activa)} estaciones con lluvia.")

        # 3. Preparación de Datos
        if len(lluvia_activa) >= 2:
            df_obs = pd.DataFrame([{
                'id': s['id'], 'nombre': s['nombre'], 'lat': float(s['latitud']),
                'lon': float(s['longitud']), 'rain': float(s['acumulado_actual'])
            } for s in lluvia_activa])
            max_rain_actual = float(df_obs['rain'].max())
        else:
            df_obs = pd.DataFrame(columns=['id', 'nombre', 'lat', 'lon', 'rain'])
            max_rain_actual = 0.0

        delta_time_min = (ahora - fecha_previa).total_seconds() / 60.0
        delta_rain = max_rain_actual - max_rain_previo
        
        derivada = 0.0
        alerta_status = "NORMAL"
        if delta_time_min > 0 and delta_rain > 0:
            derivada = delta_rain / delta_time_min
            if derivada >= UMBRAL_NARANJA: alerta_status = "PREVENTIVA_NARANJA"
            if derivada >= UMBRAL_PURPURA: alerta_status = "CRITICA_PURPURA"

        print(f"📈 Derivada: {derivada:.2f} mm/min | Alerta: {alerta_status}")

        # 4. Cálculo Espacial Protegido
        if max_rain_actual > 0:
            grid['rain_predicted'] = ejecutar_interpolacion(df_obs, grid)
        else:
            grid['rain_predicted'] = 0.0
        
        output_cells = []
        tree = cKDTree(grid[['lat', 'lon']].values)
        
        # 5. Inicializamos las 3,000 celdas
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
                "alert_status": "NORMAL",
                "station": None,
                "source": "Modelo Espacial (RBF)"
            })

        # 6. Planchamos TODAS las estaciones sobre la malla (llueva o no)
        for s in estaciones:
            try:
                s_lat = float(s['latitud'])
                s_lon = float(s['longitud'])
                s_rain = float(s['acumulado_actual'])
                _, closest_idx = tree.query([s_lat, s_lon])
                
                nombre_est = str(s.get('nombre', ''))
                id_est = str(s.get('id', ''))
                
                if 'chaak' in nombre_est.lower() or 'smability' in nombre_est.lower():
                    origen = "Sensor Activo (Red Smability)"
                else:
                    origen = "Sensor Activo (Red SACMEX)"

                output_cells[closest_idx].update({
                    "station": f"{nombre_est} (ID: {id_est})",
                    "source": origen
                })

                # Si además está lloviendo, forzamos la realidad dura
                if s_rain > 0:
                    output_cells[closest_idx]["rain_mm_h"] = float(s_rain)
                    output_cells[closest_idx]["risk"] = "Crítico" if alerta_status != "NORMAL" else "Moderado"
                    
                    if max_rain_actual > 0 and s_rain == max_rain_actual:
                        output_cells[closest_idx]["derivative_mm_min"] = float(round(derivada, 2))
                        output_cells[closest_idx]["alert_status"] = alerta_status

            except Exception:
                continue

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
