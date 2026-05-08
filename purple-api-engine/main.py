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
S3_KEY_HISTORY = "history_24h.json"
S3_KEY_ACCUMULATED = "accumulated_24h.json"
MIRROR_API_URL = "https://onr6tt7eohxppmqaak3jyapt3e0knhvu.lambda-url.us-east-1.on.aws/"
GEOJSON_PATH = '/var/task/zmvm_malla_consolidada.geojson'

UMBRAL_NARANJA = 0.5
UMBRAL_PURPURA = 1.0

s3_client = boto3.client('s3')

# ==========================================
# ⚡ CACHÉ GLOBAL (Optimización Cold Start)
# ==========================================
RIESGOS_CACHE = None

def cargar_riesgos_historicos():
    global RIESGOS_CACHE
    if RIESGOS_CACHE is None:
        try:
            print("📦 Descargando catálogo de riesgos 2019-2024 (Cold Start)...")
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key="risk_zones_2019_2024.json")
            RIESGOS_CACHE = json.loads(obj['Body'].read().decode('utf-8')).get('risk_zones', [])
            print(f"✅ {len(RIESGOS_CACHE)} zonas de riesgo cargadas en RAM.")
        except Exception as e:
            print(f"⚠️ Error cargando caché de riesgos: {e}")
            RIESGOS_CACHE = []
    return RIESGOS_CACHE

# 1. ESTA ES TU FUNCIÓN ORIGINAL (No la tocamos, se queda para el Modelo Live / SACMEX)
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

# 2. 🚨 FIX: ESTA ES LA NUEVA FUNCIÓN (Exclusiva para el Pronóstico / Open-Meteo)
def ejecutar_interpolacion_atmosferica(df_puntos, malla_base):
    """Modelo SCIT (Storm Cell Identification Tracking) con Kernels Gaussianos"""
    import numpy as np
    import math
    from scipy.spatial import cKDTree
    
    try:
        # 1. Identificar nodos activos (Umbral mínimo para existir)
        nodos_humedos = df_puntos[df_puntos['rain'] >= 0.15].reset_index(drop=True)
        
        # Grid final inicializado en 0 (Lienzo en negro)
        prediccion_global = np.zeros(len(malla_base))
        
        if len(nodos_humedos) == 0:
            return prediccion_global # Si no hay lluvia en el valle, entregamos el lienzo limpio
            
        lon_malla = malla_base['lon'].values
        lat_malla = malla_base['lat'].values
        
        # 2. Algoritmo de Clustering Espacial (Vecinos a menos de ~8km o 0.075 grados)
        coords = nodos_humedos[['lon', 'lat']].values
        tree = cKDTree(coords)
        pares = tree.query_pairs(r=0.075)
        
        # Construir grupos (Connected Components) nativamente
        adj = {i: [] for i in range(len(coords))}
        for i, j in pares:
            adj[i].append(j)
            adj[j].append(i)
            
        clusters = []
        visitados = set()
        for i in range(len(coords)):
            if i not in visitados:
                cluster = []
                cola = [i]
                while cola:
                    nodo = cola.pop(0)
                    if nodo not in visitados:
                        visitados.add(nodo)
                        cluster.append(nodo)
                        cola.extend(adj[nodo])
                clusters.append(cluster)
        
        # 3. Modelación de Células de Tormenta por Cluster
        for indices in clusters:
            cluster_data = nodos_humedos.iloc[indices]
            es_tormenta = len(indices) >= 3
            
            # Viento local extraído SÓLO de esta tormenta
            viento_vel = cluster_data['wind_speed'].mean()
            viento_dir = cluster_data['wind_dir'].mean()
            
            # Geometría de la nube
            angulo_rad = math.radians(270 - viento_dir)
            stretch_factor = 1.0 + (viento_vel / 15.0) if es_tormenta else 1.0 + (viento_vel / 35.0)
            sigma = 0.038 if es_tormenta else 0.022 # Radio base (0.038 grados ~ 4.2 km)

            # Generar el Kernel (Campana de Gauss) para cada nodo del cluster
            for _, fila in cluster_data.iterrows():
                lon_c = lon_malla - fila['lon']
                lat_c = lat_malla - fila['lat']
                
                # Rotar la malla hacia la dirección del viento local
                x_rot = lon_c * math.cos(angulo_rad) + lat_c * math.sin(angulo_rad)
                y_rot = -lon_c * math.sin(angulo_rad) + lat_c * math.cos(angulo_rad)
                
                # Estirar la nube a lo largo del eje direccional
                x_rot = x_rot / stretch_factor
                
                # Distancia deformada
                dist_sq = x_rot**2 + y_rot**2
                
                # Pinta la Campana de Lluvia que va cayendo a 0 suavemente
                intensidad = fila['rain'] * np.exp(-dist_sq / (2 * sigma**2))
                
                # Fusión líquida: Toma el valor más alto entre lo que ya había y la nueva nube
                prediccion_global = np.maximum(prediccion_global, intensidad)
        
        # 4. Limpieza final de colas (Recortar los bordes casi invisibles)
        prediccion_global = np.round(prediccion_global, 2)
        prediccion_global[prediccion_global < 0.15] = 0
        
        return prediccion_global

    except Exception as e:
        print(f"⚠️ Caída a KDTree de emergencia (Atmosférico): {e}")
        tree = cKDTree(df_puntos[['lon', 'lat']].values)
        dist, _ = tree.query(malla_base[['lon', 'lat']].values)
        vals = np.zeros(len(malla_base))
        vals[dist < 0.03] = df_puntos['rain'].max()
        return vals

def fetch_open_meteo():
    """Descarga el pronóstico en micro-lotes para evitar el Error 502 de Open-Meteo en horas pico"""
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
        
        # 🚨 FIX 1: Lotes más pequeños (10 en vez de 20)
        size = 10 
        chunks = [all_coords[i:i + size] for i in range(0, len(all_coords), size)]
        full_results = []
        
        horas_futuras = 12
        
        for idx, chunk in enumerate(chunks):
            lats = [c[0] for c in chunk]
            lons = [c[1] for c in chunk]
            url = f"https://api.open-meteo.com/v1/forecast?latitude={','.join(lats)}&longitude={','.join(lons)}&hourly=temperature_2m,relative_humidity_2m,precipitation,surface_pressure,wind_speed_10m,wind_direction_10m&timezone=America%2FMexico_City&forecast_hours={horas_futuras}"
            
            # 🚨 FIX 2: 3 Intentos y Timeout más generoso (30 segundos)
            exito_lote = False
            for intento in range(3):
                try:
                    print(f"📡 Lote {idx+1}/{len(chunks)} - Intento {intento+1}...")
                    res = requests.get(url, timeout=30)
                    
                    if res.status_code == 200:
                        data = res.json()
                        if not isinstance(data, list): data = [data]
                        
                        for item in data:
                            full_results.append({
                                "lat": item['latitude'], 
                                "lon": item['longitude'], 
                                "hourly": item['hourly']
                            })
                        exito_lote = True
                        break # Salimos del bucle si fue exitoso
                    else:
                        print(f"⚠️ Error {res.status_code} en Lote {idx+1}")
                except Exception as e:
                    print(f"⚠️ Fallo de red en Lote {idx+1}: {e}")
                
                # 🚨 FIX 3: Backoff exponencial (Esperamos antes de volver a golpear la API)
                if intento < 2:
                    time.sleep(2 + intento) 
            
            if not exito_lote:
                print(f"❌ Abortando: El Lote {idx+1} falló después de 3 intentos.")
                return None
            
            # Respiro entre lotes exitosos
            time.sleep(1.5) 
            
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
    # --- 1. DETECCIÓN DEL TIPO DE GATILLO (Event-Driven) ---
    # ==========================================
    print("🧠 Despertando Motor de IA...")
    
    es_trabajo_pronostico = event.get('action') == 'run_forecast'
    
    # 🚨 NUEVO: ¿Me despertó S3 porque Lambda A subió un archivo nuevo?
    es_evento_s3 = False
    if 'Records' in event and len(event['Records']) > 0:
        if 's3' in event['Records'][0]:
            llave_modificada = event['Records'][0]['s3']['object']['key']
            if llave_modificada == 'latest_sacmex.json':
                es_evento_s3 = True
                print("🔔 TRIG: S3 detectó lluvia nueva. Forzando Modelo Live.")

    ahora = datetime.datetime.now(datetime.timezone.utc)
    
    with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
        malla_json = json.load(f)
        
    grid = gpd.GeoDataFrame.from_features(malla_json['features'])
    grid['lon'] = grid.geometry.x
    grid['lat'] = grid.geometry.y

    # ⛰️ FIX OROGRÁFICO: Extraemos la altitud y metadatos reales del GeoJSON de la ZMVM
    grid['altitud'] = [f['properties'].get('elevation', 2250) for f in malla_json['features']]
    grid['colonia'] = [f['properties'].get('colonia', 'Sin Colonia') for f in malla_json['features']]
    grid['municipio'] = [f['properties'].get('municipio', 'CDMX/Edomex') for f in malla_json['features']]
    grid['estado'] = [f['properties'].get('estado', 'CDMX') for f in malla_json['features']]
    
    # 🌲 Pre-calculamos el árbol espacial para cruzar Open-Meteo vs GeoJSON a velocidad luz
    from scipy.spatial import cKDTree
    tree_malla_global = cKDTree(grid[['lon', 'lat']].values)

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
            # 5. El Motor IA Atmosférico (De T+1 a T+6)
            for i in range(1, 7): 
                datos_hora = []
                hora_iso = forecast_raw[0]['hourly']['time'][i]
                
                for p in forecast_raw:
                    # 📍 GATILLO ESPACIAL: Buscamos a qué celda de nuestro GeoJSON pertenece este nodo
                    _, idx_nodo = tree_malla_global.query([p['lon'], p['lat']])
                    
                    # ⛰️ Heredamos la altitud REAL y constante de la malla (no de Open-Meteo)
                    alt_nodo = grid.iloc[idx_nodo]['altitud']
                    
                    datos_hora.append({
                        'lat': p['lat'], 
                        'lon': p['lon'], 
                        'rain': p['hourly']['precipitation'][i],
                        'wind_speed': p['hourly']['wind_speed_10m'][i],
                        'wind_dir': p['hourly']['wind_direction_10m'][i],
                        'altitud': alt_nodo
                    })
                
                print(f"🌪️ Simulando Dinámica Atmosférica para: {hora_iso}...")
                df_h = pd.DataFrame(datos_hora)
                
                # 🚨 FIX: Invocamos al NUEVO motor 3D (el que agregamos en el Corte 2)
                lluvia_proyectada = ejecutar_interpolacion_atmosferica(df_h, grid)
                
                # Empaquetamos la lluvia proyectada (Sparse Array) para enviarla al Frontend
                bloque_futuro["time_steps"][hora_iso] = [
                    {
                        "lat": round(grid.iloc[idx]['lat'], 5), 
                        "lon": round(grid.iloc[idx]['lon'], 5), 
                        "mm": float(lluvia_proyectada[idx])
                    }
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
    # CASO B: PRESENTE (Gatillo S3 o Cron de Limpieza)
    # ==========================================
    elif es_evento_s3 or event.get('action') == 'run_live' or event.get('action') == 'cleanup_cron':
        es_cron = event.get('action') == 'cleanup_cron'
        print(f"🚀 Iniciando Motor de Lluvia Actual (Modo: {'CRON Limpieza' if es_cron else 'Event-Driven S3'})...")
        
        # Constantes locales para los nuevos archivos S3
        S3_KEY_HISTORY = "history_24h.json"
        S3_KEY_ACCUMULATED = "accumulated_24h.json"

        # --- PASO 1: Descargar el Historial de 24h ---
        try:
            res_hist = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_HISTORY)
            historial_24h = json.loads(res_hist['Body'].read().decode('utf-8'))
        except Exception:
            print("⚠️ No hay historial de 24h previo, creando uno nuevo.")
            historial_24h = []

        nuevo_registro_valido = False
        final_payload = None

        if not es_cron:
            # === RUTA ACTIVA (Gatillo S3: Generar RBF) ===
            try:
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

            # 2. Lógica de Reposo (Manteniendo N < 2)
            if len(lluvia_activa) < 2:
                if max_rain_previo > 0.0:
                    print("🧹 FIN DE TORMENTA: Limpiando mapa...")
                else:
                    print("💤 ESTADO SLEEP: Mapa ya en cero.")
                    # 🚨 IMPORTANTE: En lugar de hacer return, cerramos la ruta activa
                    # para que la Lambda siga y aplique el barrendero si es necesario.
                    pass 
            else:
                print(f"⛈️ ESTADO ACTIVO: {len(lluvia_activa)} estaciones con lluvia.")

            # Si el mapa no está en cero absoluto, procedemos a calcular
            if len(lluvia_activa) >= 2 or max_rain_previo > 0.0:
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
                
                # 5. Inicializamos las celdas
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

                        if s_rain > 0:
                            output_cells[closest_idx]["rain_mm_h"] = float(s_rain)
                            output_cells[closest_idx]["risk"] = "Crítico" if alerta_status != "NORMAL" else "Moderado"
                            
                            if max_rain_actual > 0 and s_rain == max_rain_actual:
                                output_cells[closest_idx]["derivative_mm_min"] = float(round(derivada, 2))
                                output_cells[closest_idx]["alert_status"] = alerta_status

                    except Exception:
                        continue

                # ==========================================
                # 🚨 NUEVO: CRUCE ESPACIAL CON ZONAS DE RIESGO
                # ==========================================
                try:
                    zonas_de_riesgo = cargar_riesgos_historicos()
                    if zonas_de_riesgo and output_cells:
                        # Extraemos las coords del grid RBF que acabamos de generar
                        coords_malla = [[c['lat'], c['lon']] for c in output_cells]
                        arbol_malla = cKDTree(coords_malla)

                        for zona in zonas_de_riesgo:
                            # 1. Encontramos la celda que cubre este punto
                            _, idx_cercano = arbol_malla.query([zona['lat'], zona['lon']])
                            
                            celda_afectada = output_cells[idx_cercano]
                            lluvia_actual = celda_afectada.get('rain_mm_h', 0)
                            
                            # 2. EL GATILLO: Lluvia Fuerte (> 15mm/h) + Zona de Peligro
                            alerta_telegram = False
                            if lluvia_actual >= 15.0 and zona.get('vulnerability') in ['CRÍTICO', 'ALTO']:
                                alerta_telegram = True

                            # 3. Inyectamos la historia (Ley del peor escenario)
                            if 'riesgo_historico' not in celda_afectada or zona['historical_depth_m'] > celda_afectada['riesgo_historico']['tirante_historico_m']:
                                celda_afectada['riesgo_historico'] = {
                                    "id": zona['id'],
                                    "nombre": zona['name'],
                                    "vulnerabilidad": zona['vulnerability'],
                                    "tirante_historico_m": zona['historical_depth_m'],
                                    "anios_inundado": zona.get('recurrence_years', 1),
                                    "alerta_inundacion_en_vivo": alerta_telegram
                                }
                except Exception as e:
                    print(f"⚠️ Error al cruzar zonas de riesgo (Ignorado para no romper modelo): {e}")
                    
                # ==========================================
                # 🚲 7. SNAPSHOT DE ECOBICI (100% DINÁMICO)
                # ==========================================
                try:
                    print("🚲 Obteniendo catálogo estático y status dinámico de Ecobici...")
                    
                    # 1. Descargamos el catálogo de estaciones directo de la API (Siempre actualizado)
                    res_info = requests.get("https://gbfs.mex.lyftbikes.com/gbfs/en/station_information.json", timeout=4)
                    
                    # 2. Descargamos la disponibilidad de bicis
                    res_status = requests.get("https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json", timeout=4)
                    
                    if res_info.status_code == 200 and res_status.status_code == 200:
                        catalogo_estaciones = res_info.json().get('data', {}).get('stations', [])
                        status_data = res_status.json().get('data', {}).get('stations', [])
                        
                        # Diccionario ultra rápido de disponibilidad { "ID": bicis_disp }
                        status_dict = {str(st['station_id']): st['num_bikes_available'] for st in status_data}
                        
                        estaciones_mapeadas = 0
                        
                        # 3. Iteramos el catálogo que acabamos de descargar
                        for st in catalogo_estaciones:
                            st_id = str(st['station_id'])
                            lat_eco = float(st['lat'])
                            lon_eco = float(st['lon'])
                            
                            bicis_disp = status_dict.get(st_id, 0)
                            
                            # Optimización: Solo inyectamos a la malla si la estación tiene bicis disponibles
                            if bicis_disp > 0:
                                # Usamos el 'tree' de la malla para hallar a qué celda pertenece
                                _, idx_cercano = tree.query([lat_eco, lon_eco])
                                celda_destino = output_cells[idx_cercano]
                                
                                # Inicializamos el bloque de movilidad si es la primera bici en esa celda
                                if "movilidad" not in celda_destino:
                                    celda_destino["movilidad"] = {"ecobicis_en_celda": []}
                                    
                                celda_destino["movilidad"]["ecobicis_en_celda"].append({
                                    "id": st_id,
                                    "nombre": st.get('name', 'Estación Ecobici'),
                                    "disponibles": bicis_disp
                                })
                                estaciones_mapeadas += 1
                                
                        print(f"✅ {estaciones_mapeadas} estaciones activas asignadas espacialmente a sus celdas.")
                    else:
                        print(f"⚠️ Error en API Ecobici. Info: {res_info.status_code}, Status: {res_status.status_code}")
                        
                except Exception as e:
                    print(f"⚠️ Error al cruzar Ecobici con la malla (ignorado): {e}")

                # ==========================================
                # 8. EMPAQUETADO FINAL (Lluvia + Riesgo + Ecobici Integrado)
                # ==========================================
                final_payload = {
                    "timestamp": ahora.isoformat(),
                    "metadata": {
                        "lluvia_max": max_rain_actual,
                        "derivada_max": round(derivada, 3),
                        "alerta_global": alerta_status
                    },
                    "values": output_cells
                }

                # Guardamos el Snapshot "Live" (El presente)
                s3_client.put_object(
                    Bucket=S3_BUCKET, Key=S3_KEY_LATEST,
                    Body=json.dumps(final_payload), ContentType='application/json', CacheControl='max-age=60'
                )

                print("✅ Modelado PRESENTE completado y subido a S3.")
                
                # AÑADIMOS EL NUEVO REGISTRO AL HISTORIAL
                # (Al hacer esto, las bicis de hoy quedan congeladas en este frame)
                historial_24h.append(final_payload)
                nuevo_registro_valido = True
            # === FIN RUTA ACTIVA ===

        # --- PASO 2: EL BARRENDERO (Purga de >24h) ---
        ahora_ts = int(ahora.timestamp() * 1000)
        limite_24h_ms = 24 * 60 * 60 * 1000
        
        len_original = len(historial_24h)
        # Filtramos para quedarnos solo con lo que es más nuevo que 24 horas
        historial_24h = [
            reg for reg in historial_24h 
            if ahora_ts - int(datetime.datetime.fromisoformat(reg['timestamp']).timestamp() * 1000) <= limite_24h_ms
        ]
        
        datos_purgados = len_original - len(historial_24h)

        # --- PASO 3: CÁLCULO DEL ACUMULADO GLOBAL ---
        # Solo calculamos y subimos a S3 si entró un dato nuevo (tormenta) o si se borró un dato viejo (cron de limpieza)
        if nuevo_registro_valido or datos_purgados > 0:
            print(f"🧹 Actualizando Historial: {datos_purgados} regs viejos borrados. Total actual: {len(historial_24h)}")
            
            s3_client.put_object(
                Bucket=S3_BUCKET, Key=S3_KEY_HISTORY,
                Body=json.dumps(historial_24h), ContentType='application/json', CacheControl='max-age=300'
            )

            print("➕ Colapsando matrices para Acumulado Total...")
            
            if final_payload is None and len(historial_24h) > 0:
                final_payload = historial_24h[-1] 
            
            if final_payload:
                matriz_acumulada = final_payload.copy()
                matriz_acumulada['timestamp'] = ahora.isoformat()
                matriz_acumulada['metadata']['tipo'] = "ACUMULADO_24H"
                
                # Inicializamos todo en 0.0
                for celda in matriz_acumulada['values']:
                    celda['rain_mm_h'] = 0.0
                    celda['derivative_mm_min'] = 0.0

                # Sumamos la lluvia de cada celda
                for registro in historial_24h:
                    for i, celda in enumerate(registro['values']):
                        matriz_acumulada['values'][i]['rain_mm_h'] += float(celda.get('rain_mm_h', 0.0))

                for celda in matriz_acumulada['values']:
                    celda['rain_mm_h'] = round(celda['rain_mm_h'], 2)

                s3_client.put_object(
                    Bucket=S3_BUCKET, Key=S3_KEY_ACCUMULATED,
                    Body=json.dumps(matriz_acumulada), ContentType='application/json', CacheControl='max-age=300'
                )
                print("✅ Acumulado de 24h Guardado Exitosamente en S3.")
            else:
                print("☁️ Historial de 24h completamente vacío. Cielos despejados.")

        else:
            print("💤 Mantenimiento: No hubo ingresos nuevos ni purga de viejos. Ignorando cálculo.")

        return {"statusCode": 200, "body": "Model Executed Successfully"}
