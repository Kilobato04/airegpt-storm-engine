import json
import boto3
import os
import time
import datetime
import requests
import traceback
import pytz
import urllib3 # 🚨 FIX 1: Agregar esto


# 🚨 FIX 1 (continuación): Silenciar los warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURACIÓN ---
CACHE_FILE = '/tmp/lluvia_cdmx_cache.json'
CACHE_TTL_MINUTES = 10
CACHE_STALE_THRESHOLD = 5

# Cache global para persistencia entre invocaciones de Lambda (Warm Start)
global_cache = {
    'data': None,
    'lastUpdate': None,
    'isUpdating': False,
    'updateInterval': 1 * 60 * 1000,
    'requestCount': 0,
    'errorCount': 0,
    'lastSuccessfulUpdate': None,
    'consecutiveTimeouts': 0,
    'lastAttemptTime': 0,
    'dataFreshness': 'unknown',
    'maxAcceptableAge': 8 * 60 * 1000,
    'criticalAge': 15 * 60 * 1000,
    'lastDataChange': None,
    'redConfianzaPromedio': 0
}

class EarlyWarningSacmexAPI:
    def __init__(self):
        self.cache = global_cache
        self.debug = True
        self.baseURL = 'https://data.sacmex.cdmx.gob.mx'
        self.maxRetries = 3
        self.retryDelay = 3000
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; EarlyWarningCDMX/1.0; +https://rainappcdmx.netlify.app)',
            'Accept': 'application/json, */*',
            'Accept-Language': 'es-MX,es;q=0.9',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
        
        # Zona horaria de CDMX
        self.cdmx_tz = pytz.timezone('America/Mexico_City')
        
        self.initialize_cache()

    def log(self, message):
        if self.debug:
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            print(f"[EARLY-WARNING-API {timestamp}] {message}")

    def initialize_cache(self):
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, 'r') as f:
                    parsed = json.load(f)
                
                if parsed.get('data') and isinstance(parsed['data'], list) and len(parsed['data']) > 0:
                    age = int(time.time() * 1000) - parsed.get('lastUpdate', 0)
                    
                    if age < 30 * 60 * 1000: # 30 min max
                        self.cache.update(parsed)
                        self.update_data_freshness()
                        self.log(f"🔄 Cache restaurado: {len(parsed['data'])} estaciones, edad: {age//60000}min, freshness: {self.cache['dataFreshness']}")
                        return
            self.log("📝 Cache muy viejo o inválido, iniciando fresh")
        except Exception as e:
            self.log(f"📝 No cache encontrado: {str(e)}")

    def save_persisted_cache(self):
        try:
            if self.cache['data'] and len(self.cache['data']) > 0:
                payload = {
                    'data': self.cache['data'],
                    'lastUpdate': self.cache['lastUpdate'],
                    'lastSuccessfulUpdate': self.cache['lastSuccessfulUpdate'],
                    'errorCount': self.cache['errorCount'],
                    'consecutiveTimeouts': self.cache['consecutiveTimeouts'],
                    'dataFreshness': self.cache['dataFreshness'],
                    'lastDataChange': self.cache['lastDataChange'],
                    'savedAt': datetime.datetime.now(datetime.timezone.utc).isoformat()
                }
                with open(CACHE_FILE, 'w') as f:
                    json.dump(payload, f)
                self.log(f"💾 Cache guardado: {len(self.cache['data'])} estaciones")
        except Exception as e:
            self.log(f"❌ Error guardando cache: {str(e)}")

    def update_data_freshness(self):
        if not self.cache['lastUpdate']:
            self.cache['dataFreshness'] = 'unknown'
            return

        age = int(time.time() * 1000) - self.cache['lastUpdate']
        
        if age <= 5 * 60 * 1000:
            self.cache['dataFreshness'] = 'fresh'
        elif age <= self.cache['maxAcceptableAge']:
            self.cache['dataFreshness'] = 'acceptable'
        elif age <= CACHE_TTL_MINUTES * 60 * 1000:
            self.cache['dataFreshness'] = 'stale'
        else:
            self.cache['dataFreshness'] = 'critical'

    def is_cache_valid(self):
        if not self.cache['data'] or not self.cache['lastUpdate']:
            return False

        now = datetime.datetime.now(self.cdmx_tz)
        cache_age = int(time.time() * 1000) - self.cache['lastUpdate']
        
        minutes = now.minute
        next_update_minute = minutes + 1
        
        next_update = now.replace(second=0, microsecond=0)
        if next_update_minute >= 60:
            next_update = next_update + datetime.timedelta(hours=1)
            next_update = next_update.replace(minute=0)
        else:
            next_update = next_update.replace(minute=next_update_minute)

        should_update = now >= next_update or cache_age >= self.cache['updateInterval']
        return not should_update

    def get_cache_age(self):
        return int(time.time() * 1000) - self.cache['lastUpdate'] if self.cache['lastUpdate'] else float('inf')

    def get_data(self):
        self.cache['requestCount'] += 1
        self.update_data_freshness()
        
        self.log(f"📊 Request #{self.cache['requestCount']} | Freshness: {self.cache['dataFreshness']}")

        if self.is_cache_valid() and self.cache['dataFreshness'] == 'fresh':
            self.log("✅ Cache ultra-fresco - perfecto para alerta temprana")
            return self.build_response(self.cache['data'], True, 'fresh_early_warning')

        if not self.cache['isUpdating']:
            self.update_cache_sync() # En Python forzamos la sincronía para Lambda

        if self.cache['data'] and self.cache['lastUpdate']:
            age_minutes = self.get_cache_age() // 60000
            
            if age_minutes > CACHE_TTL_MINUTES:
                self.log("🚨 Cache MUY VIEJO - FORZANDO renovación")
                self.update_cache_sync()
                return self.build_response(self.cache['data'], True, 'forced_refresh') if self.cache['data'] else self.build_emergency_response()
            
            if self.cache['dataFreshness'] == 'acceptable':
                return self.build_response(self.cache['data'], False, 'acceptable_early_warning')
            elif self.cache['dataFreshness'] == 'stale':
                return self.build_response(self.cache['data'], False, 'stale_warning')
            elif self.cache['dataFreshness'] == 'critical':
                return self.build_response(self.cache['data'], False, 'critical_unreliable')

        self.update_cache_sync()
        return self.build_response(self.cache['data'], True, 'fresh_after_wait') if self.cache['data'] else self.build_emergency_response()

    def update_cache_sync(self):
        if self.cache['isUpdating']:
            return

        if self.cache['consecutiveTimeouts'] >= 4:
            backoff_minutes = min(self.cache['consecutiveTimeouts'] - 1, 3)
            time_since = int(time.time() * 1000) - self.cache['lastAttemptTime']
            backoff_time = backoff_minutes * 60 * 1000
            
            if time_since < backoff_time:
                self.log(f"🚫 Circuit breaker activo. Backoff {backoff_time//60000}min")
                return

        self.cache['isUpdating'] = True
        self.cache['lastAttemptTime'] = int(time.time() * 1000)
        
        try:
            # 🚨 NUEVO CEREBRO CENTRAL: Une S3 (SACMEX) con API (CHAAK)
            fresh_data = []
            
            # 1. Traer SACMEX crudo desde S3
            sacmex_data = self.fetch_sacmex_from_s3()
            if sacmex_data:
                fresh_data.extend(sacmex_data)
            
            # 2. Traer CHAAK en vivo
            try:
                chaak_data = self.fetch_chaak_station()
                if chaak_data:
                    self.log("✅ Estación CHAAK detectada y Online. Añadiendo al payload.")
                    fresh_data.append(chaak_data)
            except Exception as e:
                self.log(f"⚠️ Error menor consultando CHAAK: {e}")

            if fresh_data and len(fresh_data) > 0:
                
                # 🚨 INYECTAMOS VIRTUALES ANTES DE CHECAR CAMBIOS 🚨
                fresh_data = self.inyectar_estaciones_virtuales(fresh_data)
                
                has_changed = self.detect_data_changes(fresh_data)
                
                self.cache['data'] = fresh_data
                self.cache['lastUpdate'] = int(time.time() * 1000)
                self.cache['lastSuccessfulUpdate'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                self.cache['errorCount'] = 0
                self.cache['consecutiveTimeouts'] = 0
                
                if has_changed:
                    self.cache['lastDataChange'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    self.log("✅ DATOS CAMBIARON: SACMEX actualizó datos reales")
                    
                    self.update_data_freshness()
                    self.save_persisted_cache()
                    
                    self.log_to_sheets(fresh_data, "EARLY_WARNING_OK")
                    
                    # 🚨 EL GATILLO: Al subir esto a S3, S3 despertará a Lambda B
                    payload_s3 = self.build_response(fresh_data, False, 'fresh_background_update')
                    self.upload_to_s3(payload_s3)
                else:
                    # Si no hay cambios, solo logueamos en Google Sheets y terminamos
                    self.log("💤 SIN CAMBIOS: No se sube a S3. Lambda B descansa.")
                    self.update_data_freshness()
                    self.save_persisted_cache()
                    self.log_to_sheets(fresh_data, "NO_CHANGES_DETECTED")
            else:
                self.cache['errorCount'] += 1
                
        except Exception as e:
            self.cache['errorCount'] += 1
            error_str = str(e).lower()
            if 'timeout' in error_str or 'connect' in error_str:
                self.cache['consecutiveTimeouts'] += 1
                self.log(f"⏱️ TIMEOUT CRÍTICO #{self.cache['consecutiveTimeouts']}: {str(e)}")
            else:
                self.log(f"❌ Error conexión: {str(e)}")
                
            if self.cache['errorCount'] >= 8:
                self.cache['errorCount'] = 2
                
        finally:
            self.cache['isUpdating'] = False

    def upload_to_s3(self, response_payload):
        try:
            s3 = boto3.client('s3')
            # 🚨 CAMBIA ESTO por el nombre exacto de tu bucket
            bucket_name = 'airegpt-storm-data' 
            
            # S3 requiere un string formateado, no un diccionario de Python
            body_data = json.dumps(response_payload)
            
            s3.put_object(
                Bucket=bucket_name,
                Key='latest_sacmex.json',
                Body=body_data,
                ContentType='application/json',
                # 🚨 CRÍTICO: Esto evita que el navegador del usuario guarde un archivo viejo
                CacheControl='max-age=0, no-cache, no-store, must-revalidate',
            )
            self.log("☁️ S3 UPDATE: latest_sacmex.json subido exitosamente.")
        except Exception as e:
            self.log(f"❌ Error subiendo a S3: {e}")
    
    def log_to_sheets(self, stations, sys_stat):
        try:
            ahora_ms = int(time.time() * 1000)
            # Calculamos frecuencia con un decimal
            frecuencia = round((ahora_ms - self.cache.get('lastAttemptTime', ahora_ms)) / 1000, 1)
            
            # Aseguramos que siempre haya una lista de valores numéricos
            lluvias = [float(s.get('acumulado_actual', 0)) for s in stations]
            max_rain = round(max(lluvias), 2) if lluvias else 0.0
            
            fecha_str = datetime.datetime.now(self.cdmx_tz).strftime("%d/%m/%Y, %H:%M:%S")
            
            # Construimos el mini-JSON de estaciones con lluvia > 0
            est_dict = {str(s['id']): float(s['acumulado_actual']) 
                        for s in stations if float(s.get('acumulado_actual', 0)) > 0}
            
            # Forzamos que sea un string JSON para que Google no lo interprete mal
            mini_json_str = json.dumps(est_dict) if est_dict else "{}"
            
            payload = {
                "fecha": fecha_str,
                "estado": sys_stat,
                "kpi_salud": float(self.cache.get('redConfianzaPromedio', 0)),
                "frecuencia_muestreo": frecuencia,
                "lluvia_max": max_rain,
                "estaciones_activas": mini_json_str
            }
            
            # Verifica que esta URL sea la de la "Nueva Implementación"
            requests.post('https://script.google.com/macros/s/AKfycbyUkZw2lrADxGMPJOrlLqej_6QD5e_pRS66ZmkDolZrA-vcef3o-MupM6k45t-xABYt/exec', json=payload, timeout=5)
            
        except Exception as e:
            self.log(f"⚠️ Error en log_to_sheets: {e}")
    
    def detect_data_changes(self, new_data):
        if not self.cache['data'] or len(self.cache['data']) != len(new_data):
            return True
            
        old_checksum = "|".join(sorted([f"{s.get('estacion_id','_')}:{s.get('acumulado_actual',0)}:{s.get('acumulado_desde',0)}" for s in self.cache['data']]))
        new_checksum = "|".join(sorted([f"{s.get('estacion_id','_')}:{s.get('acumulado_actual',0)}:{s.get('acumulado_desde',0)}" for s in new_data]))
        
        return old_checksum != new_checksum

    def float_safe(self, value, default=0.0):
        if value is None or value == '':
            return default
        try:
            return float(value)
        except ValueError:
            return default

    def calculate_intensity(self, val):
        if 0.1 <= val <= 3.0: return 'VERDE'
        if 3.1 <= val <= 7.0: return 'AMARILLO'
        if 7.1 <= val <= 13.0: return 'NARANJA'
        if 13.1 <= val <= 20.0: return 'ROJO'
        if val >= 20.1: return 'PURPURA'
        return 'BLANCO'

    def inyectar_estaciones_virtuales(self, estaciones_reales):
        import math

        # 1. Nuestro catálogo calibrado definitivo (10 Nodos)
        virtuales = {
            "VIRT_070": {"lat": 19.390924, "lon": -99.262071},
            "VIRT_040": {"lat": 19.245071, "lon": -99.164461},
            "VIRT_097": {"lat": 19.280115, "lon": -99.194532},
            "VIRT_102": {"lat": 19.316158, "lon": -99.283203},
            "VIRT_012": {"lat": 19.277291, "lon": -99.285157},
            "VIRT_106": {"lat": 19.306704, "lon": -99.141086},
            "VIRT_061": {"lat": 19.343439, "lon": -99.253465},
            "VIRT_110": {"lat": 19.286203, "lon": -99.246332},
            "VIRT_019": {"lat": 19.414350, "lon": -99.305507},
            "VIRT_041": {"lat": 19.206379, "lon": -99.203736}
        }

        # 2. Filtramos solo físicas con buena salud para evitar retroalimentación
        fisicas_validas = [
            st for st in estaciones_reales 
            if st.get('auditoria', {}).get('confianza_index', 1.0) > 0.0
        ]
        
        if not fisicas_validas:
            return estaciones_reales
            
        ahora_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        # 3. Calculamos IDW por cada nodo virtual
        for v_id, coords in virtuales.items():
            distancias = []
            
            # Medir distancia contra todas las físicas
            for st in fisicas_validas:
                d_lat = coords['lat'] - float(st['latitud'])
                d_lon = coords['lon'] - float(st['longitud'])
                dist_km = math.sqrt(d_lat**2 + d_lon**2) * 111.3
                
                # Evitar división por cero
                distancias.append({
                    'val': float(st.get('acumulado_actual', 0.0)),
                    'dist': max(dist_km, 0.1) 
                })
            
            # Tomar los 4 vecinos más cercanos (k=4)
            vecinos = sorted(distancias, key=lambda x: x['dist'])[:4]
            
            # 🚨 FIX 1: Candado de Distancia (Cut-Off 3.5 km)
            # Solo permitimos que el nodo virtual copie valores si hay sensores cerca.
            vecinos_validos = [v for v in vecinos if v['dist'] <= 3.5]
            
            # Matemática IDW (Python puro)
            sum_pesos = 0.0
            sum_valores = 0.0
            for vec in vecinos_validos:
                peso = 1.0 / (vec['dist'] ** 2)
                sum_pesos += peso
                sum_valores += vec['val'] * peso
                
            # Si no hay vecinos válidos en el radio de 3.5 km, la lluvia es 0.0
            val_idw = sum_valores / sum_pesos if sum_pesos > 0 else 0.0
            
            # 4. Empaquetar como estación oficial
            estaciones_reales.append({
                "id": v_id,
                "nombre": f"NODO {v_id}",
                "latitud": coords['lat'],
                "longitud": coords['lon'],
                "alcaldia": "RED_VIRTUAL",
                "acumulado_actual": round(val_idw, 2),
                "acumulado_desde_6am": round(val_idw, 2), 
                "precipitacion_horaria": 0.0,
                "intensidad": self.calculate_intensity(val_idw),
                "auditoria": {
                    "confianza_index": 1.0,
                    "alertas": ["NODO_VIRTUAL_IDW"],
                    "frescura_dato_segundos": 0
                },
                "ultima_actualizacion": ahora_iso,
                "cache_timestamp_ISO": ahora_iso,
                "origen": "MODELO_IDW"
            })
            
        return estaciones_reales

    def fetch_sacmex_from_s3(self):
        try:
            s3 = boto3.client('s3')
            bucket_name = 'airegpt-storm-data'
            response = s3.get_object(Bucket=bucket_name, Key='test_monitoreo/sacmex_lenovo.json')
            content = response['Body'].read().decode('utf-8')
            sacmex_data = json.loads(content)
            
            # 1. Desenvolver el JSON
            lista_estaciones = []
            if isinstance(sacmex_data, dict) and 'data' in sacmex_data:
                lista_estaciones = sacmex_data['data']
            elif isinstance(sacmex_data, list):
                lista_estaciones = sacmex_data

            # 2. 🚨 Adaptador: Normalizar la estructura al estándar que espera el Modelo
            estaciones_normalizadas = []
            for st in lista_estaciones:
                lluvia = float(st.get('acumulado_actual', 0.0))
                
                est_norm = {
                    "id": str(st.get('id', '0')),
                    "estacion_id": str(st.get('id', '0')), # Crítico para detect_data_changes
                    "nombre": st.get('nombre', 'Desconocido'),
                    "latitud": float(st.get('latitud', 0.0)),
                    "longitud": float(st.get('longitud', 0.0)),
                    "alcaldia": st.get('alcaldia', 'CDMX'), # Fallback para el frontend
                    "acumulado_actual": lluvia,
                    "acumulado_desde": lluvia, # Crítico para el checksum
                    "intensidad": self.calculate_intensity(lluvia),
                    "origen": st.get('origen', 'SACMEX_S3'),
                    "ultima_actualizacion": st.get('ultima_actualizacion', ''),
                    "auditoria": {
                        "confianza_index": 1.0,
                        "alertas": []
                    }
                }
                estaciones_normalizadas.append(est_norm)
                
            self.log(f"✅ SACMEX extraído y NORMALIZADO de S3: {len(estaciones_normalizadas)} estaciones.")
            return estaciones_normalizadas
            
        except Exception as e:
            self.log(f"❌ Error leyendo SACMEX de S3: {e}")
            return []
            
    def fetch_chaak_station(self):
        base_data = {
            "id": "CHAAK-01", "nombre": "SMAA CHAAK (Ibero)",
            "latitud": 19.37, "longitud": -99.26,
            "alcaldia": "ALVARO OBREGON", "origen": "SMABILITY_HARDWARE"
        }
        try:
            token = '9b56e023d84c4c0e9af2d0ee95549392'
            
            end = datetime.datetime.now(self.cdmx_tz)
            # 🚨 FIX 1: Ventana de 5 minutos exactos (Sincronizado con SACMEX)
            start = end - datetime.timedelta(minutes=5)
            
            fmt = "%Y-%m-%d %H:%M:%S"
            dt_start = start.strftime(fmt).replace(" ", "%20")
            dt_end = end.strftime(fmt).replace(" ", "%20")
            
            url_base = f"https://smability.sidtecmx.com/SmabilityAPI/GetData?token={token}&dtStart={dt_start}&dtEnd={dt_end}&idSensor="
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Connection": "close"
            }
            
            def get_sensor_data(sensor_id):
                try:
                    res = requests.get(url_base + str(sensor_id), headers=headers, timeout=10, verify=False)
                    time.sleep(0.5) 
                    raw = res.json() if res.status_code == 200 else []
                    
                    if isinstance(raw, dict):
                        return raw.get('data', raw.get('Data', []))
                    elif isinstance(raw, list):
                        return raw
                    return []
                except Exception as e:
                    self.log(f"Micro-falla en CHAAK ({sensor_id}): {e}")
                    return []

            # 🚨 EXTRACCIÓN DE 5 MINUTOS
            raw_rain = get_sensor_data(23) # Intensidad Lluvia
            raw_wind = get_sensor_data(19) # Velocidad Viento
            raw_deg  = get_sensor_data(18) # Dirección Viento

            def process_rain(sensor_list):
                valid_data = [d for d in sensor_list if isinstance(d, dict) and 'Data' in d]
                if valid_data:
                    # Lluvia: El valor MÁXIMO en los 5 min
                    val = max([self.float_safe(d.get('Data', 0)) for d in valid_data])
                    return val, valid_data[-1].get('TimeStamp', "OFFLINE")
                return 0.0, "OFFLINE"

            def process_wind(sensor_list):
                valid_data = [d for d in sensor_list if isinstance(d, dict) and 'Data' in d]
                if valid_data:
                    # Viento: El valor PROMEDIO sostenido en los 5 min
                    vals = [self.float_safe(d.get('Data', 0)) for d in valid_data]
                    val = sum(vals) / len(vals)
                    return val, valid_data[-1].get('TimeStamp', "OFFLINE")
                return 0.0, "OFFLINE"

            max_lluvia, f_lluvia = process_rain(raw_rain)
            wind_speed, f_viento = process_wind(raw_wind)
            wind_deg, f_deg = process_wind(raw_deg)
            
            fechas_validas = [f for f in [f_lluvia, f_viento, f_deg] if f != "OFFLINE"]
            ultima_fecha = max(fechas_validas) if fechas_validas else "OFFLINE"

            return {
                **base_data,
                "acumulado_actual": round(max_lluvia, 2),
                "viento_velocidad": round(wind_speed, 1),
                "viento_direccion": round(wind_deg, 0),
                "intensidad": self.calculate_intensity(max_lluvia),
                "auditoria": {"confianza_index": 1.0 if ultima_fecha != "OFFLINE" else 0.0, "alertas": [], "frescura_dato_segundos": 0},
                "ultima_actualizacion": ultima_fecha,
                "cache_timestamp_ISO": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            
        except Exception as e:
            self.log(f"❌ Error crítico en CHAAK: {e}")
            return {
                **base_data,
                "acumulado_actual": 0.0, 
                "viento_velocidad": 0.0, 
                "viento_direccion": 0.0,
                "intensidad": "OFFLINE", 
                "auditoria": {"confianza_index": 0.0, "alertas": [f"Falla: {str(e)[:30]}"], "frescura_dato_segundos": 999999},
                "ultima_actualizacion": "OFFLINE",
                "cache_timestamp_ISO": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }

    def generate_weather_alerts(self, stations, max_rain):
        alerts = []
        max_r = float(max_rain)
        
        if max_r >= 20.1:
            alerts.append({"level": "CRITICAL", "type": "TORRENTIAL_RAINFALL", "sacmex_color": "PURPLE", "message": f"Lluvia TORRENCIAL: {max_r}mm"})
        elif max_r >= 13.1:
            alerts.append({"level": "CRITICAL", "type": "INTENSE_RAINFALL", "sacmex_color": "RED", "message": f"Lluvia INTENSA: {max_r}mm"})
        elif max_r >= 7.1:
            alerts.append({"level": "WARNING", "type": "HEAVY_RAINFALL", "sacmex_color": "ORANGE", "message": f"Lluvia FUERTE: {max_r}mm"})
        elif max_r >= 3.1:
            alerts.append({"level": "WATCH", "type": "MODERATE_RAINFALL", "sacmex_color": "YELLOW", "message": f"Lluvia MODERADA: {max_r}mm"})
            
        return alerts


    def build_response(self, stations, is_cache, cache_status='fresh'):
        if not stations: return self.build_emergency_response()
        
        tot = len(stations)
        lluvias = [s['acumulado_actual'] for s in stations]
        max_rain = f"{max(lluvias):.2f}" if lluvias else "0.00"
        avg_rain = f"{(sum(lluvias)/tot):.2f}" if lluvias else "0.00"
        
        sys_stat = 'EARLY_WARNING_OK'
        if self.cache['dataFreshness'] == 'fresh': sys_stat = 'EARLY_WARNING_OPTIMAL'
        elif self.cache['dataFreshness'] == 'critical': sys_stat = 'EARLY_WARNING_COMPROMISED'

        return {
            "success": True,
            "data": stations,
            "api_status": {
                "status": sys_stat,
                "data_quality": {"total_stations": tot, "max_rainfall_mm": max_rain, "average_rainfall_mm": avg_rain},
                "cache_health": {"served_from_cache": is_cache, "cache_status": cache_status, "data_freshness": self.cache['dataFreshness']}
            },
            "early_warning_status": {
                "alert_level": "NORMAL" if float(max_rain) < 3.1 else "WARNING",
                "weather_alerts": self.generate_weather_alerts(stations, max_rain),
                "is_reliable_for_alerts": self.cache['dataFreshness'] in ['fresh', 'acceptable']
            },
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "metadata": {"cache_info": {"served_from_cache": is_cache}}
        }

    def build_emergency_response(self):
        return {"success": False, "api_status": {"status": "EMERGENCY_MODE"}, "data": []}

def handler(event, context):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=60'
    }
    
    # AWS Function URLs usan rawPath, API Gateway usa path
    ruta = event.get('rawPath', event.get('path', '/'))
    query = event.get('queryStringParameters', {}) or {}
    
    if event.get('httpMethod') == 'OPTIONS':
        return {"statusCode": 200, "headers": headers, "body": ""}

    api = EarlyWarningSacmexAPI()

    # ==========================================
    # RUTA: EL FUTURO (/forecast)
    # ==========================================
    if ruta == '/forecast' or 'forecast' in query.get('type', ''):
        res = api.get_forecast_data()
        return {"statusCode": 200 if res['success'] else 500, "headers": headers, "body": json.dumps(res)}

    # ==========================================
    # RUTA: EL PRESENTE (/)
    # ==========================================
    try:
        res = api.get_data()
        return {"statusCode": 200, "headers": headers, "body": json.dumps(res)}
    except Exception as e:
        err = api.build_emergency_response()
        err['critical_error'] = str(e)
        return {"statusCode": 500, "headers": headers, "body": json.dumps(err)}
