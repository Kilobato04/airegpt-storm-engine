import json
import os
import time
import datetime
import requests
import traceback
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            # EJECUCIÓN PARALELA: SACMEX y CHAAK al mismo tiempo
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_sacmex = executor.submit(self.fetch_from_sacmex)
                future_chaak = executor.submit(self.fetch_chaak_station)
                
                try:
                    fresh_data = future_sacmex.result() # Lanza excepción si falla
                except Exception as e:
                    raise e
                    
                try:
                    chaak_data = future_chaak.result()
                    if chaak_data:
                        self.log("✅ Estación CHAAK detectada y Online. Añadiendo.")
                        fresh_data.append(chaak_data)
                except Exception as e:
                    self.log(f"⚠️ Error menor consultando CHAAK: {e}")

            if fresh_data and len(fresh_data) > 0:
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
                # 🟢 AQUÍ DISPARAMOS EL LOG A GOOGLE SHEETS
                # Solo se dispara cuando la actualización en background fue un éxito
                estado_actual = "EARLY_WARNING_OK" if has_changed else "NO_CHANGES_DETECTED"
                self.log_to_sheets(fresh_data, estado_actual)
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

    def log_to_sheets(self, stations, sys_stat):
        try:
            ahora_ms = int(time.time() * 1000)
            frecuencia = round((ahora_ms - self.cache.get('lastAttemptTime', ahora_ms)) / 1000, 1)
            
            lluvias = [s['acumulado_actual'] for s in stations]
            max_rain = max(lluvias) if lluvias else 0.0
            
            # Formato de fecha limpio y homologado
            fecha_str = datetime.datetime.now(self.cdmx_tz).strftime("%d/%m/%Y, %H:%M:%S")
            
            # Construimos el "mini-JSON" de estaciones activas que tenías en Netlify
            est_dict = {s['id']: s['acumulado_actual'] for s in stations if s['acumulado_actual'] > 0}
            mini_json_estaciones = json.dumps(est_dict) if est_dict else "{}"
            
            payload = {
                "fecha": fecha_str,
                "estado": sys_stat,
                "kpi_salud": self.cache.get('redConfianzaPromedio', 0),
                "frecuencia_muestreo": frecuencia,
                "lluvia_max": max_rain,
                "estaciones_activas": mini_json_estaciones
            }
            
            # URL de despliegue de tu Apps Script
            requests.post('https://script.google.com/macros/s/AKfycbyUkZw2lrADxGMPJOrlLqej_6QD5e_pRS66ZmkDolZrA-vcef3o-MupM6k45t-xABYt/exec', json=payload, timeout=3)
        except Exception as e:
            self.log(f"⚠️ No se pudo enviar log a Google Sheets: {e}")

    
    def detect_data_changes(self, new_data):
        if not self.cache['data'] or len(self.cache['data']) != len(new_data):
            return True
            
        old_checksum = "|".join(sorted([f"{s.get('estacion_id','_')}:{s.get('acumulado_actual',0)}:{s.get('acumulado_desde',0)}" for s in self.cache['data']]))
        new_checksum = "|".join(sorted([f"{s.get('estacion_id','_')}:{s.get('acumulado_actual',0)}:{s.get('acumulado_desde',0)}" for s in new_data]))
        
        return old_checksum != new_checksum

    def fetch_from_sacmex(self):
        endpoint = '/pluviometros/index.php/lluvia/get_pluviometros'
        last_error = None

        for attempt in range(1, self.maxRetries + 1):
            try:
                timeout_val = 15 + (attempt - 1) * 2
                self.log(f"📡 FETCH SACMEX ({attempt}/{self.maxRetries}) Timeout: {timeout_val}s")
                
                response = requests.get(self.baseURL + endpoint, headers=self.headers, timeout=timeout_val)
                response.raise_for_status()
                data = response.json()
                
                if isinstance(data, list) and len(data) > 0:
                    self.log(f"✅ SACMEX ÉXITO: {len(data)} estaciones")
                    return self.process_raw_data(data)
                else:
                    raise ValueError("Array vacío de SACMEX")
                    
            except Exception as e:
                last_error = e
                self.log(f"❌ Fetch {attempt} FALLÓ: {str(e)}")
                if attempt < self.maxRetries:
                    time.sleep((self.retryDelay + (attempt * 1000)) / 1000.0)

        raise last_error

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

    def process_raw_data(self, raw_data):
        processed = []
        system_now = int(time.time() * 1000)
        
        for idx, punto in enumerate(raw_data):
            try:
                acum_actual = self.float_safe(punto.get('acumulado_actual'))
                acum_desde = self.float_safe(punto.get('acumulado_desde'))
                ts_orig = punto.get('ultimaActualizacion', '').strip()
                
                sanity_score = 1.0
                alertas = []
                
                # Parse timezone
                try:
                    fecha_obj = datetime.datetime.strptime(ts_orig, "%Y-%m-%d %H:%M:%S")
                    fecha_obj = self.cdmx_tz.localize(fecha_obj)
                    data_age = system_now - int(fecha_obj.timestamp() * 1000)
                except:
                    data_age = 0
                    
                if data_age > 10 * 60 * 1000:
                    sanity_score -= 0.3
                    alertas.append("HIGH_LATENCY")
                    
                station = {
                    'id': str(punto.get('id', f'EST_{idx}')),
                    'nombre': punto.get('nombre', f'Pluvio {idx}'),
                    'latitud': self.float_safe(punto.get('latitud', punto.get('coordenadas', [0,0])[0])),
                    'longitud': self.float_safe(punto.get('longitud', punto.get('coordenadas', [0,0])[1])),
                    'alcaldia': punto.get('municipality', 'CDMX'),
                    'acumulado_actual': round(acum_actual, 2),
                    'acumulado_desde_6am': round(acum_desde, 2),
                    'precipitacion_horaria': round(max(0.0, acum_actual - acum_desde), 2),
                    'intensidad': self.calculate_intensity(acum_actual),
                    'auditoria': {
                        'confianza_index': round(max(0.0, sanity_score), 2),
                        'alertas': alertas,
                        'frescura_dato_segundos': max(0, data_age // 1000)
                    },
                    'hora_actual_ISO': fecha_obj.isoformat() if 'fecha_obj' in locals() else "",
                    'ultima_actualizacion': ts_orig,
                    'cache_timestamp_ISO': datetime.datetime.now(datetime.timezone.utc).isoformat()
                }
                processed.append(station)
            except Exception as e:
                self.log(f"Error procesando estación {idx}: {e}")

        if processed:
            avg_conf = sum(s['auditoria']['confianza_index'] for s in processed) / len(processed)
            self.cache['redConfianzaPromedio'] = round(avg_conf, 2)
            
        return sorted(processed, key=lambda x: x['acumulado_actual'], reverse=True)

    def fetch_chaak_station(self):
        base_data = {
            "id": "CHAAK-01", "nombre": "SMAA CHAAK (Ibero)",
            "latitud": 19.37, "longitud": -99.26,
            "alcaldia": "ALVARO OBREGON", "origen": "SMABILITY_HARDWARE"
        }
        try:
            token = '9b56e023d84c4c0e9af2d0ee95549392'
            
            end = datetime.datetime.now(self.cdmx_tz)
            start = end - datetime.timedelta(minutes=15)
            
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
                    res = requests.get(url_base + str(sensor_id), headers=headers, timeout=10)
                    time.sleep(0.5) # Respiro vital para el firewall IIS
                    raw = res.json() if res.status_code == 200 else []
                    
                    if isinstance(raw, dict):
                        return raw.get('data', raw.get('Data', []))
                    elif isinstance(raw, list):
                        return raw
                    return []
                except Exception as e:
                    self.log(f"Micro-falla en CHAAK ({sensor_id}): {e}")
                    return []

            # 🚨 SOLO LOS 3 CRÍTICOS PARA CFD (Dinámica de Fluidos)
            raw_rain = get_sensor_data(24) # Lluvia
            raw_wind = get_sensor_data(19) # Velocidad Viento
            raw_deg  = get_sensor_data(18) # Dirección Viento

            def extract_last_value(sensor_list):
                valid_data = [d for d in sensor_list if isinstance(d, dict) and 'Data' in d]
                if valid_data:
                    return self.float_safe(valid_data[-1].get('Data', 0)), valid_data[-1].get('TimeStamp', "OFFLINE")
                return 0.0, "OFFLINE"

            def extract_max_value(sensor_list):
                valid_data = [d for d in sensor_list if isinstance(d, dict) and 'Data' in d]
                if valid_data:
                    return max([self.float_safe(d.get('Data', 0)) for d in valid_data]), valid_data[-1].get('TimeStamp', "OFFLINE")
                return 0.0, "OFFLINE"

            max_lluvia, f_lluvia = extract_max_value(raw_rain)
            wind_speed, f_viento = extract_last_value(raw_wind)
            wind_deg, f_deg = extract_last_value(raw_deg)
            
            fechas_validas = [f for f in [f_lluvia, f_viento, f_deg] if f != "OFFLINE"]
            ultima_fecha = max(fechas_validas) if fechas_validas else "OFFLINE"

            # 🚨 OBJETO LIMPIO: Sin campos fantasma
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
            
    def fetch_open_meteo(self):
        try:
            # Cuadrícula de 100 puntos en CDMX/Edomex
            latS, latN, lonW, lonE = 19.155, 19.772, -99.352, -98.867
            steps = 9
            lats, lons = [], []
            for i in range(steps + 1):
                lat = latS + (i * (latN - latS) / steps)
                for j in range(steps + 1):
                    lon = lonW + (j * (lonE - lonW) / steps)
                    lats.append(f"{lat:.4f}")
                    lons.append(f"{lon:.4f}")
                    
            horas_futuras = 6
            url = f"https://api.open-meteo.com/v1/forecast?latitude={','.join(lats)}&longitude={','.join(lons)}&hourly=temperature_2m,relative_humidity_2m,precipitation,surface_pressure,wind_speed_10m,wind_direction_10m&timezone=America%2FMexico_City&forecast_hours={horas_futuras}"
            
            # 🚨 FIX: Subimos el timeout a 25s. Open-Meteo sufre armando 100 puntos.
            res = requests.get(url, timeout=25) 
            
            # Imprimimos el error real si Open-Meteo nos rechaza (ej. 429 Too Many Requests)
            if res.status_code != 200:
                self.log(f"⚠️ Open-Meteo rechazó la petición con HTTP {res.status_code}")
                
            res.raise_for_status()
            data = res.json()
            
            if isinstance(data, list):
                return [{"lat": n['latitude'], "lon": n['longitude'], "hourly": n['hourly']} for n in data]
            return None
            
        except Exception as e:
            self.log(f"❌ Error OpenMeteo: {e}")
            return None

    def get_forecast_data(self):
        grid = self.fetch_open_meteo()
        return {"success": True, "data": grid} if grid else {"success": False, "error": "Error Open-Meteo"}

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
