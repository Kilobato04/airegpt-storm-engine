import json
import boto3
import math
import datetime

S3_BUCKET = "airegpt-storm-data"
S3_KEY_LATEST = "latest_model.json"
S3_KEY_FORECAST = "latest_forecast.json"

s3_client = boto3.client('s3')

def get_intensidad(mm):
    if mm == 0: return "Sin lluvia"
    if mm <= 3.0: return "Ligera"
    if mm <= 7.0: return "Moderada"
    if mm <= 13.0: return "Fuerte"
    if mm <= 20.0: return "Intensa"
    return "Torrencial"

def lambda_handler(event, context):
    headers = {'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json'}
    query = event.get('queryStringParameters', {}) or {}
    
    try:
        user_lat = float(query.get('lat'))
        user_lon = float(query.get('lon'))
    except:
        return {"statusCode": 400, "headers": headers, "body": json.dumps({"status": "error", "message": "lat/lon required"})}

    try:
        # 1. Cargar Datos (Presente y Forecast)
        resp_presente = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
        presente_data = json.loads(resp_presente['Body'].read().decode('utf-8'))
        
        try:
            resp_forecast = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_FORECAST)
            forecast_data = json.loads(resp_forecast['Body'].read().decode('utf-8'))
        except:
            forecast_data = {"time_steps": {}}

        # 2. Localizar la celda del usuario (Latest Model)
        celdas = presente_data.get('values', [])
        # Usamos math.hypot para encontrar la más cercana al GPS del usuario
        celda_cercana = min(celdas, key=lambda c: math.hypot(c['lat'] - user_lat, c['lon'] - user_lon))
        
        # Coordenadas de referencia redondeadas para el cruce con el Forecast
        ref_lat = round(celda_cercana['lat'], 5)
        ref_lon = round(celda_cercana['lon'], 5)

        # 3. Procesar Ecobici (Top 3 por Disponibilidad con Ubicación Exacta)
        movilidad_obj = None
        if 'movilidad' in celda_cercana:
            # Ordenamos estaciones por cantidad de bicis (Descendente)
            estaciones = sorted(celda_cercana['movilidad'].get('ecobicis_en_celda', []), 
                                key=lambda x: x.get('disponibles', 0), reverse=True)
            if estaciones:
                movilidad_obj = {
                    "estaciones_en_celda": len(estaciones),
                    "total_bicicletas": sum(s.get('disponibles', 0) for s in estaciones),
                    "mejores_opciones": [{
                        "nombre": s['nombre'],
                        "disponibles": s['disponibles'],
                        "lat": s['lat'],
                        "lon": s['lon']
                    } for s in estaciones[:3]]
                }

        # 4. EXTRAER VECTOR DE PRONÓSTICO (6 Horas Futuras)
        pronostico_6h = []
        vector_mm = []
        
        # Obtenemos las llaves del tiempo (ISO strings) ordenadas
        time_steps_keys = sorted(forecast_data.get('time_steps', {}).keys())
        
        for i in range(6):
            mm_h = 0.0
            label_hora = "--:--"
            
            if i < len(time_steps_keys):
                iso_key = time_steps_keys[i]
                label_hora = iso_key.split('T')[1][:5] # "18:00"
                
                # Buscamos la lluvia en esta celda específica para esta hora
                # Aplicamos redondeo a 5 decimales para asegurar el "match"
                for p in forecast_data['time_steps'][iso_key]:
                    if round(p['lat'], 5) == ref_lat and round(p['lon'], 5) == ref_lon:
                        mm_h = float(p['mm'])
                        break
            
            vector_mm.append(mm_h)
            pronostico_6h.append({
                "hora": label_hora,
                "mm_h": mm_h,
                "intensidad": get_intensidad(mm_h)
            })

        # 5. Construcción del Mensaje y Respuesta
        mm_actual = celda_cercana.get('rain_mm_h', 0.0)
        alerta = celda_cercana.get('alert_status', 'NORMAL')
        
        # Riesgo Histórico (Opcional)
        riesgo_obj = None
        if 'riesgo_historico' in celda_cercana:
            rh = celda_cercana['riesgo_historico']
            riesgo_obj = {
                "nivel": rh.get('vulnerabilidad'),
                "detalle": rh.get('nombre'),
                "alerta_activa": rh.get('alerta_inundacion_en_vivo', False)
            }

        respuesta = {
            "status": "success",
            "ts": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "ubicacion": {
                "col": celda_cercana.get('col'),
                "mun": celda_cercana.get('mun'),
                "distancia_km": round(math.hypot(ref_lat - user_lat, ref_lon - user_lon) * 111.0, 2)
            },
            "lluvia": {
                "mm_h": mm_actual,
                "intensidad": get_intensidad(mm_actual),
                "alerta_predictiva": alerta,
                "mensaje_corto": "Lluvia crítica inminente." if alerta == "CRITICA" else "Cielo despejado." if mm_actual == 0 else "Lluvia detectada."
            },
            "movilidad_ecobici": movilidad_obj,
            "riesgo_historico": riesgo_obj,
            "pronostico_6h": pronostico_6h,
            "vectores": { "futuro_mm": vector_mm }
        }

        # Limpieza de valores nulos
        return {"statusCode": 200, "headers": headers, "body": json.dumps({k: v for k, v in respuesta.items() if v is not None})}

    except Exception as e:
        return {"statusCode": 500, "headers": headers, "body": json.dumps({"status": "error", "message": str(e)})}
