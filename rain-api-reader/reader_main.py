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
        # 1. Cargar Datos Presentes
        resp_model = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
        presente_data = json.loads(resp_model['Body'].read().decode('utf-8'))
        
        # 2. Hallar Celda más cercana (Para ubicar el contexto del usuario)
        celdas = presente_data.get('values', [])
        celda_cercana = min(celdas, key=lambda c: math.hypot(c['lat'] - user_lat, c['lon'] - user_lon))
        dist_km = round(math.hypot(celda_cercana['lat'] - user_lat, celda_cercana['lon'] - user_lon) * 111.0, 2)

        # 3. Procesar Movilidad (Ecobici) - TOP 3 POR DISPONIBILIDAD
        movilidad_obj = None
        if 'movilidad' in celda_cercana and 'ecobicis_en_celda' in celda_cercana['movilidad']:
            estaciones = celda_cercana['movilidad']['ecobicis_en_celda']
            
            # Ordenamos de MAYOR a MENOR disponibilidad
            estaciones_ordenadas = sorted(estaciones, key=lambda x: x.get('disponibles', 0), reverse=True)
            top_3 = estaciones_ordenadas[:3]

            movilidad_obj = {
                "estaciones_en_celda": len(estaciones),
                "total_bicicletas": sum(s.get('disponibles', 0) for s in estaciones),
                "mejores_opciones": [
                    {
                        "nombre": s['nombre'],
                        "disponibles": s['disponibles']
                    } for s in top_3
                ]
            }

        # 4. Procesar Riesgo
        riesgo_obj = None
        if 'riesgo_historico' in celda_cercana:
            rh = celda_cercana['riesgo_historico']
            riesgo_obj = {
                "nivel": rh.get('vulnerabilidad'),
                "detalle": rh.get('nombre'),
                "alerta_activa": rh.get('alerta_inundacion_en_vivo', False)
            }

        # 5. Generar Respuesta JSON
        mm_actual = celda_cercana.get('rain_mm_h', 0.0)
        alerta = celda_cercana.get('alert_status', 'NORMAL')

        respuesta = {
            "status": "success",
            "ts": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "ubicacion": {
                "col": celda_cercana.get('col'),
                "mun": celda_cercana.get('mun'),
                "distancia_centroide_km": dist_km
            },
            "lluvia": {
                "mm_h": mm_actual,
                "intensidad": get_intensidad(mm_actual),
                "alerta_predictiva": alerta,
                "mensaje_corto": "Lluvia crítica inminente." if alerta == "CRITICA" else "Cielo despejado." if mm_actual == 0 else "Lluvia detectada."
            },
            "movilidad_ecobici": movilidad_obj,
            "riesgo_historico": riesgo_obj
        }

        # Limpiar campos sin datos
        respuesta = {k: v for k, v in respuesta.items() if v is not None}

        return {"statusCode": 200, "headers": headers, "body": json.dumps(respuesta)}

    except Exception as e:
        return {"statusCode": 500, "headers": headers, "body": json.dumps({"error": str(e)})}
