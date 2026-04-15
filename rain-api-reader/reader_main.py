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
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json'
    }
    
    query = event.get('queryStringParameters', {}) or {}
    
    # 1. Extraer Lat/Lon del usuario
    try:
        user_lat = float(query.get('lat'))
        user_lon = float(query.get('lon'))
    except (TypeError, ValueError):
        return {"statusCode": 400, "headers": headers, "body": json.dumps({"status": "error", "message": "lat and lon required"})}

    try:
        # 2. Descargar Presente de S3
        resp_model = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LATEST)
        presente_data = json.loads(resp_model['Body'].read().decode('utf-8'))
        
        # 3. Encontrar la celda más cercana (Fórmula de Distancia Euclidiana rápida)
        celdas = presente_data.get('values', [])
        celda_cercana = None
        min_dist = float('inf')
        
        for c in celdas:
            # Distancia aproximada plana (suficiente para mallas locales)
            dist = math.hypot(c['lat'] - user_lat, c['lon'] - user_lon)
            if dist < min_dist:
                min_dist = dist
                celda_cercana = c
                
        if not celda_cercana:
            raise Exception("No se encontraron celdas")

        # Distancia en KM aprox (1 grado ~ 111 km)
        dist_km = round(min_dist * 111.0, 2)

        # 4. Descargar Futuro de S3
        try:
            resp_forecast = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_FORECAST)
            forecast_data = json.loads(resp_forecast['Body'].read().decode('utf-8'))
        except:
            forecast_data = {"time_steps": {}}

        # 5. Extraer el Futuro solo para esta celda
        target_lat = celda_cercana['lat']
        target_lon = celda_cercana['lon']
        
        pronostico_timeline = []
        vector_futuro = []
        
        for hora_iso, puntos in forecast_data.get('time_steps', {}).items():
            hora_corta = hora_iso.split('T')[1][:5] # Extrae "16:00"
            mm_predicho = 0.0
            
            # Recordar que S3 solo guarda celdas > 0 para ahorrar espacio
            for p in puntos:
                if p['lat'] == target_lat and p['lon'] == target_lon:
                    mm_predicho = p['mm']
                    break
            
            vector_futuro.append(mm_predicho)
            
            # Solo guardamos las primeras 4 horas para el timeline
            if len(pronostico_timeline) < 4:
                pronostico_timeline.append({
                    "hora": hora_corta,
                    "mm_h": mm_predicho,
                    "intensidad": get_intensidad(mm_predicho)
                })

        # 6. Calcular Tendencia
        mm_actual = celda_cercana['rain_mm_h']
        tendencia_str = "Estable ➡️"
        if vector_futuro:
            if vector_futuro[0] > mm_actual: tendencia_str = "Aumentando ↗️"
            elif vector_futuro[0] < mm_actual: tendencia_str = "Disminuyendo ↘️"

        # 7. Armar Mensaje Corto
        if mm_actual == 0: msg = "Cielo despejado en tu zona."
        elif mm_actual < 5: msg = "Lluvia ligera. Precaución al manejar."
        else: msg = "Lluvia intensa detectada. Posibles encharcamientos."

        # 8. Construir JSON Final Homologado
        respuesta = {
            "status": "success",
            "origen": "live",
            "ts": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "ubicacion": {
                "distancia_km": dist_km,
                "col": celda_cercana.get('col'),
                "mun": celda_cercana.get('mun'),
                "edo": celda_cercana.get('edo')
            },
            "lluvia": {
                "mm_h": mm_actual,
                "intensidad": get_intensidad(mm_actual),
                "alerta": celda_cercana.get('alert_status', 'NORMAL'),
                "tendencia": tendencia_str,
                "mensaje_corto": msg
            },
            "pronostico_timeline": pronostico_timeline,
            "vectores": {
                "hoy": { "lluvia_mm": [mm_actual] }, # Se puede expandir luego si guardas histórico
                "futuro": { "lluvia_mm": vector_futuro }
            }
        }

        return {"statusCode": 200, "headers": headers, "body": json.dumps(respuesta)}

    except Exception as e:
        return {"statusCode": 500, "headers": headers, "body": json.dumps({"status": "error", "message": str(e)})}
