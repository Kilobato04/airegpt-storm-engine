#!/bin/bash
echo "⬇️ Obteniendo últimos cambios de GitHub..."
git pull origin main

echo "📦 Empaquetando el Lector..."
cd rain-api-reader
zip -r reader.zip lambda_function.py

echo "🚀 Desplegando en AWS Lambda..."
aws lambda update-function-code \
    --function-name PurpleRain_ApiReader \
    --zip-file fileb://reader.zip

echo "🧹 Limpiando..."
rm reader.zip

echo "✅ Despliegue del Lector completado."
