# ⛈️ AireGPT Storm Engine (AWS Serverless Backend)

Backend de alto rendimiento y bajo costo (Serverless) para la monitorización, modelado matemático y sistema de alerta temprana de tormentas en la Zona Metropolitana del Valle de México (ZMVM). 

Este sistema alimenta el Front-end de **RainApp CDMX** y el Bot de Telegram de **AireGPT**.

## 🏗️ Arquitectura del Sistema

El motor está dividido en dos microservicios (AWS Lambdas) independientes, ambos escritos en Python 3.11, diseñados para minimizar costos de cómputo y maximizar la resiliencia ante caídas de red.

### 1. Lambda A: SACMEX Mirror API (`sacmex-mirror-api/`)
Actúa como un *Gatekeeper* y memoria caché.
* **Frecuencia:** Ejecución vía EventBridge cada 1 minuto.
* **Función:** Consume la API oficial de SACMEX y sensores propietarios (CHAAK) en paralelo. 
* **Auditoría:** Evalúa la latencia de la red, calcula un KPI de salud (`redConfianzaPromedio`) y activa *Circuit Breakers* si SACMEX sufre *timeouts*.
* **Output:** Sirve un JSON ultra-rápido al Front-end con el estado real de las estaciones.

### 2. Lambda B: Purple API Engine (`purple-api-engine/`)
El cerebro matemático del gemelo digital. Desplegado vía Docker (Amazon ECR) debido a la complejidad de sus librerías en C++ (`geopandas`, `scipy`).
* **Frecuencia:** Ejecución vía EventBridge cada 3 minutos (Controlado por Máquina de Estados).
* **Máquina de Estados (Cost Optimization):**
  * **SLEEP:** Si el Mirror API reporta `< 2` estaciones con lluvia, la Lambda aborta en 100ms. Costo casi cero.
  * **ACTIVE:** Si hay lluvia, ejecuta la interpolación Gaussiana (RBF) sobre el Valle de México.
  * **CRITICAL:** Compara la lluvia máxima actual vs. la de hace 3 minutos (vía S3). Calcula la **Derivada (mm/min)**. Si supera los umbrales (Ej. `> 0.5 mm/min`), detona alertas de prevención (Naranja/Púrpura).
* **Output:** Genera el archivo `latest_model.json` en Amazon S3.

## 🚀 Guía de Despliegue (AWS CloudShell)

El despliegue se realiza directamente desde la consola de AWS CloudShell para evitar dependencias locales.

### Despliegue de Lambda A (Mirror API)
Esta Lambda es ligera y se sube mediante un archivo `.zip` estándar.
```bash
cd airegpt-storm-engine
git pull origin main
sh deploy_rainapp.sh --> deploy Lamnda A y B
deploy_reader.sh --> deploy Lambda para el bot

