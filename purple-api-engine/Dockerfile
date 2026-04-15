FROM public.ecr.aws/lambda/python:3.11

# 1. Instalar herramientas de compilación de C++ (Vital para scipy/geopandas)
RUN dnf update -y && \
    dnf install -y gcc gcc-c++ python3-devel

# 2. Copiar requirements
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# 3. Actualizar pip e instalar librerías (usando --no-cache-dir para ahorrar memoria RAM)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# 4. Copiar tu código y la malla topográfica
COPY main.py ${LAMBDA_TASK_ROOT}
COPY zmvm_malla_consolidada.geojson ${LAMBDA_TASK_ROOT}

# 5. Comando de arranque de la Lambda
CMD [ "main.handler" ]
