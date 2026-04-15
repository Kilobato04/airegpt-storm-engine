# Usamos imagen base de AWS para Lambda Python 3.11
FROM public.ecr.aws/lambda/python:3.11

# 1. Instalar dependencias pesadas (Geopandas, Scipy)
COPY requirements.txt .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# 2. Copiar código fuente y topografía
COPY main.py ${LAMBDA_TASK_ROOT}
COPY zmvm_malla_consolidada.geojson ${LAMBDA_TASK_ROOT}

# 3. Comando por defecto
CMD [ "main.lambda_handler" ]
