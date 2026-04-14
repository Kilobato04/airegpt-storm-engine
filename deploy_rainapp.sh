#!/bin/bash
# ---------------------------------------------------------
# 🤖 AIREGPT STORM ENGINE - UNIFIED DEPLOYER
# ---------------------------------------------------------

PROJECT_NAME="AireGPT-Storm-Engine-Deployer"
REGION="us-east-1"
REPO_BRANCH="main"
LAMBDA_MIRROR_NAME="SacmexMirrorAPI" # <--- Asegúrate que este nombre coincida en AWS

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' 

echo -e "${GREEN}🚀 INICIANDO DESPLIEGUE GLOBAL (LAMBDA A + LAMBDA B)${NC}"

# ==========================================
# 1. SINCRONIZACIÓN CON GITHUB
# ==========================================
echo -e "\n📦 ${YELLOW}1. Sincronizando cambios con GitHub...${NC}"
git add .
git commit -m "Deploy automático: Actualización Global (Mirror + Engine)" > /dev/null 2>&1 || echo "   (Sin cambios pendientes...)"

git push origin $REPO_BRANCH

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Error: Falló la subida a GitHub.${NC}"
    exit 1
fi

# ==========================================
# 2. DESPLIEGUE RÁPIDO: LAMBDA A (Espejo SACMEX)
# ==========================================
echo -e "\n⚡ ${YELLOW}2. Desplegando Lambda A (SACMEX Mirror API)...${NC}"
cd sacmex-mirror-api

# Crear archivo ZIP silenciosamente
zip -r ../sacmex-mirror.zip ./* > /dev/null
cd ..

# Subir a AWS Lambda
aws lambda update-function-code \
    --function-name $LAMBDA_MIRROR_NAME \
    --zip-file fileb://sacmex-mirror.zip > /dev/null

# Limpiar basura
rm sacmex-mirror.zip
echo -e "${GREEN}✅ Lambda A actualizada y operativa en AWS.${NC}"

# ==========================================
# 3. DESPLIEGUE PESADO: LAMBDA B (API Púrpura)
# ==========================================
echo -e "\n📡 ${YELLOW}3. Contactando a AWS CodeBuild para Lambda B ($PROJECT_NAME)...${NC}"
BUILD_ID=$(aws codebuild start-build --project-name $PROJECT_NAME --region $REGION --query 'build.id' --output text)

if [ -z "$BUILD_ID" ] || [ "$BUILD_ID" == "None" ]; then
    echo -e "${RED}❌ Error al iniciar el build de Lambda B.${NC}"
    exit 1
fi

LINK="https://$REGION.console.aws.amazon.com/codesuite/codebuild/projects/$PROJECT_NAME/build/$BUILD_ID/?region=$REGION"

echo "------------------------------------------------------------"
echo -e "${GREEN}✅ ORDEN DE CONSTRUCCIÓN ENVIADA EXITOSAMENTE${NC}"
echo "👇 VER PROGRESO EN VIVO (LAMBDA B):"
echo -e "${YELLOW}$LINK${NC}"
echo "------------------------------------------------------------"
