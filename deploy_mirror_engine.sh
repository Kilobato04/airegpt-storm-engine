#!/bin/bash
# ---------------------------------------------------------
# 🤖 AIREGPT STORM ENGINE DEPLOYER
# ---------------------------------------------------------

PROJECT_NAME="AireGPT-Storm-Engine-Deployer"
REGION="us-east-1"
REPO_BRANCH="main"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' 

echo -e "${GREEN}🚀 INICIANDO DESPLIEGUE - API PÚRPURA${NC}"

echo -e "📦 ${YELLOW}Sincronizando cambios con GitHub...${NC}"
git add .
git commit -m "Deploy automático: Motor API Púrpura" > /dev/null 2>&1 || echo "   (Sin cambios pendientes...)"

echo -e "⬆️  Subiendo a rama ${REPO_BRANCH}..."
git push origin $REPO_BRANCH

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Error: Falló la subida a GitHub.${NC}"
    exit 1
fi

echo -e "📡 ${YELLOW}Contactando a AWS CodeBuild ($PROJECT_NAME)...${NC}"
BUILD_ID=$(aws codebuild start-build --project-name $PROJECT_NAME --region $REGION --query 'build.id' --output text)

if [ -z "$BUILD_ID" ] || [ "$BUILD_ID" == "None" ]; then
    echo -e "${RED}❌ Error al iniciar el build.${NC}"
    exit 1
fi

LINK="https://$REGION.console.aws.amazon.com/codesuite/codebuild/projects/$PROJECT_NAME/build/$BUILD_ID/?region=$REGION"

echo "------------------------------------------------------------"
echo -e "${GREEN}✅ ORDEN ENVIADA A CODEBUILD EXITOSAMENTE${NC}"
echo "👇 VER PROGRESO EN VIVO:"
echo -e "${YELLOW}$LINK${NC}"
echo "------------------------------------------------------------"
