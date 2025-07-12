#!/bin/bash

# Salir si algo falla
set -e

GREEN='\033[0;32m'
NC='\033[0m' # Sin color

echo -e "${GREEN}📦 Instalando dependencias...${NC}"

# Si no hay package.json, lo inicializa
[ ! -f "package.json" ] && npm init -y

# Instalar solo si no está ya
npm install aws-sdk @elastic/elasticsearch

echo -e "${GREEN}🚀 Desplegando con Serverless...${NC}"
serverless deploy 
