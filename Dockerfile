FROM node:20-alpine

# Instalación del cliente de Docker dentro del contenedor para ejecutar docker run en el host
RUN apk add --no-cache docker-cli

WORKDIR /app

# Copiar archivos de dependencias e instalar
COPY package*.json ./
RUN npm install --production

# Copiar el resto del código fuente
COPY . .

EXPOSE 3000

CMD ["node", "index.js"]
