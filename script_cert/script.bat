@echo off
setlocal

echo =========================================
echo  EASYCAB - GENERADOR DE CERTIFICADOS
echo =========================================

:: Configura la IP del servidor
set SERVER_IP=192.168.1.44

:: Taxis para los que quieres certificados
set TAXIS=taxi1 taxi2 taxi3

:: Ruta de destino de certificados
set OUTPUT_DIR=easycab_certs
mkdir %OUTPUT_DIR%

:: =======================
:: Verificar si OpenSSL existe
:: =======================
where openssl >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [INFO] OpenSSL no encontrado en PATH.

    echo [INFO] Descargando instalador de OpenSSL (Win64)...
    curl -L -o Win64OpenSSL_Light.exe https://slproweb.com/download/Win64OpenSSL_Light-3_3_0.exe

    echo [INFO] Instalando OpenSSL en C:\OpenSSL-Win64...
    start /wait Win64OpenSSL_Light.exe /silent /dir=C:\OpenSSL-Win64

    echo [INFO] Añadiendo OpenSSL al PATH temporalmente...
    set PATH=C:\OpenSSL-Win64\bin;%PATH%
) else (
    echo [INFO] OpenSSL encontrado.
)

echo.

cd %OUTPUT_DIR%

echo [STEP] Generando clave de la CA...
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "/C=ES/ST=Alicante/O=EasyCab/CN=EasyCab-CA"

echo [STEP] Generando clave y certificado para el servidor (EC_Central)...
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/C=ES/ST=Alicante/O=EasyCab/CN=%SERVER_IP%"

echo subjectAltName = IP:%SERVER_IP% > server_ext.cnf
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -sha256 -extfile server_ext.cnf

echo [STEP] Generando claves para taxis...
for %%T in (%TAXIS%) do (
    echo Generando certificado para %%T...
    openssl genrsa -out %%T.key 2048
    openssl req -new -key %%T.key -out %%T.csr -subj "/C=ES/ST=Alicante/O=EasyCab/CN=%%T"
    openssl x509 -req -in %%T.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out %%T.crt -days 3650 -sha256
)

echo.
echo =========================================
echo Certificados generados en: %CD%
echo Copia el archivo ca.crt a TODOS los taxis y el servidor.
echo =========================================

pause
