@echo off
setlocal EnableDelayedExpansion

:: Configura cuántos taxis y sensores por taxi deseas
set NUM_TAXIS=1
set NUM_SENSORES=1

:: Configuración base
set SENSORS_IP=127.0.0.1
set CENTRAL_IP=192.168.56.1
set BROKER_IP=127.0.0.1
set CENTRAL_PORT=5050
set BROKER_PORT=9092
set SENSOR_PORT_BASE=8800

set SENSOR_ID=1

for /L %%t in (1,1,!NUM_TAXIS!) do (
    set /a TAXI_ID=%%t
    set /a SENSOR_PORT=!SENSOR_PORT_BASE! + %%t

    echo Lanzando Taxi !TAXI_ID! en puerto !SENSOR_PORT!
    start "Taxi_!TAXI_ID!" cmd /k py EC_DE.py !TAXI_ID! !SENSORS_IP! !SENSOR_PORT! !CENTRAL_IP! !CENTRAL_PORT! !BROKER_IP! !BROKER_PORT!

    timeout /t 2 > nul

    for /L %%s in (1,1,!NUM_SENSORES!) do (
        echo Lanzando Sensor !SENSOR_ID! para Taxi !TAXI_ID! en puerto !SENSOR_PORT!
        start "Sensor_!SENSOR_ID!" cmd /k py EC_S.py !SENSORS_IP! !SENSOR_PORT! S!SENSOR_ID!
        set /a SENSOR_ID+=1
        timeout /t 1 > nul
    )
)

pause

