@echo off
set NUM_CLIENTES=1

for /L %%i in (1,1,%NUM_CLIENTES%) do (
    start "Cliente_%%i" cmd /k py EC_Customer.py 192.168.1.44 9092 %%i EC_Requests%%i.json
)

pause
