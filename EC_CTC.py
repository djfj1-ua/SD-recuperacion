from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# ====================
# Configuración
# ====================

# Ciudad inicial por defecto
ciudad_actual = "Alicante"

# API Key de OpenWeather (pon la tuya aquí)
API_KEY = "04f2d806a868c3866f36b7ffd804e1cc"

# URL base de la API de OpenWeather
WEATHER_API_URL = "https://api.openweathermap.org/data/2.5/weather"

# ====================
# Ruta para consultar el estado del tráfico
# ====================
@app.route("/consulta", methods=["GET"])
def estado_trafico():
    global ciudad_actual
    try:
        # Consulta a OpenWeather
        params = {
            "q": ciudad_actual,
            "appid": API_KEY,
            "units": "metric"
        }
        response = requests.get(WEATHER_API_URL, params=params)
        data = response.json()

        if response.status_code != 200:
            return jsonify({"estado": "ERROR", "detalle": f"No se pudo consultar clima: {data}"}), 500

        temperatura = data["main"]["temp"]
        print(f"[CTC] Temperatura actual en {ciudad_actual}: {temperatura}°C")

        if temperatura < 25:
            estado = "KO"
        else:
            estado = "OK"

        return jsonify({"estado": estado, "temperatura": temperatura})

    except Exception as e:
        print(f"[CTC] Error al consultar el estado del tráfico: {e}")
        return jsonify({"estado": "ERROR", "detalle": str(e)}), 500

# ====================
# Ruta para cambiar de ciudad
# ====================
#@app.route("/cambiar_ciudad", methods=["POST"])
#def cambiar_ciudad():
#    global ciudad_actual
#    data = request.get_json()
#    if not data or "ciudad" not in data:
#        return jsonify({"estado": "ERROR", "detalle": "Falta campo 'ciudad'"}), 400
#
#    ciudad_actual = data["ciudad"]
#    print(f"[CTC] Ciudad cambiada a: {ciudad_actual}")
#    return jsonify({"estado": "OK", "nueva_ciudad": ciudad_actual})

import threading

def menu_cambiar_ciudad():
    global ciudad_actual
    while True:
        print("\n=== Menú EC_CTC ===")
        print("1. Cambiar ciudad")
        print("2. Mostrar ciudad actual")
        print("3. Salir (no cierra Flask, solo el menú)")
        opcion = input("Elige una opción: ")

        if opcion == "1":
            nueva_ciudad = input("Introduce el nombre de la nueva ciudad: ")
            ciudad_actual = nueva_ciudad
            print(f"[CTC] Ciudad cambiada a: {ciudad_actual}")
        elif opcion == "2":
            print(f"[CTC] Ciudad actual: {ciudad_actual}")
        elif opcion == "3":
            print("[CTC] Cerrando menú interactivo...")
            break
        else:
            print("[CTC] Opción no válida. Intenta de nuevo.")

# ====================
# Main
# ====================
if __name__ == "__main__":

    threading.Thread(target=menu_cambiar_ciudad, daemon=True).start()

    print("[CTC] Iniciando servidor EC_CTC en puerto 5000...")
    app.run(host="0.0.0.0", port=5000)
    
