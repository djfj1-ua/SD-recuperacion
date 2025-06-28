from flask import Flask, request, jsonify
import sqlite3
import os
import base64
from Crypto.Random import get_random_bytes

app = Flask(__name__)
DB_PATH = "registry.db"

# ==================================
# Inicializar base de datos si no existe
# ==================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS taxis (
            id TEXT PRIMARY KEY,
            clave TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# ==================================
# Endpoint para registrar taxi
# ==================================
@app.route('/register_taxi', methods=['POST'])
def register_taxi():
    data = request.json
    taxi_id = data.get('id')

    if not taxi_id:
        return jsonify({"estado": "ERROR", "detalle": "Falta ID del taxi"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM taxis WHERE id = ?', (taxi_id,))
    if c.fetchone():
        conn.close()
        return jsonify({"estado": "ERROR", "detalle": "Taxi ya registrado"}), 409

    # Generar clave secreta AES de 16 bytes (128 bits)
    clave = base64.urlsafe_b64encode(get_random_bytes(16)).decode()

    c.execute('INSERT INTO taxis (id, clave) VALUES (?, ?)', (taxi_id, clave))
    conn.commit()
    conn.close()

    return jsonify({"estado": "OK", "clave": clave}), 201

@app.route('/authenticate_taxi', methods=['POST'])
def authenticate_taxi():
    data = request.json
    taxi_id = data.get('id')

    if not taxi_id:
        return jsonify({"estado": "ERROR", "detalle": "Falta ID del taxi"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT clave FROM taxis WHERE id = ?', (taxi_id,))
    result = c.fetchone()
    conn.close()

    if result:
        return jsonify({"estado": "OK", "key": result[0]})
    else:
        return jsonify({"estado": "ERROR", "detalle": "Taxi no encontrado"}), 404


# ==================================
# Endpoint para eliminar taxi
# ==================================
@app.route('/delete_taxi', methods=['DELETE'])
def delete_taxi():
    data = request.json
    taxi_id = data.get('id')

    if not taxi_id:
        return jsonify({"estado": "ERROR", "detalle": "Falta ID del taxi"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM taxis WHERE id = ?', (taxi_id,))
    conn.commit()
    conn.close()

    return jsonify({"estado": "OK", "detalle": f"Taxi {taxi_id} eliminado"}), 200

# ==================================
# Endpoint para obtener clave secreta de un taxi
# ==================================
@app.route('/get_taxi_key', methods=['POST'])
def get_taxi_key():
    data = request.json
    taxi_id = data.get('id')

    if not taxi_id:
        return jsonify({"estado": "ERROR", "detalle": "Falta ID del taxi"}), 400

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT clave FROM taxis WHERE id = ?', (taxi_id,))
    result = c.fetchone()
    conn.close()

    if result:
        return jsonify({"estado": "OK", "key": result[0]}), 200
    else:
        return jsonify({"estado": "ERROR", "detalle": "Taxi no encontrado"}), 404


# ==================================
# Endpoint para listar taxis registrados (opcional)
# ==================================
@app.route('/list_taxis', methods=['GET'])
def list_taxis():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id FROM taxis')
    taxis = [row[0] for row in c.fetchall()]
    conn.close()
    return jsonify({"estado": "OK", "taxis": taxis})

# ==================================
# Main (con HTTPS - usa tu propio certificado autofirmado)
# ==================================
if __name__ == '__main__':
    # Necesitas crear estos archivos antes de ejecutar:
    # openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes
    cert_file = 'registry.crt'
    key_file = 'registry.key'

    if not os.path.exists(cert_file) or not os.path.exists(key_file):
        print("Faltan los archivos cert.pem y key.pem. Crea un certificado autofirmado primero.")
        exit(1)

    app.run(host='0.0.0.0', port=5001, ssl_context=(cert_file, key_file))
