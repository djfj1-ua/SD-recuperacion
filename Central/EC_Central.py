import socket
import time
import threading
import argparse 
import json

SERVER = socket.gethostbyname(socket.gethostname())

class EC_Central:
    def __init__(self, puerto, ip_broker, puerto_broker):
        self.puerto = puerto
        self.ip_broker = ip_broker
        self.puerto_broker = puerto_broker
        self.taxis_disponibles = {}  # Diccionario para almacenar taxis y sus estados
        self.db_path = 'taxis.json'  # Ruta del archivo JSON con los taxis
        self.cargar_taxis_desde_bd()
        self.iniciar_servidor()

    def iniciar_servidor(self):
        threading.Thread(target=self.iniciar_servidor_taxis, daemon=True).start()

    def iniciar_servidor_taxis(self):
        self.servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor.bind((SERVER, self.puerto))
        self.servidor.listen()
        print(f"[EC_Central] Servidor iniciado en {SERVER}:{self.puerto}")
        while True:
            print("[EC_Central] Esperando conexión de taxis...")
            taxi_socket, direccion = self.servidor.accept()
            print(f"[EC_Central] Conexión de taxi desde {direccion}")
            threading.Thread(target=self.autenticacion_taxi, args=(taxi_socket, direccion,), daemon=True).start()

    def autenticacion_taxi(self, taxi_socket, direccion):
        while True:
            try:    
                mensaje = taxi_socket.recv(1024).decode()
                if mensaje:
                    #print(f"[EC_DE] Mensaje recibido de EC_S: {mensaje}")
                    stx_index = mensaje.find('<STX>')
                    etx_index = mensaje.find('<ETX>')
                    lrc_index = mensaje.find('<LRC>')

                    if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                        data = mensaje[stx_index+5:etx_index]
                        lrc = mensaje[lrc_index+5:]
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#')
                            if campos[0] == 'TAXI':
                                id_taxi_auth = campos[1]
                                if id_taxi_auth in self.taxis_disponibles:
                                    taxi_socket.send('ACK'.encode())
                                    print(f"[EC_Central] Taxi {id_taxi_auth} autenticado.")
                                    taxi_socket.send(f'ACK'.encode())
                                else:
                                    taxi_socket.send('NACK'.encode())
                                    print(f"[EC_Central] Taxi {id_taxi_auth} no reconocido.")
                                    taxi_socket.send('NACK'.encode())
                            else:
                                taxi_socket.send('NACK'.encode())
                        else:
                            taxi_socket.send('NACK'.encode())
                    else:
                        taxi_socket.send('NACK'.encode())
                else:
                    break
            except Exception as e:
                print(f"[EC_Central] Error en la conexión con el taxi: {e}")
                break

    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)  
    
    def verificar_lrc(self, data, lrc):
        calculated_lrc = self.calcular_lrc(data)
        return str(calculated_lrc) == lrc.strip()
    
    def cargar_taxis_desde_bd(self):
        try:
            with open(self.db_path, 'r') as archivo_taxis:
                taxis_data = json.load(archivo_taxis)
                for taxi in taxis_data:
                    taxi_id = taxi['id']
                    estado = taxi.get('estado', 'FREE')
                    posicion = taxi.get('posicion', [1, 1])

                    # Validar estado
                    if estado not in ['FREE', 'BUSY', 'STOPPED', 'END',]:
                        print(f"Estado '{estado}' no reconocido para el taxi {taxi_id}. Taxi omitido.")
                        continue

                    # Validar posición
                    x, y = posicion
                    if not (0 <= x < 20 and 0 <= y < 20):
                        print(f"Posición {posicion} fuera del mapa para el taxi {taxi_id}. Taxi omitido.")
                        continue

                    # Verificar ID duplicado
                    if taxi_id in self.taxis_disponibles:
                        print(f"Taxi con id {taxi_id} ya cargado. Esperando su autenticación.")
                        continue

                    self.taxis_disponibles[taxi_id] = {
                        'estado': estado,
                        'posicion': posicion,
                        'destino': None  
                    }
                print(f"Taxi {taxi_id} cargado con posición {posicion} y estado {estado}.")    
            print("Taxis cargados correctamente.")
        except Exception as e:
            print(f"Error cargando los taxis: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EC_Central - Central de control de taxis")
    
    parser.add_argument('puerto', type=int, help='puerto del servidor central')
    parser.add_argument('ip_broker', type=str, help='IP del broker')
    parser.add_argument('puerto_broker', type=int, help='Puerto del broker')

    args = parser.parse_args()

    puerto = args.puerto
    ip_broker = args.ip_broker
    puerto_broker = args.puerto_broker

    ec_central = EC_Central(puerto, ip_broker, puerto_broker)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[EC_Central] Programa finalizado por el usuario.")
        ec_central.servidor.close()
        ec_central.broker_socket.close()
        exit(0)