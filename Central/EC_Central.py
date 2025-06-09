import socket
import time
import threading
import argparse 
import json
import kafka
from kafka import KafkaProducer, KafkaConsumer

SERVER = socket.gethostbyname(socket.gethostname())

class EC_Central:
    def __init__(self, puerto, ip_broker, map_path, db_path):
        self.puerto = puerto
        self.ip_broker = ip_broker
        self.taxis_disponibles = {}  # Diccionario para almacenar taxis y sus estados
        self.taxis_autenticados = {}  # Diccionario para almacenar taxis autenticados
        self.db_path = db_path  # Ruta del archivo JSON con los taxis
        self.mapa = [[0 for _ in range(20)] for _ in range(20)]  # Mapa de 20x20
        self.lock = threading.Lock()  # Lock para manejar el acceso concurrente a taxis_disponibles y taxis_autenticados
        self.cerrar = False  # Flag para cerrar el servidor
        self.map_path = map_path  # Ruta del mapa de localizaciones
        self.cargar_taxis()
        self.iniciar_servidor()
        self.init_kafka()

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

    def init_kafka(self):
        self.producer = KafkaProducer(bootstrap_servers=[self.ip_broker])
        self.consumer = KafkaConsumer('solicitudes_taxis', bootstrap_servers=[self.ip_broker], group_id='central')
        self.producer_resp = KafkaProducer(bootstrap_servers=[self.ip_broker])

        self.producer_map = KafkaProducer(bootstrap_servers=[self.ip_broker])

    def cargar_mapa(self):
        try:
            with open(self.map_path, 'r') as mapa:
                config_map = json.load(mapa)
                for localizacion in config_map['localizaciones']:
                    x, y = localizacion['x'], localizacion['y']
                    self.mapa[x][y] = localizacion['id']
                
                for taxi in config_map['taxis']:
                    x, y = taxi['posicion']['x'], taxi['posicion']['y']
                    self.mapa[x][y] = f'T{taxi["id"]}'
                    self.taxis_disponibles.append(taxi['id'])
            print("[Central] Mapa cargado correctamente.")
        except Exception as e:
            print(f"[Central] Error al cargar el mapa: {e}")

    def cargar_localizaciones(self):
        # Lee el archivo EC_locations.json y carga las localizaciones en el mapa
        try:
            with open(self.map_path, 'r') as archivo_localizaciones:
                configuracion_localizaciones = json.load(archivo_localizaciones)
                self.localizaciones = {}
                for localizacion in configuracion_localizaciones['locations']:
                    id_localizacion = localizacion['Id']
                    x, y = map(int, localizacion['POS'].split(','))
                    self.mapa[x][y] = id_localizacion
                    self.localizaciones[id_localizacion] = (x, y)
                print("Localizaciones cargadas correctamente.")
        except Exception as e:
            print(f"Error cargando las localizaciones: {e}")

    def autenticacion_taxi(self, taxi_socket, direccion):
        id_taxi_auth = None  # Inicializar id_taxi_auth 
        autenticado_en_esta_sesion = False  # Flag para verificar autenticación en esta sesión
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
                                    if id_taxi_auth not in self.taxis_autenticados:
                                        print(f"[EC_Central] Taxi {id_taxi_auth} autenticado.")
                                        self.taxis_autenticados[id_taxi_auth] = self.taxis_disponibles[id_taxi_auth]
                                        autenticado_en_esta_sesion = True
                                        taxi_socket.send('ACK'.encode())
                                    else:
                                        taxi_socket.send('NACK'.encode())
                                        print(f"[EC_Central] Error: Taxi {id_taxi_auth} ya autenticado.")
                                        taxi_socket.send('NACK'.encode())
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
            except ConnectionResetError:
                if autenticado_en_esta_sesion and id_taxi_auth in self.taxis_autenticados:
                    del self.taxis_autenticados[id_taxi_auth]
                    print(f"[EC_Central] Conexión con el taxi {id_taxi_auth}:{direccion} perdida.")
                    autenticado_en_esta_sesion = False
                    taxi_socket.close()
                else:
                    print(f"[EC_Central] Taxi {direccion} no autenticado. Cerrando conexión.")
                    taxi_socket.close() 
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
    
    def cargar_taxis(self):
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

    #def iniciar_central(self): #Iniciar GUI y los comandos arbitrarios

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EC_Central - Central de control de taxis")
    
    parser.add_argument('puerto', type=int, help='puerto del servidor central')
    parser.add_argument('ip_broker', type=str, help='IP del broker')
    parser.add_argument('db_path', type=str, help='Base de datos de los taxis')

    args = parser.parse_args()

    puerto = args.puerto
    ip_broker = args.ip_broker
    db_path = args.db_path
    map_path = "EC_locations.json"  # Ruta del mapa de localizaciones

    ec_central = EC_Central(puerto, ip_broker, map_path, db_path)

    ec_central.cargar_mapa()  # Cargar el mapa al iniciar
    ec_central.cargar_taxis()  # Cargar los taxis al iniciar

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Central] Programa finalizado por el usuario.")