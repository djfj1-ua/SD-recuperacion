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
        self.init_kafka()
        self.cargar_taxis()
        self.iniciar_servidor()

    def iniciar_servidor(self):
        threading.Thread(target=self.iniciar_servidor_taxis, daemon=True).start()
        threading.Thread(target=self.procesar_solicitudes_cliente, daemon=True).start()
        threading.Thread(target=self.escuchar_estado_taxis, daemon=True).start()

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
        self.consumerSol = KafkaConsumer('cliente_solicitud', bootstrap_servers=[self.ip_broker])
        self.consumerTaxi = kafka.KafkaConsumer('taxi_instrucciones', bootstrap_servers=[self.ip_broker])
        self.consumerTaxiEstado = kafka.KafkaConsumer('taxi_estado', bootstrap_servers=[self.ip_broker])

    def procesar_solicitudes_cliente(self):
        for mensaje in self.consumerSol:
            mensaje = mensaje.value.decode()
            print(f"[EC_Central] Mensaje recibido: {mensaje}")
            try:
                solicitud = json.loads(mensaje)
                cliente_id = solicitud['cliente_id']
                destino_coord = solicitud['destino']
                origen_coord = solicitud['origen']

                print(f"[EC_Central] Cliente {cliente_id} solicita taxi de {origen_coord} a {destino_coord}")
    
                taxi_id = self.unir_taxi_cliente(cliente_id, destino_coord)
                print(f"El taxi asignado al cliente {cliente_id} es: {taxi_id}")
                if taxi_id:
                    self.enviar_mensaje_cliente(cliente_id, 'OK')
                    #self.llevar_taxi_a_cliente(taxi_id, cliente_id, destino_coord, origen_coord)
                    threading.Thread(target=self.llevar_taxi_a_cliente, args=(taxi_id, cliente_id, destino_coord, origen_coord), daemon=True).start()
                else:
                    self.enviar_mensaje_cliente(cliente_id, 'KO')
            except json.JSONDecodeError as e:
                print(f"[EC_Central] Error al procesar el mensaje: {e}")

    def cargar_mapa(self):
        try:
            with open(self.map_path, 'r') as mapa:
                config_map = json.load(mapa)
                for localizacion in config_map['locations']:
                    x, y = localizacion['x'], localizacion['y']
                    self.mapa[x][y] = localizacion['id']
                
                for taxi in config_map['taxis']:
                    x, y = taxi['posicion']['x'], taxi['posicion']['y']
                    self.mapa[x][y] = f'T{taxi["id"]}'
                    self.taxis_disponibles.append(taxi['id'])
            print("[Central] Mapa cargado correctamente.")
        except Exception as e:
            print(f"[Central] Error al cargar el mapa: {e}")

    def escuchar_estado_taxis(self):
        for mensaje in self.consumerTaxiEstado:
            try:
                solicitud = json.loads(mensaje.value.decode())
                estado = solicitud['estado']
                taxi_id = solicitud['taxi_id']
                posicion = solicitud['posicion']
                with self.lock:
                    if taxi_id in self.taxis_autenticados:
                        self.taxis_autenticados[taxi_id]['estado_sensor'] = estado
                        self.taxis_autenticados[taxi_id]['posicion'] = posicion
                        print(f"[EC_Central] Estado del taxi {taxi_id} actualizado: {estado}, Posición: {posicion}")
            except Exception as e:
                print(f"[EC_Central] Error procesando mensaje de taxi_estado: {e}")

    def esperar_llegada_taxi(self, taxi_id, destino, timeout=60):
        start_time = time.time()
        while time.time() - start_time < timeout:
            with self.lock:
                posicion = self.taxis_autenticados.get(taxi_id, {}).get('posicion')
            if posicion and tuple(posicion) == tuple(destino):
                print(f"[EC_Central] Taxi {taxi_id} ha llegado a su destino {destino}.")
                return True
            time.sleep(0.5)
        print(f"[EC_Central] Tiempo de espera agotado para el taxi {taxi_id} en destino {destino}")
        return False


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
                    #del self.taxis_autenticados[id_taxi_auth]
                    #del self.taxis_disponibles[id_taxi_auth]
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
                    estado = taxi['estado']
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
                        'estado_taxi': estado,
                        'estado_sensor': 'OK',
                        'posicion': posicion,
                        'destino': None,
                        'cliente': None
                    }
                print(f"Taxi {taxi_id} cargado con posición {posicion} y estado {estado}.")    
            print("Taxis cargados correctamente.")
        except Exception as e:
            print(f"Error cargando los taxis: {e}")

    def enviar_mensaje_cliente(self, id_cliente, estado):
        time.sleep(0.5)  # Simular un pequeño retraso para evitar colisiones en el envío
        mensaje = {
            'cliente_id': id_cliente,
            'estado': estado,
        }
        self.producer.send('cliente_respuesta', key=id_cliente.encode(),value=json.dumps(mensaje).encode())
        print(f"[EC_Central] Enviada respuesta al cliente {id_cliente}: {estado}")

    def enviar_mensaje_cliente_final(self, id_cliente, estado, posicion):
        time.sleep(0.5)
        mensaje = {
            'cliente_id': id_cliente,
            'estado': estado,
            'posicion': posicion
        }
        self.producer.send('cliente_respuesta', key=id_cliente.encode(), value=json.dumps(mensaje).encode())
        print(f"[EC_Central] Enviada respuesta final al cliente {id_cliente}: {estado}, Posición: {posicion}")

    def unir_taxi_cliente(self, cliente_id, destino_coord):
        with self.lock:
            print(f"[DEBUG] Estados de taxis autenticados:")
            for taxi_id, taxi_info in self.taxis_autenticados.items():
                print(f"  Taxi {taxi_id}: {taxi_info['estado_taxi']}")
                if taxi_info['estado_taxi'] == 'FREE':
                    taxi_info['estado_taxi'] = 'BUSY'
                    taxi_info['destino'] = destino_coord
                    taxi_info['cliente'] = cliente_id
                    self.actualizar_mapa = True
                    print(f"Taxi {taxi_id} asignado al cliente {cliente_id} con destino {destino_coord}.")
                    return taxi_id
        print(f"No hay taxis disponibles.")

    def llevar_taxi_a_cliente(self, taxi_id, cliente_id, destino, cliente_origen):
        taxi_info = self.taxis_autenticados.get(taxi_id)
        #cliente_origen = self.localizaciones[cliente_id]['origen']

        if taxi_info:
            print(f"Taxi {taxi_id} se dirige a recoger al cliente {cliente_id} en {cliente_origen}.")
            taxi_info['estado_taxi'] = 'RUN'
            self.actualizar_mapa = True
            self.enviar_instrucciones_taxi(taxi_id, cliente_origen)
            self.esperar_llegada_taxi(taxi_id, cliente_origen)
            # Una vez en el origen, cambia el estado del cliente a "RECOGIDO"
            with self.lock:
                taxi_info['estado_taxi'] = 'BUSY'
            print(f"Taxi {taxi_id} ha llegado a {cliente_origen} para recoger al cliente.")
            self.enviar_mensaje_cliente(cliente_id, 'RECOGIDO')
            print(f"Enviado mensaje recogido al cliente")
            self.actualizar_tabla = True
            # Instrucciones para llevar al cliente al destino
            print(f"Taxi {taxi_id} llevando al cliente {cliente_id} a {destino}.")
            self.enviar_instrucciones_taxi(taxi_id, self.localizaciones[destino])
            self.esperar_llegada_taxi(taxi_id, self.localizaciones[destino])
            self.enviar_mensaje_cliente_final(cliente_id, 'DESTINO', self.localizaciones[destino])
            with self.lock:
                taxi_info['estado_taxi'] = 'FREE'
                taxi_info['destino'] = None
                taxi_info['cliente'] = None
            print(f"[DEBUG] Taxi {taxi_id} marcado como FREE después de entregar al cliente. ->> {taxi_info['estado_taxi']}")


        else:
            print(f"[CENTRAL] No se pudo enviar taxi {taxi_id} al cliente {cliente_id}.")

    def enviar_instrucciones_taxi(self, taxi_id, destino):
        print(f"[EC_Central] Enviando instrucciones al taxi {taxi_id} para ir a {destino}.")
        time.sleep(0.5)
        mensaje = {
            'taxi_id': taxi_id,
            'destino': destino
        }
        self.producer.send('taxi_instrucciones', key=taxi_id.encode(), value=json.dumps(mensaje).encode())

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

    ec_central.cargar_localizaciones()  

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[EC_Central] Programa finalizado por el usuario.")