import base64
import socket
import time
import threading
import argparse 
import json
import ssl
import kafka
from kafka import KafkaProducer, KafkaConsumer
import tkinter as tk
from Crypto.Cipher import AES

import requests

SERVER = socket.gethostbyname(socket.gethostname())

class EC_Central:
    def __init__(self, puerto, ip_broker, map_path, db_path):
        self.puerto = puerto
        self.ip_broker = ip_broker
        self.claves_taxis = {}
        self.taxis_disponibles = {}  # Diccionario para almacenar taxis y sus estados
        self.taxis_autenticados = {}  # Diccionario para almacenar taxis autenticados
        self.db_path = db_path  # Ruta del archivo JSON con los taxis
        self.mapa = [[0 for _ in range(20)] for _ in range(20)]  # Mapa de 20x20
        self.lock = threading.Lock()  # Lock para manejar el acceso concurrente a taxis_disponibles y taxis_autenticados
        self.cerrar = False  # Flag para cerrar el servidor
        self.map_path = map_path  # Ruta del mapa de localizaciones
        self.clientes_activos = {}  # Diccionario para almacenar clientes activos
        self.ctc_estado = 'OK'
        self.init_kafka()
        self.cargar_taxis()
        self.iniciar_servidor()

    def iniciar_servidor(self):
        threading.Thread(target=self.iniciar_servidor_taxis, daemon=True).start()
        threading.Thread(target=self.procesar_solicitudes_cliente, daemon=True).start()
        threading.Thread(target=self.escuchar_estado_taxis, daemon=True).start()
        threading.Thread(target=self.enviar_estado_global, daemon=True).start()
        threading.Thread(target=self.procesar_comandos_arbitrarios, daemon=True).start()
        threading.Thread(target=self.revisar_estado_ctc, daemon=True).start()


    def iniciar_servidor_taxis(self):

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(certfile="server.crt", keyfile="server.key")

        raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_socket.bind((SERVER, self.puerto))
        raw_socket.listen()

        self.servidor = context.wrap_socket(raw_socket, server_side=True)
        #print(f"[EC_Central] Servidor iniciado en {SERVER}:{self.puerto}")
        while True:
            #print("[EC_Central] Esperando conexión de taxis...")
            taxi_socket, direccion = self.servidor.accept()
            print(f"[EC_Central] Conexión de taxi desde {direccion}")
            threading.Thread(target=self.autenticacion_taxi, args=(taxi_socket, direccion,), daemon=True).start()

    def init_kafka(self):
        self.producer = KafkaProducer(bootstrap_servers=[self.ip_broker])
        self.consumerSol = KafkaConsumer('cliente_solicitud', bootstrap_servers=[self.ip_broker])
        self.consumerTaxi = kafka.KafkaConsumer('taxi_instrucciones', bootstrap_servers=[self.ip_broker])
        self.consumerTaxiEstado = kafka.KafkaConsumer('taxi_estado', bootstrap_servers=[self.ip_broker])

    def iniciar_interfaz_grafica(self):
        self.ventana = tk.Tk()
        self.ventana.title("Estado del sistema EasyCab")

        self.frame_tablas = tk.Frame(self.ventana)
        self.frame_tablas.pack()

        self.label_taxis = tk.Label(self.frame_tablas, text="Taxis", font=("Arial", 10, "bold"))
        self.label_taxis.grid(row=0, column=0)

        self.label_clientes = tk.Label(self.frame_tablas, text="Clientes", font=("Arial", 10, "bold"))
        self.label_clientes.grid(row=0, column=1)

        self.text_taxis = tk.Text(self.frame_tablas, height=7, width=40)
        self.text_taxis.grid(row=1, column=0)

        self.text_clientes = tk.Text(self.frame_tablas, height=7, width=40)
        self.text_clientes.grid(row=1, column=1)

        self.canvas = tk.Canvas(self.ventana, width=400, height=400, bg="white")
        self.canvas.pack()

        self.cuadros = {}
        self.actualizar_grafico()

    def actualizar_grafico(self):
        self.canvas.delete("all")
        tam = 20

        for i in range(20):
            for j in range(20):
                x0, y0 = j * tam, i * tam
                x1, y1 = x0 + tam, y0 + tam
                contenido = self.mapa[i][j]
                if isinstance(contenido, str) and contenido.isupper():
                    self.canvas.create_rectangle(x0, y0, x1, y1, fill="blue", outline="gray")
                    self.canvas.create_text(x0 + 10, y0 + 10, text=contenido, fill="white", font=("Arial", 8))
                else:
                    self.canvas.create_rectangle(x0, y0, x1, y1, outline="gray")

        with self.lock:

            for cliente_id, pos in self.clientes_activos.items():
                if 'origen' in pos:
                    origen = pos['origen']
                    x, y = origen
                    self.canvas.create_rectangle(y * tam, x * tam, (y + 1) * tam, (x + 1) * tam, fill="yellow")
                    self.canvas.create_text(y * tam + 10, x * tam + 10, text=str(cliente_id).lower(), fill="black", font=("Arial", 8))


            for taxi_id, info in self.taxis_autenticados.items():
                if 'posicion' in info:
                    x, y = info['posicion']
                    estado_sensor = info.get("estado_sensor")
                    estado_taxi = info.get("estado_taxi")
                    if estado_sensor != "OK" or estado_taxi == "FREE":
                        color = "red"
                    else:
                        color = "green"

                    self.canvas.create_rectangle(y * tam, x * tam, (y + 1) * tam, (x + 1) * tam, fill=color)
                    self.canvas.create_text(y * tam + 10, x * tam + 10, text=str(taxi_id), fill="white", font=("Arial", 8))

        self.text_taxis.delete('1.0', tk.END)
        self.text_taxis.insert(tk.END, "Id\tDestino\tEstado\n")

        self.text_clientes.delete('1.0', tk.END)
        self.text_clientes.insert(tk.END, "Id\tDestino\tEstado\n")

        with self.lock:
            for cliente_id, info in self.clientes_activos.items():
                destino = info.get("destino", "")
                taxi_asignado = ""
                for t_id, t_info in self.taxis_autenticados.items():
                    if t_info.get("cliente") == cliente_id:
                        taxi_asignado = f"Taxi {t_id}"
                        break
                self.text_clientes.insert(tk.END, f"{cliente_id}\t{destino}\t{info.get("estado")}. {taxi_asignado}\n")

        with self.lock:
            for taxi_id, info in self.taxis_autenticados.items():
                destino = info.get("destino", "")
                estado = info.get("estado_taxi", "")
                self.text_taxis.insert(tk.END, f"{taxi_id}\t{destino}\t{info.get("estado_sensor")}. Servicio {destino}\n")

        
        self.ventana.after(1000, self.actualizar_grafico)

    def procesar_comandos_arbitrarios(self):
        while True:
            comando_input = input("Ingrese un comando (formato: TAXI_ID [PARAR, REANUDAR, IR_A_DESTINO, VOLVER_BASE] [DESTINO]): ")
            if comando_input:
                partes = comando_input.strip().split()
                if len(partes) >= 2:
                    taxi_id = partes[0]
                    comando = partes[1].upper()
                    if taxi_id in self.taxis_autenticados:
                        if comando == 'PARAR':
                            self.enviar_comando_taxi(taxi_id, 'PARAR')
                        elif comando == 'REANUDAR':
                            self.enviar_comando_taxi(taxi_id, 'REANUDAR')
                        elif comando == 'IR_A_DESTINO' and len(partes) == 3:
                            destino_id = partes[2]
                            destino_coord = self.localizaciones.get(destino_id)
                            if destino_coord:
                                self.enviar_instrucciones_taxi(taxi_id, destino_coord)
                            else:
                                print(f"[CENTRAL] Destino {destino_id} no encontrado.")
                        elif comando == 'VOLVER_BASE':
                            #self.taxis_autenticados[taxi_id]['estado_taxi'] = 'FREE'
                            cliente_id = self.taxis_autenticados[taxi_id].get('cliente')
                            posicion_taxi = self.taxis_autenticados[taxi_id].get('posicion')

                            with self.lock:
                                self.taxis_autenticados[taxi_id]['estado_taxi'] = 'FREE'
                                self.taxis_autenticados[taxi_id]['destino'] = ''
                                self.taxis_autenticados[taxi_id]['cliente'] = ''

                            if cliente_id:
                                mensaje = {
                                    'cliente_id': cliente_id,
                                    'estado': 'ABANDONADO',
                                    'posicion': posicion_taxi
                                }

                            try:
                                self.producer.send('cliente_respuesta', key=cliente_id.encode(), value=json.dumps(mensaje).encode())
                                self.producer.flush()
                                print(f"[CENTRAL] Cliente {cliente_id} notificado de abandono en {posicion_taxi}.")
                            except Exception as e:
                                print(f"[CENTRAL] Error notificando al cliente {cliente_id}: {e}")


                            self.enviar_instrucciones_taxi(taxi_id, (0, 0))
                        else:
                            print("[CENTRAL] Comando no reconocido o parámetros insuficientes.")
                    else:
                        print(f"[CENTRAL] Taxi {taxi_id} no está autenticado.")
                else:
                    print("[CENTRAL] Formato incorrecto. Use: TAXI_ID COMANDO [DESTINO]")

    def enviar_comando_taxi(self, taxi_id, comando):
        """
        Enviar un comando arbitrario a un taxi: STOP, RESUME o GO_TO_BASE.

        :param taxi_id: ID del taxi al que se quiere enviar la orden.
        :param comando: 'STOP', 'RESUME' o 'GO_TO_BASE'
        """
        try:
            print(f"[EC_Central] Enviando comando '{comando}' al taxi {taxi_id}.")

            mensaje = {
                'taxi_id': taxi_id,
                'comando': comando
            }

            self.producer.send('taxi_instrucciones', key=taxi_id.encode(), value=json.dumps(mensaje).encode())
            self.producer.flush()
            print(f"[EC_Central] Comando '{comando}' enviado al taxi {taxi_id} correctamente.")

        except Exception as e:
            print(f"[EC_Central] Error enviando comando '{comando}' al taxi {taxi_id}: {e}")

    def verificar_taxi_en_registry(self, id_taxi):
        try:#Cambiar ip por la de DE
            response = requests.get("https://127.0.0.1:5001/list_taxis", verify="ca.crt")
            if response.status_code == 200:
                taxis_registrados = response.json().get("taxis", [])
                return id_taxi in taxis_registrados
        except Exception as e:
            print(f"[EC_Central] Error consultando el Registry: {e}")
        return False


    def enviar_estado_global(self):
        while True: 
            with self.lock:
                estado = {
                    "taxis": self.taxis_autenticados,
                    "clientes": self.clientes_activos,
                    "mapa": self.mapa
                }
                mensaje = json.dumps(estado).encode()
                self.producer.send('mapa_estado', value=mensaje)
            time.sleep(1)

    def revisar_estado_ctc(self):
        while True:
            time.sleep(10)  # Revisa cada 10 segundos
            try:
                respuesta = requests.get("http://127.0.0.1:5000/consulta").json()
                nuevo_estado = respuesta["estado"]

                # Si cambia el estado, actualízalo
                if nuevo_estado != self.ctc_estado:
                    print(f"[EC_Central] Estado CTC actualizado: {nuevo_estado}")
                    self.ctc_estado = nuevo_estado

                if self.ctc_estado == "KO":
                    print("[EC_Central] Estado KO: Ordenando a todos los taxis volver a base.")
                    with self.lock:
                        for taxi_id in self.taxis_autenticados:
                            self.enviar_instrucciones_taxi(taxi_id, (0, 0))

            except Exception as e:
                print(f"[EC_Central] Error al consultar EC_CTC: {e}")


        
    def procesar_solicitudes_cliente(self):
        for mensaje in self.consumerSol:
            mensaje = mensaje.value.decode()
            #print(f"[EC_Central] Mensaje recibido: {mensaje}")
            try:
                solicitud = json.loads(mensaje)
                cliente_id = solicitud['cliente_id']
                destino_coord = solicitud['destino']
                origen_coord = solicitud['origen']

                if cliente_id not in self.clientes_activos:
                    print(f"Posicion del cliente {cliente_id}: {origen_coord}")
                    self.clientes_activos[cliente_id] = {
                        'destino': destino_coord,
                        'origen': origen_coord,
                        'estado': 'OK'
                    }
                else:
                    print(f"Posicion del cliente {cliente_id}: {origen_coord}")
                    self.clientes_activos[cliente_id] = {
                        'destino': destino_coord,
                        'origen': origen_coord,
                        'estado': self.clientes_activos[cliente_id]["estado"]
                    }

                print(f"[EC_Central] Cliente {cliente_id} solicita taxi de {origen_coord} a {destino_coord}")
    
                taxi_id = self.unir_taxi_cliente(cliente_id, destino_coord)
                if taxi_id:
                    self.enviar_mensaje_cliente(cliente_id, 'OK')
                    #self.llevar_taxi_a_cliente(taxi_id, cliente_id, destino_coord, origen_coord)
                    self.clientes_activos[cliente_id]["estado"] = 'OK'
                    threading.Thread(target=self.llevar_taxi_a_cliente, args=(taxi_id, cliente_id, destino_coord, origen_coord), daemon=True).start()
                else:
                    print(f"El taxi asignado al cliente {cliente_id} es: {taxi_id}")
                    self.enviar_mensaje_cliente(cliente_id, 'KO')
                    self.clientes_activos[cliente_id]["estado"] = 'ESPERA'
                    #del self.clientes_activos[cliente_id]  # Eliminar cliente si no hay taxi disponible
            except json.JSONDecodeError as e:
                print(f"[EC_Central] Error al procesar el mensaje: {e}")
                del self.clientes_activos[cliente_id]  # Eliminar cliente si hay error en el mensaje

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
            #print("[Central] Mapa cargado correctamente.")
        except Exception as e:
            print(f"[Central] Error al cargar el mapa: {e}")

    def obtener_clave_compartida_del_taxi(self, taxi_id):
        try:
            url = "https://127.0.0.1:5001/get_taxi_key"
            response = requests.post(url, json={"id": taxi_id}, verify="ca.crt")
            if response.status_code == 200:
                key = response.json().get("clave")
                self.claves_taxis[taxi_id] = key
                return key
            else:
                print(f"[EC_Central] No se pudo obtener clave para taxi {taxi_id}: {response.text}")
                return None
        except Exception as e:
            print(f"[EC_Central] Error obteniendo clave de Registry: {e}")
            return None


    def escuchar_estado_taxis(self):
        for mensaje in self.consumerTaxiEstado:
            try:

                valor = json.loads(mensaje.value.decode())
                iv = base64.urlsafe_b64decode(valor['iv'])
                ciphertext = base64.urlsafe_b64decode(valor['data'])

                # Aquí deberías tener la clave compartida del taxi correspondiente
                shared_key = self.obtener_clave_compartida_del_taxi(valor['taxi_id'])  # <-- Implementa esto según tu central
                if shared_key is None:
                    print(f"[EC_Central] Clave del taxi {valor['taxi_id']} no encontrada. Ignorando mensaje.")
                    continue
                cipher = AES.new(base64.urlsafe_b64decode(shared_key), AES.MODE_CBC, iv=iv)
                plaintext = cipher.decrypt(ciphertext)
                plaintext_json = json.loads(plaintext.decode().strip())  # Aquí ya tienes el JSON original con "estado"

                estado = plaintext_json['estado']  # Ahora sí puedes acceder sin error
                taxi_id = plaintext_json['taxi_id']
                posicion = plaintext_json['posicion']
                with self.lock:
                    if taxi_id in self.taxis_autenticados:
                        self.taxis_autenticados[taxi_id]['estado_sensor'] = estado
                        self.taxis_autenticados[taxi_id]['posicion'] = posicion
                        print(f"[EC_Central] Estado del taxi {taxi_id} actualizado: {estado}, Posición: {posicion}")

                        cliente_id = self.taxis_autenticados[taxi_id].get('cliente')
                        if cliente_id and cliente_id in self.clientes_activos and self.taxis_autenticados[taxi_id]['estado_taxi'] == 'BUSY':
                            self.clientes_activos[cliente_id]['origen'] = posicion

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
                #print("Localizaciones cargadas correctamente.")
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

                                # Nueva comprobación contra el Registry
                                if not self.verificar_taxi_en_registry(id_taxi_auth):
                                    print(f"[EC_Central] Taxi {id_taxi_auth} no existe en Registry. Rechazando autenticación.")
                                    taxi_socket.send('NACK'.encode())
                                    return  # Salir de la función, no aceptar conexión

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
                            elif campos[0] == 'BAJA':
                                id_taxi = campos[1]
                                with self.lock:
                                    if id_taxi in self.taxis_autenticados:
                                        del self.taxis_autenticados[id_taxi]
                                        print(f"[EC_Central] Taxi {id_taxi} ha sido dado de baja (notificado por socket).")
                                taxi_socket.close()
                                return  # Fin de la sesión de este taxi

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
                    
                        # Restablecer valores del taxi en taxis_disponibles
                    if id_taxi_auth in self.taxis_disponibles:
                        self.taxis_disponibles[id_taxi_auth] = {
                            'estado_taxi': 'FREE',
                            'estado_sensor': 'OK',
                            'posicion': [0, 0],  # o lo que consideres como posición inicial
                            'destino': '',
                            'cliente': ''
                        }
                    print(f"[EC_Central] Taxi {id_taxi_auth} reseteado en taxis_disponibles.")

                    print(f"[EC_Central] Conexión con el taxi {id_taxi_auth}:{direccion} perdida.")
                    print(f"[EC_Central] Taxi {id_taxi_auth} eliminado de taxis autenticados.")
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
                    posicion = taxi.get('posicion', [0, 0])

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
                        'destino': '',
                        'cliente': ''
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
        #print(f"[EC_Central] Enviada respuesta al cliente {id_cliente}: {estado}")

    def enviar_mensaje_cliente_final(self, id_cliente, estado, posicion):
        time.sleep(0.5)
        mensaje = {
            'cliente_id': id_cliente,
            'estado': estado,
            'posicion': posicion
        }
        self.producer.send('cliente_respuesta', key=id_cliente.encode(), value=json.dumps(mensaje).encode())
        #print(f"[EC_Central] Enviada respuesta final al cliente {id_cliente}: {estado}, Posición: {posicion}")

    def unir_taxi_cliente(self, cliente_id, destino_coord):
        with self.lock:
            #print(f"[DEBUG] Estados de taxis autenticados:")
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
                self.clientes_activos[cliente_id]['en_taxi'] = True
            print(f"Taxi {taxi_id} ha llegado a {cliente_origen} para recoger al cliente.")
            self.enviar_mensaje_cliente(cliente_id, 'RECOGIDO')
            #print(f"Enviado mensaje recogido al cliente")
            self.actualizar_tabla = True
            # Instrucciones para llevar al cliente al destino
            print(f"Taxi {taxi_id} llevando al cliente {cliente_id} a {destino}.")
            self.enviar_instrucciones_taxi(taxi_id, self.localizaciones[destino])
            self.esperar_llegada_taxi(taxi_id, self.localizaciones[destino])
            self.enviar_mensaje_cliente_final(cliente_id, 'DESTINO', self.localizaciones[destino])
            with self.lock:
                taxi_info['estado_taxi'] = 'FREE'
                taxi_info['destino'] = ''
                taxi_info['cliente'] = ''
            print(f"[DEBUG] Taxi {taxi_id} marcado como FREE después de entregar al cliente. ->> {taxi_info['estado_taxi']}")


        else:
            print(f"[CENTRAL] No se pudo enviar taxi {taxi_id} al cliente {cliente_id}.")

    def enviar_instrucciones_taxi(self, taxi_id, destino):
        if self.ctc_estado == "KO" and destino != (0, 0):
            print(f"[EC_Central] No puedo enviar instrucciones al taxi {taxi_id} porque el tráfico está en KO.")
            return  # No hacer nada si está en KO y no va a base

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

    # Esto se debe ejecutar en el hilo principal
    ec_central.iniciar_interfaz_grafica()

    # mainloop bloquea, por eso debe ir aquí
    ec_central.ventana.mainloop()

    #try:
    #    while True:
    #        time.sleep(1)
    #except KeyboardInterrupt:
    #    print("[EC_Central] Programa finalizado por el usuario.")