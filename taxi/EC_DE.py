import socket
import threading
import time
import json
#from Crypto.Cipher import AES
#from Crypto.Util.Padding import pad
from Cryptodome.Cipher import AES
from Cryptodome.Util.Padding import pad
import base64
import os
import argparse
import tkinter as tk
import kafka
import ssl
from kafka import KafkaProducer, KafkaConsumer
import requests

class EC_DE:
    def __init__(self, id_taxi, sensores_ip, sensores_puerto, central_ip, central_puerto, broker_ip, broker_puerto):
        self.sensores_ip = sensores_ip
        self.sensores_puerto = sensores_puerto
        self.central_ip = central_ip
        self.central_puerto = central_puerto
        self.broker_ip = broker_ip
        self.broker_puerto = broker_puerto
        self.id_taxi = id_taxi
        self.instrucciones_escuchando = False
        self.conectado_central = False
        self.parar_taxi_central = False
        self.estado_sensores = {}
        self.parar_taxi = False  # Indica si el taxi debe detenerse
        self.parar_taxi_sensor = False
        self.posicion = (0,0)
        self.mapa = [[0 for _ in range(20)] for _ in range(20)]
        self.taxis = {}
        self.clientes = {}
        self.lock = threading.Lock()
        self.sensor_status = 'OK'  # Estado inicial de los sensores
        try:
            print(f"Intentando conectar al broker Kafka en: {self.broker_ip}:{self.broker_puerto}")
            self.producer = KafkaProducer(bootstrap_servers=[f"{self.broker_ip}:{self.broker_puerto}"])
            self.consumerTaxi = KafkaConsumer('taxi_instrucciones', bootstrap_servers=[f"{self.broker_ip}:{self.broker_puerto}"])
            self.inicio_sensores()
            self.consumer_estado = KafkaConsumer('mapa_estado', bootstrap_servers=[f"{self.broker_ip}:{self.broker_puerto}"])
            print(f"[Taxi - {self.id_taxi}] Conexión Kafka creada correctamente.")
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] ERROR al crear producer/consumer de Kafka: {e}")
            raise e

        threading.Thread(target=self.recibir_estado_global, daemon=True).start()   
    
    def iniciar_interfaz_grafica(self):
        print("[DEBUG] Entrando a iniciar_interfaz_grafica")
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
        print("[DEBUG] GUI inicializada correctamente")

    def recibir_estado_global(self):
        time.sleep(3)
        for mensaje in self.consumer_estado:
            try:
                estado = json.loads(mensaje.value.decode())
                with self.lock:
                    self.taxis_autenticados = estado.get("taxis", {})
                    self.clientes_activos = estado.get("clientes", {})
                    self.mapa = estado.get("mapa", [])
            except Exception as e:
                print(f"Error recibiendo estado global: {e}")


    def actualizar_grafico(self):

        if not hasattr(self, 'clientes_activos') or not hasattr(self, 'taxis_autenticados'):# Esperar hasta que los atributos esten listos
            self.ventana.after(500, self.actualizar_grafico)
            return

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

    def inicio_sensores(self):
        threading.Thread(target=self.iniciar_servidor_sensores, daemon=True).start()

    def iniciar_servidor_sensores(self):
        self.servidor_sensores = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor_sensores.bind((self.sensores_ip, self.sensores_puerto))
        self.servidor_sensores.listen()
        print(f"[Taxi - {self.id_taxi}] Servidor de sensores iniciado en {self.sensores_ip}:{self.sensores_puerto}")
        while True:
            print(f"[Taxi - {self.id_taxi}] Esperando conexión de EC_S...")
            sensor_socket, direccion = self.servidor_sensores.accept()
            print(f"[Taxi - {self.id_taxi}] Conexión de EC_S desde {direccion}")
            threading.Thread(target=self.recibir_datos_sensor, args=(sensor_socket,), daemon=True).start()

    def recibir_datos_sensor(self, sensor_socket):
        while True:
            try:    
                mensaje = sensor_socket.recv(1024).decode()
                if mensaje:
                    #print(f"[EC_DE] Mensaje recibido de EC_S: {mensaje}")
                    stx_index = mensaje.find('<STX>')
                    etx_index = mensaje.find('<ETX>')
                    lrc_index = mensaje.find('<LRC>')

                    if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                        data = mensaje[stx_index+5:etx_index]
                        if data == "DISCONNECT":
                            print(f"[Taxi - {self.id_taxi}] Sensor {self.sensores_ip} solicitó desconexión.")
                            break
                        lrc = mensaje[lrc_index+5:]
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#')
                            if campos[0] == 'SENSOR':
                                self.sensor_status = campos[1]
                                with self.lock:
                                    if campos[1] == 'PARADA':
                                        self.estado_sensores[campos[2]] = True
                                    else:
                                        self.estado_sensores[campos[2]] = False
                                    self.actualizar_estado_taxi()
                                    self.enviar_estado_central()
                                sensor_socket.send('ACK'.encode())
                                #print(f"[Taxi] Estado del sensor actualizado: {self.sensor_status}")
                            else:
                                sensor_socket.send('NACK'.encode())
                        else:
                            sensor_socket.send('NACK'.encode())
                    else:
                        sensor_socket.send('NACK'.encode())
                else:
                    break
            except Exception as e:
                print(f"[Taxi - {self.id_taxi}] Error en la conexión con el sensor: {e}")
                break
            
        self.sensor_status = 'CONTINGENCY'
    
    def actualizar_estado_taxi(self):
        #with self.lock:
        self.parar_taxi_sensor = any(self.estado_sensores.values())
        self.parar_taxi = self.parar_taxi_sensor or self.parar_taxi_central
        #print(f"[Taxi] Estado del taxi {self.id_taxi} actualizado. Estado: {self.parar_taxi}")

    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)  
    
    def verificar_lrc(self, data, lrc):
        calculated_lrc = self.calcular_lrc(data)
        return str(calculated_lrc) == lrc.strip()
    
    def conectar_de(self):
        try:
            context = ssl.create_default_context(cafile='ca.crt')
            context.check_hostname = False
            context.verify_mode = ssl.CERT_REQUIRED

            raw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_de = context.wrap_socket(raw_socket)

            print(f"[Taxi - {self.id_taxi}] Intentando conectar con EC_Central...")

            self.socket_de.connect((self.central_ip, self.central_puerto))
            print(f"[Taxi - {self.id_taxi}] Conexión SSL establecida con EC_Central.")

            if self.enviar_datos():
                self.conectado_central = True
                print(f"[Taxi - {self.id_taxi}] Autenticación con EC_Central completada con éxito.")
                if not self.instrucciones_escuchando:
                    threading.Thread(target=self.escuchar_instrucciones, daemon=True).start()
                    self.instrucciones_escuchando = True

            else:
                print(f"[Taxi - {self.id_taxi}] Fallo de autenticación. Reintentando en 5 segundos...")
                self.socket_de.close()

        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Error al intentar conectar o autenticar: {e}")

        time.sleep(5)  # Esperar 5 segundos antes de reintentar


    def enviar_datos(self):
        data = f'TAXI#{self.id_taxi}'
        lrc = self.calcular_lrc(data)
        mensaje = f'<STX>{data}<ETX><LRC>{lrc}'

        try:
            self.socket_de.send(mensaje.encode())
            respuesta = self.socket_de.recv(1024).decode()
            if respuesta == 'ACK':
                print(f"[Taxi - {self.id_taxi}] Taxi autenticado con éxito.")
                return True
            else:
                print(f"[Taxi - {self.id_taxi}] Autenticación rechazada por Central (respuesta: {respuesta}).")
                return False
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Error enviando datos de autenticación: {e}")
            return False


    def reconectar_central(self):
        while not self.conectado_central:
            try:
                self.socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket_central.connect((self.central_ip, self.central_puerto))
                print(f"[Taxi - {self.id_taxi}] Reconectado a EC_Central.")
                self.conectado_central = True
                #self.autenticar()
                time.sleep(5)
                # Reanudar escuchando instrucciones
                threading.Thread(target=self.escuchar_instrucciones, daemon=True).start()
            except Exception as e:
                print(f"[Taxi - {self.id_taxi}] No se pudo reconectar a EC_Central: {e}")
                time.sleep(5)

    def enviar_estado_central(self):
        if not hasattr(self, 'shared_key') or not self.shared_key:
            print(f"[Taxi - {self.id_taxi}] No tengo clave compartida, no envío estado.")
            return
        if self.parar_taxi:
            estado = 'BLOQUEADO'
        else:
            estado = 'OK'

        mensaje = {
            'taxi_id': self.id_taxi,
            'estado': estado,
            'posicion': self.posicion
        }

        try:
            iv, ciphertext = self.encrypt_data(json.dumps(mensaje))
            mensaje_cifrado = {
                'taxi_id': self.id_taxi,
                'iv': iv,
                'data': ciphertext
            }
            self.producer.send('taxi_estado', key=self.id_taxi.encode(), value=json.dumps(mensaje_cifrado).encode())
            self.producer.flush()
            #print(f"[Taxi - {self.id_taxi}] Estado enviado a la Central: {mensaje}")
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Error al enviar estado a la Central: {e}")
            self.reconectar_central()

    def load_key(self):
        try:
            with open(f"{self.id_taxi}_key.pem", "r") as file:
                self.shared_key = file.read()
            if not self.shared_key:
                print("La clave cargada está vacía.")
            print(f"[Taxi {self.id_taxi}] Clave cargada desde archivo.")
        except Exception as e:
            print(f"[Taxi {self.id_taxi}] Error cargando la clave: {e}")
            exit(1)


    def menu_registry(self):
        while True:
            print("\n--- MENU REGISTRY ---")
            print("1. Registrarse")
            print("2. Autenticarse")
            print("3. Dar de baja")
            print("4. Salir")

            option = input("Elija una opción: ")

            if option == '1':
                self.register_taxi()
            elif option == '2':
                self.authenticate_taxi()
            elif option == '3':
                self.unregister_taxi()
            elif option == '4':
                exit(0)
            else:
                print("Opción no válida.")

    def register_taxi(self):
        try:
            url = "https://192.168.1.40:5001/register_taxi"
            response = requests.post(url, json={"id": self.id_taxi}, verify="registry.crt")
            if response.status_code == 201:
                print(f"[Taxi {self.id_taxi}] Registro exitoso en el Registry.")
            else:
                print(f"[Taxi {self.id_taxi}] Error al registrar: {response.text}")
        except Exception as e:
            print(f"Error registrando taxi: {e}")

    def encrypt_data(self, plaintext):
        key = base64.urlsafe_b64decode(self.shared_key)
        cipher = AES.new(key, AES.MODE_CBC)
        ct_bytes = cipher.encrypt(pad(plaintext.encode('utf-8'), AES.block_size))
        iv = base64.urlsafe_b64encode(cipher.iv).decode('utf-8')
        ciphertext = base64.urlsafe_b64encode(ct_bytes).decode('utf-8')
        return iv, ciphertext


    def authenticate_taxi(self):
        try:
            # Paso 1: Consultar lista de taxis registrados
            list_url = "https://192.168.1.40:5001/list_taxis"
            response = requests.get(list_url, verify="registry.crt")

            if response.status_code == 200:
                taxis_registrados = response.json().get("taxis", [])

            # Paso 2: Comprobar si este taxi está en la lista
            if self.id_taxi not in taxis_registrados:
                print(f"[Taxi {self.id_taxi}] No está registrado en el Registry. No se puede autenticar.")
                return
            url = "https://192.168.1.40:5001/authenticate_taxi"
            response = requests.post(url, json={"id": self.id_taxi}, verify="registry.crt")
            if response.status_code == 200:
                self.shared_key = response.json().get("key")
                self.save_key()
                print(f"[Taxi {self.id_taxi}] Autenticación exitosa. Clave recibida.")
                ec_de.load_key()
                self.conectar_de()
            else:
                print(f"[Taxi {self.id_taxi}] Error autenticando: {response.text}")
                exit(1)
        except Exception as e:
            print(f"Error autenticando taxi: {e}")

    def unregister_taxi(self):
        try:
            # Paso 1: Consultar lista de taxis registrados
            list_url = "https://192.168.1.40:5001/list_taxis"
            response = requests.get(list_url, verify="registry.crt")

            if response.status_code == 200:
                taxis_registrados = response.json().get("taxis", [])

                # Paso 2: Comprobar si este taxi está en la lista
                if self.id_taxi not in taxis_registrados:
                    print(f"[Taxi {self.id_taxi}] No está registrado en el Registry. No se puede dar de baja.")
                    return

                # Paso 3: Si está, proceder a eliminarlo
                delete_url = "https://192.168.1.40:5001/delete_taxi"
                delete_response = requests.delete(delete_url, json={"id": self.id_taxi}, verify="registry.crt")

                if delete_response.status_code == 200:
                    print(f"[Taxi {self.id_taxi}] Baja exitosa.")
                    self.notificar_baja_por_socket()
                    print(f"[Taxi - {self.id_taxi}] Taxi dado de baja. Finalizando ejecución.")

                else:
                    print(f"[Taxi {self.id_taxi}] Error al dar de baja: {delete_response.text}")

            else:
                print(f"[Taxi {self.id_taxi}] Error consultando Registry: {response.status_code} {response.text}")

        except Exception as e:
            print(f"Error dando de baja taxi: {e}")

    def notificar_baja_por_socket(self):
        try:
            data = f'BAJA#{self.id_taxi}'
            lrc = self.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}'

            if hasattr(self, 'socket_de'):
                self.socket_de.send(mensaje.encode())
                print(f"[Taxi {self.id_taxi}] Baja notificada a la Central por socket.")
            else:
                print(f"[Taxi {self.id_taxi}] No hay conexión socket activa para notificar baja.")

        except Exception as e:
            print(f"[Taxi {self.id_taxi}] Error notificando baja por socket: {e}")


    def save_key(self):
        try:
            with open(f"{self.id_taxi}_key.pem", "w") as file:
                file.write(self.shared_key)
            print(f"Clave guardada para {self.id_taxi}")
        except Exception as e:
            print(f"Error guardando la clave: {e}")

    def calcular_siguiente_posicion(self, destino):
        x, y = self.posicion
        destino_x, destino_y = destino

        if x < destino_x:
            x += 1
        elif x > destino_x:
            x -= 1

        if y < destino_y:
            y += 1
        elif y > destino_y:
            y -= 1

        print(f"[Taxi - {self.id_taxi}] Moviendo de {self.posicion} a ({x}, {y})")

        self.posicion = (x, y)
        print(f"[Taxi - {self.id_taxi}] Nueva posición: {self.posicion}")
        
        # Enviar estado a la Central después de mover
        self.enviar_estado_central()
        time.sleep(2)

    def mover_destino(self, instruccion):
        try:
            print(f"---> self.posicion: {self.posicion}")
            print(f"---> Instruccion destino: {instruccion['destino']}")
            while self.posicion != tuple(instruccion['destino']):
                print(f"---> self.posicion: {self.posicion}")
                print(f"---> Instruccion destino: {instruccion['destino']}")
                if self.parar_taxi == True:
                    time.sleep(2)
                    self.enviar_estado_central()
                else:
                    print(f"[Taxi - {self.id_taxi}] Moviendo hacia destino: {instruccion['destino']}")
                    self.calcular_siguiente_posicion(instruccion['destino'])
            print(f"[Taxi - {self.id_taxi}] Taxi ha llegado a su destino: {instruccion['destino']}")
            self.enviar_estado_central()
            return True
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Error al mover hacia destino: {e}")
            self.enviar_estado_central()
            self.reconectar_central()

    def escuchar_instrucciones(self):
        print(f"[Taxi - {self.id_taxi}] Escuchando instrucciones de taxi...")
        try: 
            for mensaje in self.consumerTaxi:
                try:
                    if self.id_taxi == mensaje.key.decode():
                        instruccion = json.loads(mensaje.value.decode())
                        print(f"[Taxi - {self.id_taxi}] Instrucción recibida: {instruccion}")

                        # Si el mensaje es un comando tipo STOP / RESUME / GO_TO_BASE
                        if 'comando' in instruccion:
                            comando = instruccion['comando']
                            print(f"------> El comando es: {comando}.")
                            if comando == 'PARAR':
                                print(f"[Taxi - {self.id_taxi}] Comando recibido: {comando}.")
                                self.parar_taxi_central = True
                                self.actualizar_estado_taxi()
                                self.enviar_estado_central()
                            elif comando == 'REANUDAR':
                                print(f"[Taxi - {self.id_taxi}] Comando recibido: {comando}.")
                                self.parar_taxi_central = False
                                print(f"Entro en reanudar y la variable de parar taxis es: {self.parar_taxi_central}.")
                                self.actualizar_estado_taxi()
                                self.enviar_estado_central()
                            else:
                                print(f"[Taxi - {self.id_taxi}] Comando desconocido: {comando}")

                        # Si es una instrucción de movimiento (destino normal)
                        elif 'destino' in instruccion:
                            print(f"[Taxi - {self.id_taxi}] Instrucción de movimiento hacia: {instruccion['destino']}")
                            self.mover_destino(instruccion)

                        else:
                            print(f"[Taxi - {self.id_taxi}] Instrucción no reconocida: {instruccion}")

                except Exception as e:
                    print(f"[Taxi - {self.id_taxi}] Error al procesar instrucción: {e}")
        finally:
            self.instrucciones_escuchando = False


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Ejecutar EC_DE con parámetros de conexión y autenticación.")

        parser.add_argument('id_taxi', type=str, help='ID del taxi')
        parser.add_argument('sensores_ip', type=str, help='IP de EC_S')
        parser.add_argument('sensores_puerto', type=int, help='Puerto de EC_S')
        parser.add_argument('central_ip', type=str, help='IP de EC_Central')
        parser.add_argument('central_puerto', type=int, help='Puerto de EC_Central')
        parser.add_argument('broker_ip', type=str, help='IP de broker')
        parser.add_argument('broker_puerto', type=int, help='Puerto de broker')
        

        args = parser.parse_args()

        sensores_ip = args.sensores_ip
        sensores_puerto = args.sensores_puerto
        central_ip = args.central_ip
        central_puerto = args.central_puerto
        broker_ip = args.broker_ip
        broker_puerto = args.broker_puerto
        id_taxi = args.id_taxi
        print(f"Error. ->>>>>>>>>>>>>>")
        ec_de = EC_DE(id_taxi, sensores_ip, sensores_puerto, central_ip, central_puerto, broker_ip, broker_puerto)

        clave_file = f"{id_taxi}_key.pem"

        if os.path.exists(clave_file):
            print(f"[Taxi {id_taxi}] Clave existente detectada, cargando clave...")
            ec_de.load_key()
        else:
            print(f"[Taxi {id_taxi}] No se encontró clave local. Iniciando autenticación contra Registry...")
            ec_de.authenticate_taxi()


        threading.Thread(target=ec_de.menu_registry, daemon=True).start()

        # Esto se debe ejecutar en el hilo principal
        ec_de.iniciar_interfaz_grafica()
        # mainloop bloquea, por eso debe ir aquí
        ec_de.ventana.mainloop()
        #try:
        #    while True:
        #        time.sleep(1)
        #except KeyboardInterrupt:
        #    print("[Taxi] Programa finalizado por el usuario.")
        #    ec_de.servidor_sensores.close()
        
    except Exception as e:
        try:
            if 'ec_de' in locals():
                data = f'ERROR_TAXI#{id_taxi}'
                lrc = ec_de.calcular_lrc(data)
                mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
                ec_de.socket_de.send(mensaje.encode())
                print("[Taxi] Error inesperado, se notificó a la Central.")
            else:
                print("[Taxi] Error fatal: EC_DE no fue creado.")
        except Exception as noti_error:
            print(f"[Taxi] Error fatal y no se pudo notificar a la Central. Detalle: {noti_error}")
        finally:
            exit(1)
