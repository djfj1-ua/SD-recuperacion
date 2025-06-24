import socket
import threading
import time
import json
import argparse
import tkinter as tk
import kafka
from kafka import KafkaProducer, KafkaConsumer

class EC_DE:
    def __init__(self, id_taxi, sensores_ip, sensores_puerto, central_ip, central_puerto, broker_ip, broker_puerto):
        self.sensores_ip = sensores_ip
        self.sensores_puerto = sensores_puerto
        self.central_ip = central_ip
        self.central_puerto = central_puerto
        self.broker_ip = broker_ip
        self.broker_puerto = broker_puerto
        self.id_taxi = id_taxi
        self.estado_sensores = {}
        self.parar_taxi = False  # Indica si el taxi debe detenerse
        self.posicion = (0,0)
        self.mapa = [[0 for _ in range(20)] for _ in range(20)]
        self.taxis = {}
        self.clientes = {}
        self.lock = threading.Lock()
        self.sensor_status = 'OK'  # Estado inicial de los sensores
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumerTaxi = kafka.KafkaConsumer('taxi_instrucciones', bootstrap_servers=[self.broker_ip])

        self.inicio_sensores()
        self.conectar_de()
        self.consumer_estado = KafkaConsumer('mapa_estado', bootstrap_servers=[self.broker_ip])
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
                                    self.actualizar_estado_sensores()
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
    
    def actualizar_estado_sensores(self):
        self.parar_taxi = any(self.estado_sensores.values())
        #print(f"[Taxi] Estado de los sensores actualizado. Parar taxi: {self.parar_taxi}")

    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)  
    
    def verificar_lrc(self, data, lrc):
        calculated_lrc = self.calcular_lrc(data)
        return str(calculated_lrc) == lrc.strip()
    
    def conectar_de(self):
        self.socket_de = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket_de.connect((self.central_ip, self.central_puerto))
            print(f"[Taxi - {self.id_taxi}] Conectándose al EC_Central.")
            # Inicio del envío de datos
            threading.Thread(target=self.enviar_datos, daemon=True).start()
        except ConnectionRefusedError:
            print(f"[Taxi - {self.id_taxi}] No se pudo conectar al EC_Central en {self.ip_engine}:{self.puerto_engine}. Asegúrate de que EC_Central esté en ejecución.")
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Ocurrió un error al intentar conectarse: {e}")
            exit(1)

    def enviar_datos(self):

        data = f'TAXI#{self.id_taxi}'
        lrc = self.calcular_lrc(data)
        mensaje = f'<STX>{data}<ETX><LRC>{lrc}'

        try:
            self.socket_de.send(mensaje.encode())
            #print(f"[Sensor] Enviando estado del sensor: {sensor_status}")
            # Esperar ACK
            respuesta = self.socket_de.recv(1024).decode()
            if respuesta != 'ACK':
                print(f"[Taxi - {self.id_taxi}] Error al autenticar taxi.")
                exit(1)
            else:
                print(f"[Taxi - {self.id_taxi}] Taxi autenticado con exito.")
                threading.Thread(target=self.escuchar_instrucciones, daemon=True).start()
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Error al enviar estado del sensor: {e}")

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
            print(f"[Taxi - {self.id_taxi}] Enviando estado a la Central: {mensaje}")
            self.producer.send('taxi_estado', key=self.id_taxi.encode(), value=json.dumps(mensaje).encode())
            self.producer.flush()
            print(f"[Taxi - {self.id_taxi}] Estado enviado a la Central: {mensaje}")
        except Exception as e:
            print(f"[Taxi - {self.id_taxi}] Error al enviar estado a la Central: {e}")
            self.reconectar_central()

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
        for mensaje in self.consumerTaxi:
            try:
                if self.id_taxi == mensaje.key.decode():
                    instruccion = json.loads(mensaje.value.decode())
                    print(f"[Taxi - {self.id_taxi}] Instrucción recibida: {instruccion}")
                    self.mover_destino(instruccion)
            except Exception as e:
                print(f"[Taxi - {self.id_taxi}] Error al procesar instrucción: {e}")

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
        print(f"hola caracola")
        ec_de = EC_DE(id_taxi, sensores_ip, sensores_puerto, central_ip, central_puerto, broker_ip, broker_puerto)
        print(f"hola caracola2")
        # Esto se debe ejecutar en el hilo principal
        ec_de.iniciar_interfaz_grafica()
        print(f"hola caracola3")
        # mainloop bloquea, por eso debe ir aquí
        ec_de.ventana.mainloop()
        print(f"hola caracola4")
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
