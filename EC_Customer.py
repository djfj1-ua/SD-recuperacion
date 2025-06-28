import json
import time
import argparse
import threading
from kafka import KafkaProducer, KafkaConsumer
import tkinter as tk
from tkinter import messagebox, scrolledtext

class EC_Customer:
    def __init__(self, broker_ip, brocker_puerto,requests_path, cliente_id):
        self.broker_ip = broker_ip
        self.broker_puerto = brocker_puerto
        self.requests_path = requests_path
        self.posicion = (8, 8)
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer('cliente_respuesta', bootstrap_servers=[self.broker_ip])
        self.consumer_estado = KafkaConsumer('mapa_estado', bootstrap_servers=[self.broker_ip])
        self.cliente_id = cliente_id
        self.mapa = [[0 for _ in range(20)] for _ in range(20)]
        self.taxis = {}
        self.clientes = {}
        self.lock = threading.Lock()
        self.solicitudes_pendientes = {}
        self.solicitud_enviada = False
        self.cargar_movimientos()
        threading.Thread(target=self.recibir_estado_global, daemon=True).start()
    
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

    def cargar_movimientos(self):
        try:
            with open(self.requests_path, 'r') as file:
                movimientos = json.load(file)
                total_movimientos = movimientos['Requests']

                self.solicitudes_pendientes = total_movimientos
                self.log(f"[Cliente {self.cliente_id}] Solicitudes cargadas: {self.solicitudes_pendientes}")

                if len(self.solicitudes_pendientes) > 0:
                    self.log(f"[Cliente {self.cliente_id}] Preparando para enviar solicitudes...")
                    self.enviar_mensaje()
                
        except FileNotFoundError:
            print(f"Archivo {self.requests_path} no encontrado.")
            return []
        except json.JSONDecodeError:
            print(f"Error al decodificar el archivo {self.requests_path}.")
            return []
        
    def escuchar_estado(self):
        for mensaje in self.consumer:
            data = json.loads(mensaje.value.decode())
            self.mapa = data.get("mapa", self.mapa)
            self.taxis = data.get("taxis", {})
            self.clientes = data.get("clientes", {})

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
        
    def recibir_estado_global(self):
        for mensaje in self.consumer_estado:
            try:
                estado = json.loads(mensaje.value.decode())
                with self.lock:
                    self.taxis_autenticados = estado.get("taxis", {})
                    self.clientes_activos = estado.get("clientes", {})
                    self.mapa = estado.get("mapa", [])
            except Exception as e:
                print(f"Error recibiendo estado global: {e}")


        
    def enviar_mensaje(self):
        if not self.solicitud_enviada:
            if self.solicitudes_pendientes:
                self.solicitud_enviada = True
                solicitud = self.solicitudes_pendientes[0]  # Tomamos la primera solicitud pendiente
                destino_id = solicitud['Id']
                mensaje = {
                    'cliente_id': self.cliente_id,
                    'destino': destino_id,
                    'origen': self.posicion
                }
                try:
                    self.producer.send('cliente_solicitud', key=self.cliente_id.encode(),value=json.dumps(mensaje).encode())
                    self.log(f"[Cliente {self.cliente_id}] Solicitud enviada hacia {destino_id}")
                except Exception as e:
                    self.log(f"[Cliente {self.cliente_id}] Error al enviar solicitud: {e}")
                    self.solicitud_enviada = False
            else:
                self.log(f"[Cliente {self.cliente_id}] No hay más solicitudes pendientes.")
        else:
            self.log(f"[Cliente {self.cliente_id}] Ya se ha enviado una solicitud, en espera de una respuesta.")

    def recibir_respuestas(self):
        self.log(f"[Cliente {self.cliente_id}] Esperando respuestas...")

        while True:
            start_time = time.time()
            timeout = 120
            if time.time() - start_time > timeout:
                self.log(f"[Cliente {self.cliente_id}] Tiempo de espera agotado para respuestas.")
                break
            
            try:
                mensaje = next(self.consumer)
                if mensaje.key and mensaje.key.decode() != self.cliente_id:
                    continue
                respuesta = json.loads(mensaje.value.decode())
                id_cliente_response = respuesta.get('cliente_id')
                status = respuesta.get('estado')

                if id_cliente_response == self.cliente_id:
                    if status == 'OK':
                        self.log(f"[Cliente {self.cliente_id}] Solicitud procesada correctamente. Un taxi esta en camino.")
                        self.log(f"[Cliente {self.cliente_id}] Solicitudes cargadas: {self.solicitudes_pendientes}")
                    elif status == 'KO':
                        self.log(f"[Cliente {self.cliente_id}] Error al procesar la solicitud. No hay taxis disponibles.")
                        self.solicitud_enviada = False
                        print(f"[Cliente {self.cliente_id}] 4 segundos para volver a enviar la solicitud.")
                        time.sleep(4)
                        self.enviar_mensaje()
                    elif status == 'RECOGIDO':
                        self.log(f"[Cliente {self.cliente_id}] Cliente recogido, llegando a su destino.")
                    elif status == 'DESTINO':
                        self.log(f"[Cliente {self.cliente_id}] Cliente ha llegado a su destino.")
                        posicion = respuesta.get('posicion')
                        self.posicion = tuple(posicion)
                        self.solicitud_enviada = False
                        print(f"Cliente {self.cliente_id} ha llegado a su destino en {self.posicion}.")
                        # Ahora sí eliminamos la solicitud con éxito
                        self.solicitudes_pendientes.pop(0)
                        self.log(f"[Cliente {self.cliente_id}] 4 segundos para el siguiente movimiento.")
                        time.sleep(4)
                        self.enviar_mensaje()
                    elif status == 'ABANDONADO':
                        posicion = respuesta.get('posicion')
                        if posicion:
                            self.posicion = tuple(posicion)
                            self.solicitud_enviada = False
                            self.log(f"[Cliente {self.cliente_id}] El taxi me ha abandonado en {self.posicion}. Reintentando mi solicitud anterior en 4 segundos...")
                            time.sleep(4)
                            self.enviar_mensaje()
                        else:
                            self.log(f"[Cliente {self.cliente_id}] Recibido ABANDONADO pero sin posición. No puedo reintentar.")
                    else:
                        self.log(f"[Cliente {self.cliente_id}] Respuesta desconocida: {respuesta}")
            except StopIteration:
                self.log(f"[Cliente {self.cliente_id}] No hay más mensajes en espera.")
                break
            except Exception as e:
                self.log(f"[Cliente {self.cliente_id}] Error al recibir respuesta: {e}")
                break

    def log(self, mensaje):
        try:
            if hasattr(self, 'log_text'):
                self.log_text.insert(tk.END, mensaje + '\n')
                self.log_text.see(tk.END)
            else:
                print(mensaje)
        except Exception as e:
            print(f"[Cliente {self.cliente_id}] Error al registrar mensaje: {e}")

    def iniciar(self):
        hilo_kafka = threading.Thread(target=self.recibir_respuestas, daemon=True)
        hilo_kafka.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutar EC_Customer con parámetros de conexión y autenticación.")
    parser.add_argument('broker_ip', type=str, help='IP del Broker de Kafka')
    parser.add_argument('broker_puerto', type=int, help='Puerto del Broker de Kafka')
    parser.add_argument('cliente_id', type=str, help='ID del cliente (como letra)')
    parser.add_argument('requests', type=str, help='Archivo requests')

    args = parser.parse_args()
    broker_ip = args.broker_ip
    broker_puerto = args.broker_puerto
    cliente_id = args.cliente_id
    requests_path = args.requests
    ec_customer = EC_Customer(broker_ip, broker_puerto, requests_path, cliente_id)
    ec_customer.iniciar()
    ec_customer.iniciar_interfaz_grafica()
    ec_customer.ventana.mainloop()

    #try:
    #    while True:
    #        time.sleep(1)
    #except KeyboardInterrupt:
    #    print("[Cliente] Programa finalizado por el usuario.")