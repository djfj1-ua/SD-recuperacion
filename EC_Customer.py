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
        self.consumer = KafkaConsumer(
            'cliente_respuesta',
            bootstrap_servers=[self.broker_ip],
            #group_id=f'cliente_{cliente_id}', 
            #auto_offset_reset='earliest'
        )
        self.cliente_id = cliente_id
        self.solicitudes_pendientes = {}
        self.solicitud_enviada = False
        self.cargar_movimientos()

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
                        # Ahora sí eliminamos la solicitud con éxito
                        self.solicitudes_pendientes.pop(0)
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
                        self.log(f"[Cliente {self.cliente_id}] 4 segundos para el siguiente movimiento.")
                        time.sleep(4)
                        self.enviar_mensaje()
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

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Cliente] Programa finalizado por el usuario.")