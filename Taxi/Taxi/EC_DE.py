import socket
import threading
import time
import json
import argparse

class EC_DE:
    def __init__(self, id_taxi, sensores_ip, sensores_puerto, central_ip, central_puerto, broker_ip, broker_puerto):
        self.sensores_ip = sensores_ip
        self.sensores_puerto = sensores_puerto
        self.central_ip = central_ip
        self.central_puerto = central_puerto
        self.broker_ip = broker_ip
        self.broker_puerto = broker_puerto
        self.id_taxi = id_taxi
        self.sensor_status = 'OK'  # Estado inicial de los sensores
        self.inicio_sensores()
        self.conectar_de()

    def inicio_sensores(self):
        threading.Thread(target=self.iniciar_servidor_sensores, daemon=True).start()

    def iniciar_servidor_sensores(self):
        self.servidor_sensores = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor_sensores.bind((self.sensores_ip, self.sensores_puerto))
        self.servidor_sensores.listen()
        print(f"[Taxi] Servidor de sensores iniciado en {self.sensores_ip}:{self.sensores_puerto}")
        while True:
            print("[Taxi] Esperando conexión de EC_S...")
            sensor_socket, direccion = self.servidor_sensores.accept()
            print(f"[Taxi] Conexión de EC_S desde {direccion}")
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
                            print(f"[Taxi] Sensor {self.sensores_ip} solicitó desconexión.")
                            break
                        lrc = mensaje[lrc_index+5:]
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#')
                            if campos[0] == 'SENSOR':
                                self.sensor_status = campos[1]
                                sensor_socket.send('ACK'.encode())
                                print(f"[Taxi] Estado del sensor actualizado: {self.sensor_status}")
                            else:
                                sensor_socket.send('NACK'.encode())
                        else:
                            sensor_socket.send('NACK'.encode())
                    else:
                        sensor_socket.send('NACK'.encode())
                else:
                    break
            except Exception as e:
                print(f"[Taxi] Error en la conexión con el sensor: {e}")
                break
            
        self.sensor_status = 'CONTINGENCY'

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
            print("[Taxi] Conectándose al EC_Central.")
            # Inicio del envío de datos
            threading.Thread(target=self.enviar_datos, daemon=True).start()
        except ConnectionRefusedError:
            print(f"[Taxi] No se pudo conectar al EC_Central en {self.ip_engine}:{self.puerto_engine}. Asegúrate de que EC_Central esté en ejecución.")
        except Exception as e:
            print(f"[Taxi] Ocurrió un error al intentar conectarse: {e}")
            exit(1)

    def enviar_datos(self):

        data = f'TAXI#{id_taxi}'
        lrc = self.calcular_lrc(data)
        mensaje = f'<STX>{data}<ETX><LRC>{lrc}'

        try:
            self.socket_de.send(mensaje.encode())
            #print(f"[Sensor] Enviando estado del sensor: {sensor_status}")
            # Esperar ACK
            respuesta = self.socket_de.recv(1024).decode()
            if respuesta != 'ACK':
                print(f"[Taxi] Error al autenticar taxi.")
                exit(1)
            else:
                print(f"[Taxi] Taxi autenticado con exito.")
        except Exception as e:
            print(f"[Taxi] Error al enviar estado del sensor: {e}")

    def reconectar_central(self):
        while not self.conectado_central:
            try:
                self.socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket_central.connect((self.central_ip, self.central_puerto))
                print("[EC_DE] Reconectado a EC_Central.")
                self.conectado_central = True
                self.autenticar()
                # Reanudar escuchando instrucciones
                threading.Thread(target=self.escuchar_instrucciones, daemon=True).start()
            except Exception as e:
                print(f"[EC_DE] No se pudo reconectar a EC_Central: {e}")
                time.sleep(5)
    
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

        ec_de = EC_DE(id_taxi, sensores_ip, sensores_puerto, central_ip, central_puerto, broker_ip, broker_puerto)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[Taxi] Programa finalizado por el usuario.")
            ec_de.servidor_sensores.close()
        
    except Exception as e:
        try:
            data = f'ERROR_TAXI#{id_taxi}'
            lrc = ec_de.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
            mensaje = "<STX>ERROR_TAXI<ETX><LRC>0"
            ec_de.socket_de.send(mensaje.encode())
            print("[Taxi] Error inesperado, se notificó a la Central.")
        except:
            print("[Taxi] Error fatal y no se pudo notificar a la Central.")
        finally:
            exit(1)