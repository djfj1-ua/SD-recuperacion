import socket
import threading
import time
import json
import argparse

class EC_DE:
    def __init__(self, sensores_ip, sensores_puerto):
        self.sensores_ip = sensores_ip
        self.sensores_puerto = sensores_puerto
        self.sensor_status = 'OK'  # Estado inicial de los sensores
        self.inicio_sensores()

    def inicio_sensores(self):
        threading.Thread(target=self.iniciar_servidor_sensores, daemon=True).start()

    def iniciar_servidor_sensores(self):
        self.servidor_sensores = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor_sensores.bind((self.sensores_ip, self.sensores_puerto))
        self.servidor_sensores.listen()
        print(f"[EC_DE] Servidor de sensores iniciado en {self.sensores_ip}:{self.sensores_puerto}")
        while True:
            print("[EC_DE] Esperando conexión de EC_S...")
            sensor_socket, direccion = self.servidor_sensores.accept()
            print(f"[EC_DE] Conexión de EC_S desde {direccion}")
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
                            print(f"[EC_DE] Sensor {self.sensores_ip} solicitó desconexión.")
                            break
                        lrc = mensaje[lrc_index+5:]
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#')
                            if campos[0] == 'SENSOR':
                                self.sensor_status = campos[1]
                                sensor_socket.send('ACK'.encode())
                                print(f"[EC_DE] Estado del sensor actualizado: {self.sensor_status}")
                            else:
                                sensor_socket.send('NACK'.encode())
                        else:
                            sensor_socket.send('NACK'.encode())
                    else:
                        sensor_socket.send('NACK'.encode())
                else:
                    break
            except Exception as e:
                print(f"[EC_DE] Error en la conexión con el sensor: {e}")
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
    
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ejecutar EC_DE con parámetros de conexión y autenticación.")

    parser.add_argument('sensores_ip', type=str, default='localhost', help='IP de EC_S')
    parser.add_argument('sensores_puerto', type=int, default=8888, help='Puerto de EC_S')

    args = parser.parse_args()

    sensores_ip = args.sensores_ip
    sensores_puerto = args.sensores_puerto

    ec_de = EC_DE(sensores_ip, sensores_puerto)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[EC_DE] Programa finalizado por el usuario.")
        ec_de.servidor_sensores.close()