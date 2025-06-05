import socket
import threading
import time
import argparse

class EC_S:
    def __init__(self, ip, puerto):
        self.ip = ip
        self.puerto = puerto
        self.parar = False  # Cuando está en true, tiene que hacer que el taxi pare
        self.activo = True
        self.conectar_de()

    def conectar_de(self):
        self.socket_de = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket_de.connect((self.ip, self.puerto))
            print("[Sensor] Conectándose al EC_DE.")
            # Inicio del envío de datos
            threading.Thread(target=self.enviar_datos, daemon=True).start()
            # Iniciar el hilo para la detección de parada
            threading.Thread(target=self.detectar_paro, daemon=True).start()
        except ConnectionRefusedError:
            print(f"[Sensor] No se pudo conectar al EC_DE en {self.ip}:{self.puerto}. Asegúrate de que EC_DE esté en ejecución.")
        except Exception as e:
            print(f"[Sensor] Ocurrió un error al intentar conectarse: {e}")
            exit(1)

    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)

    def enviar_datos(self):
        while self.activo:
            time.sleep(1)
            if self.parar:
                sensor_status = 'PARADA'
                #self.parar = False  # Se resetea la flag cuando se envía el error
            else:
                sensor_status = "OK"

            data = f'SENSOR#{sensor_status}'
            lrc = self.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}'

            try:
                if self.activo:
                    self.socket_de.send(mensaje.encode())
                #print(f"[Sensor] Enviando estado del sensor: {sensor_status}")
                # Esperar ACK
                respuesta = self.socket_de.recv(1024).decode()
                if respuesta != 'ACK':
                    print(f"[Sensor] Error al enviar estado del sensor.")
                else:
                    print(f"[Sensor] ACK recibido del EC_DE.")
            except Exception as e:
                print(f"[Sensor] Error al enviar estado del sensor: {e}")
                break  # Salir del bucle si se pierde conexión

    def detectar_paro(self):
        while self.activo:
            comando = input("[Sensor] Escriba 'p' para simular un error o 'q' para salir: ").strip().lower()
            if comando == 'p':
                if self.parar:
                    self.parar = False  # Resetea la flag si ya estaba en parada
                else:
                    self.parar = True
            elif comando == 'q':
                print("[Sensor] Enviando mensaje de desconexión al servidor...")
                try:
                    mensaje = "<STX>DISCONNECT<ETX><LRC>0"
                    self.socket_de.send(mensaje.encode())
                    self.activo = False  # Detener el envío de datos
                    time.sleep(2)
                    self.socket_de.close()  # Cierra el socket
                except Exception as e:
                    print(f"[Sensor] No se pudo notificar desconexión: {e}")
    
                print("[Sensor] Finalizando programa.")
                exit(0)
            else:
                print("[Sensor] Comando no reconocido. Use 'p' o 'q'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutar EC_S con parámetros de conexión.")
    parser.add_argument('ip', type=str, help='IP de EC_DE')
    parser.add_argument('puerto', type=int, help='Puerto de EC_DE')

    args = parser.parse_args()

    ip = args.ip
    puerto = args.puerto

    ec_s = EC_S(ip, puerto)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[SENSOR] Programa finalizado por el usuario.")
