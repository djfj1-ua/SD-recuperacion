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
            self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
            self.consumer = KafkaConsumer(
                'respuestas_clientes',
                bootstrap_servers=[self.broker_ip],
                group_id=f'cliente_{cliente_id}',  # Group ID único para cada cliente
                auto_offset_reset='earliest'
            )
            self.cliente_id = cliente_id
            self.solicitudes_pendientes = []
            self.solicitud_enviada = False
            self.cargar_solicitudes()  # Cargar solicitudes antes de iniciar la GUI

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
