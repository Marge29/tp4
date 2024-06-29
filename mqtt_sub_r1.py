import logging
import sqlite3
import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensores/datos"

NEW_DATABASE = '/home/pablo/Downloads/TP4/bdsuscriptor/suscriptor.db'

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Conectado al broker MQTT")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.error(f"Conexión fallida con código de resultado: {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        if not is_duplicate_date(data['fecha']):  # Verificar si existe
            insert_into_database(data)
    except Exception as e:
        logging.error(f"Error al procesar mensaje: {str(msg.payload)} - {e}")

def is_duplicate_date(new_date):
    try:
        conn = sqlite3.connect(NEW_DATABASE)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM lectura_sensores WHERE fecha = ?", (new_date,))
        count = cursor.fetchone()[0]
        
        conn.close()
        
        # Si count > 0, significa que ya existe un registro con esa fecha
        return count > 0
    except Exception as e:
        logging.error(f"Error al verificar duplicados por fecha: {e}")
        return False

def insert_into_database(data):
    try:
        conn = sqlite3.connect(NEW_DATABASE)
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO lectura_sensores (co2, temp, hum, fecha, lugar, altura, presion, presion_nm, temp_ext)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (data['co2'], data['temp'], data['hum'], data['fecha'], data['lugar'], data['altura'],
                        data['presion'], data['presion_nm'], data['temp_ext']))
        conn.commit()
        conn.close()
        logging.debug("Datos insertados en la base de datos del suscriptor")
    except Exception as e:
        logging.error(f"Error al insertar datos en la base de datos del suscriptor: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try:
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    logging.info("Conectando al broker MQTT")
    client.loop_forever()
except Exception as e:
    logging.error(f"Error al conectar al broker MQTT: {e}")

