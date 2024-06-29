import time
import random
import sqlite3
import logging
import json
import threading
import paho.mqtt.client as mqtt
from flask import Flask, jsonify, render_template
from datetime import datetime
from funciones import geo_latlon

# Configuración del logging para depuración
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')

app = Flask(__name__)

DATABASE = '/home/pablo/bdatos/datos_sensores.db'

# Configuración de MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensores/datos"

# Crear conexión al broker MQTT
client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)

def create_table():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS lectura_sensores (
                        id INTEGER PRIMARY KEY,
                        co2 REAL,
                        temp REAL,
                        hum REAL,
                        fecha TEXT,
                        lugar TEXT,
                        altura REAL,
                        presion REAL,
                        presion_nm REAL,
                        temp_ext REAL
                    )''')
    conn.commit()
    conn.close()

@app.route('/')
def index():
    return render_template('tabla_sensores_para_editar.html')

@app.route('/datos')
def datos():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM lectura_sensores')
    records = cursor.fetchall()
    conn.close()
    return jsonify([{
        'id': record[0],
        'co2': record[1],
        'temp': record[2],
        'hum': record[3],
        'fecha': record[4],
        'lugar': record[5],
        'altura': record[6],
        'presion': record[7],
        'presion_nm': record[8],
        'temp_ext': record[9],
    } for record in records])

def captura_datos():
    temp_ext, presion, humedad_ext, descripcion_clima = geo_latlon()
    print("Resultados= ", temp_ext, presion, humedad_ext, descripcion_clima)

    while True:
        try:
            lugar = input("Lugar de la captura de los datos: ")
            tipo_lugar = input("Tipo de lugar [au=abierto urbano] [an=abierto no urbano] [c=cerrado] ")
            superficie = int(input("Superficie aproximada del lugar [m2]: "))
            altura = int(input("Altura aproximada del lugar [m]: "))
            presion_nm = presion
            cant_capturas = int(input("Cantidad de capturas: "))
            delta_t_capturas = int(input("Tiempo entre capturas (segs) : "))
        except ValueError:
            print("Error al ingresar datos...")
            continue
        else:
            break

    cont = 0
    while cont < cant_capturas:
        cont += 1
        verdadero = 1
        if verdadero == 1:
            print("Datos Disponibles!")
            CO2_medido = random.uniform(250, 1100)
            temp_sensor = random.uniform(temp_ext, temp_ext + 10)
            humedad_relativa = random.uniform(40, 80)
            print("CO2: %d PPM" % CO2_medido)
            print("Temperatura: %0.2f degrees C" % temp_sensor)
            print("Humedad: %0.2f %% rH" % humedad_relativa)

            d = datetime.now()
            print("Fecha", d)
            timestampStr = d.strftime("%d-%b-%Y (%H:%M:%S.%f)")

            conn = sqlite3.connect(DATABASE)
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO lectura_sensores (co2, temp, hum, fecha, lugar, altura, presion, presion_nm, temp_ext)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (CO2_medido, temp_sensor, humedad_relativa, timestampStr, lugar, altura, presion, presion_nm, temp_ext))
            conn.commit()
            conn.close()

            print("Registro insertado..., acumulados:", cont, "\n")
            time.sleep(delta_t_capturas)
            print("\nEsperando nuevo registro de datos ...\n")

    print("Cierro conexión ...")

def publish_data():
    while True:
        try:
            conn = sqlite3.connect(DATABASE)
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM lectura_sensores')
            records = cursor.fetchall()
            conn.close()

            for record in records:
                data = {
                    'id': record[0],
                    'co2': record[1],
                    'temp': record[2],
                    'hum': record[3],
                    'fecha': record[4],
                    'lugar': record[5],
                    'altura': record[6],
                    'presion': record[7],
                    'presion_nm': record[8],
                    'temp_ext': record[9]
                }
                client.publish(MQTT_TOPIC, json.dumps(data))
                logging.debug(f"Datos publicados: {data}")
            
            # Espera 15 segundos antes de la siguiente publicación
            time.sleep(15)
        except Exception as e:
            logging.error(f"Error al publicar datos: {e}")
            time.sleep(5)

if __name__ == '__main__':
    create_table()

    # Iniciar el servidor Flask
    logging.info("Iniciando servidor Flask...")
    flask_thread = threading.Thread(target=app.run, kwargs={'host': '0.0.0.0', 'port': 5000})
    flask_thread.start()

    # Mostrar el menú principal 
    while True:
        if flask_thread.is_alive():
            break
        time.sleep(1)
        
    while True:
        print("\nMENU:")
        print("1. Iniciar MQTT (Publicador)")
        print("2. Agregar Registro Manualmente")
        print("3. Salir")

        opcion = input("Seleccione una opción (1/2/3): ")

        if opcion == "1":
            logging.info("Iniciando publicador MQTT...")
            thread_publish = threading.Thread(target=publish_data)
            thread_publish.start()
        
        elif opcion == "2":
            logging.info("Iniciando captura de datos...")
            captura_datos()
        
        elif opcion == "3":
            break
        
        else:
            print("Opción no válida. Intente de nuevo.")
