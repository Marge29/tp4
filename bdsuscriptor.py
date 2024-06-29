import sqlite3

NEW_DATABASE = '/home/pablo/Downloads/TP4/bdsuscriptor/suscriptor.db'

def create_suscriptor_table():
    conn = sqlite3.connect(NEW_DATABASE)
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

# Llamar a la función para crear la tabla si no existe
create_suscriptor_table()
