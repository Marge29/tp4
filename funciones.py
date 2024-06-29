import requests
import json

def get_location():
    try:
        response = requests.get('https://ipinfo.io/json')
        response.raise_for_status()
        data = response.json()
        lat, lon = map(float, data['loc'].split(','))
        print(f"Lat = {lat}, Lon = {lon}")
        return lat, lon
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener la ubicación: {e}")
        return None, None

def geo_latlon():
    lat, lon = get_location()
    if lat is None or lon is None:
        return

    api_key = "2f66bd561ebc7e4bde0d2a8951df0098"
    base_url = "http://api.openweathermap.org/data/2.5/weather?"

    opcion_elegida = input("Ciudad o Geo? (escriba 'ciudad' o 'geo'): ").strip().lower()

    if opcion_elegida == 'ciudad':
        city_name = input("Ciudad: ")
        complete_url = f"{base_url}appid={api_key}&q={city_name}&units=metric&lang=es"
    elif opcion_elegida == 'geo':
        complete_url = f"{base_url}lat={lat}&lon={lon}&appid={api_key}&units=metric&lang=es"
    else:
        print("Opción no válida.")
        return

    try:
        response = requests.get(complete_url)
        response.raise_for_status()
        x = response.json()

        if x["cod"] != "404" and x["cod"] != "400":
            y = x["main"]
            temp_ext = y["temp"]
            presion = y["pressure"]
            humedad_ext = y["humidity"]
            z = x["weather"]
            descripcion_clima = z[0]["description"]

            print(f"Temperatura = {temp_ext} C\nPresión Atmosférica = {presion} hPa\nHumedad = {humedad_ext} %\nCielo = {descripcion_clima}")

            return temp_ext, presion, humedad_ext, descripcion_clima
        else:
            print("Ciudad no encontrada")
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener datos del clima: {e}")