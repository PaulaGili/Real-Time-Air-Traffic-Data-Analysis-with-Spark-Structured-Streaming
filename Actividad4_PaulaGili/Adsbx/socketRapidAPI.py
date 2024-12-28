import requests
import json
import time
import random
import socket
import re

puerto=21002#se debe cambiar por el número de puerto que tenéis asignado 

url = "https://aircraftscatter.p.rapidapi.com/lat/41.3/lon/2.1/"  # coordenadas de Barcelona

headers = {
    "X-RapidAPI-Key": "93069ceddamsh1946a5573ec51f2p10f774jsn92645eb9c752",  # Cambia esto por tu API Key
    "X-RapidAPI-Host": "aircraftscatter.p.rapidapi.com"
}


def prepara_json(datos):
    claves=[("flight",""),("lat",0),("lon",0), ("alt_baro",0),("category","") ]
    lista=datos['ac']
    listado=[]
    for elemento in lista:
        diccionario={}
        for clave in claves:
            diccionario[clave[0]]=elemento.get(clave[0],clave[1])
        listado.append(diccionario)
    datos['ac']=listado
    return datos

tiempo=0
contador=0

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
print("Esperando la conexión de Spark...")

s.bind(('localhost', puerto))
s.listen(1)
conn, addr = s.accept() 
print("Iniciando la conexión a AirCraftScatter ")

while (tiempo<60 and contador < 50):    
    inicio = time.time()
    respuesta=requests.get(url, headers=headers)    
    fin = time.time()
    tiempo=fin-inicio
    datos=prepara_json(respuesta.json())
    vuelos=datos.get('ac')
    n=len(vuelos)
    print()
    contador+=1
    print(contador,":","Aviones sobre Europa: ",n)
    datosLimpios=  json.dumps(datos)
    conn.sendall((datosLimpios+"\n").encode('utf-8'))  
    time.sleep(10) # espera para no sobrecargar el servidor con peticiones
conn.close()