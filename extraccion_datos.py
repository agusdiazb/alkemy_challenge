import os
import requests
import logging
from datetime import datetime
from url import url_museos, url_salas_cines, url_bibliotecas

logging.basicConfig(level = logging.INFO,
                    format='%(asctime)s: %(levelname)s - %(message)s')

current_date = datetime.today().strftime("%d-%m-%Y")


def obtencion_organizacion(url, categoria):
    ''' 
    esta función toma como parametro un url y la categoria (en string) de una
    base de datos, la almacena localmente 
    y organizar los archivos en rutas:
    “categoría\año-mes\categoria-dia-mes-año.csv”
    '''
    datos = requests.get(url)
    #almacenamiento
    os.makedirs(categoria + '/2022-abril')
    #organizacion
    open(categoria +
         '/2022-abril/' +
         categoria +
         '-' +
         current_date +
         '.csv', 'wb').write(datos.content)
    
if __name__ == '__main__':
    #Ejecutamos la funcion para cada base de datos
    logging.info('Generación de csv')
    
    obtencion_organizacion(url_museos, 'museos')
    obtencion_organizacion(url_salas_cines, 'salas_cines')
    obtencion_organizacion(url_bibliotecas, 'bibliotecas')