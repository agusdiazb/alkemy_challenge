import pandas as pd
import logging
from extraccion_datos import current_date


logging.info('Normalizacion y procesamiento de los datos')

#Podria estandarizar esto (reemplazar la fecha por current_date')
df_museos = pd.read_csv('museos/2022-abril/museos-'
                        + current_date 
                        + '.csv', encoding='UTF-8')
df_salas_cines = pd.read_csv('salas_cines/2022-abril/salas_cines-'
                             + current_date 
                             + '.csv', encoding='UTF-8')
df_bibliotecas = pd.read_csv('bibliotecas/2022-abril/bibliotecas-'
                             + current_date 
                             + '.csv', encoding='UTF-8')


#creo una variable con las columnas de interés
cols_interes = ['cod_localidad', 'id_provincia', 'id_departamento',
                'categoría', 'provincia', 'localidad', 'nombre',
                'domicilio', 'código postal', 'número de teléfono',
                'mail', 'web']


#Reemplazo los nombres de las columnas de df_museos segun lo solicitado (solo de las columnas a utilizar para la tabla unica)
df_museos.rename(columns = {'Cod_Loc' : 'cod_localidad',
                            'IdProvincia' : 'id_provincia',
                            'IdDepartamento' : 'id_departamento',
                            'categoria' : 'categoría',
                            'direccion' : 'domicilio',
                            'CP' : 'código postal',
                            'telefono' : 'número de teléfono',
                            'Mail' : 'mail',
                            'Web' : 'web'},
                 inplace = True)


# Hacemos lo mismo con df_salas_cines
df_salas_cines.rename(columns = {'Cod_Loc' : 'cod_localidad',
                                 'IdProvincia' : 'id_provincia',
                                 'IdDepartamento' : 'id_departamento',
                                 'Categoría' : 'categoría', 
                                 'Provincia' : 'provincia',
                                 'Localidad' : 'localidad',
                                 'Nombre' : 'nombre',
                                 'Domicilio' : 'domicilio',
                                 'CP' : 'código postal',
                                 'Fuente':'fuente',
                                 'Teléfono' : 'número de teléfono',
                                 'Mail' : 'mail',
                                 'Web' : 'web'},
                      inplace = True)

# Hacemos lo mismo con df_bibliotecas
df_bibliotecas.rename(columns = {'Cod_Loc' : 'cod_localidad',
                                 'IdProvincia' : 'id_provincia',
                                 'IdDepartamento' : 'id_departamento',
                                 'Categoría' : 'categoría', 
                                 'Provincia' : 'provincia',
                                 'Localidad' : 'localidad',
                                 'Nombre' : 'nombre',
                                 'Domicilio' : 'domicilio',
                                 'CP' : 'código postal', 
                                 'Fuente':'fuente',
                                 'Teléfono' : 'número de teléfono',
                                 'Mail' : 'mail',
                                 'Web' : 'web'},
                      inplace = True)

# data frame con el total de columnas
tabla_unica_tot = pd.concat([df_museos, df_salas_cines, df_bibliotecas])

#data frame solo con las columnas de interes a normalizar
tabla_unica = tabla_unica_tot.loc[:, cols_interes]    #contiene solo las columnas de interes a normalizar
tabla_unica['fecha_de_carga'] = current_date


#Cantidad de registros totales por categoría

total_categoria = tabla_unica.groupby(['categoría']).size().to_frame(name = 'Total por categoría')

#Cantidad de registros totales por fuente
total_fuentes = tabla_unica_tot.groupby(['categoría','fuente']).size().to_frame(name = 'Total por fuente')

#Cantidad de registros por provincia y categoría

total_provincias_categorias = tabla_unica[['categoría', 'provincia']].value_counts().to_frame('Cantidad de registros por provincia y categoría')



#Creo tabla 2 que une Cantidad de registros totales por categoría, Cantidad de registros totales por fuente, Cantidad de registros por provincia y categoría
tabla2 = total_categoria.merge(total_fuentes, how='outer', left_index=True, right_index=True)
tabla2= tabla2.merge(total_provincias_categorias, how='outer', left_index=True, right_index=True)
tabla2['fecha_de_carga'] = current_date


#tabla de cines - Provincia con: Cantidad de pantallas, Cantidad de butacas, Cantidad de espacios INCAA

tabla_cine = df_salas_cines.loc[:, ['provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']].copy()
funciones = {'Pantallas': 'sum', 'Butacas': 'sum','espacio_INCAA' : 'sum'}
tabla_cine['fecha_de_carga'] = current_date