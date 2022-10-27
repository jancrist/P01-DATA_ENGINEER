import pandas as pd
from sqlalchemy import create_engine

#APERTURA Y CONVERSION DE ARCHIVOS


#En esta etapa, abrimos todos los archivos originales que estaban en diferentes formatos, lo convertimos todo a csv para poder trabajarlo mejor. Todos estos archivos seran guardados en la carpeta allincsv. En muchos les agregamos el encoding siendo necesario. En todos le agregamos el index=False, para que nos elimine la columna 0.

def Abre_y_Convierte():
    

    psemana413_1=pd.read_csv(r'precios_semana_20200413.csv',encoding='utf-16')
    psemana413_1.to_csv(r'allincsv\psemana413_1csv',index=False)

    producto_2=pd.read_parquet(r'producto.parquet',engine='pyarrow')
    producto_2.to_csv(r'allincsv\producto_2csv',index=False)


     #En caso del Excel, como tenia dos paginas le agregamos el sheet_name=None, esto nos devolvio dos diccionarios, uno por cada sheet. Usamos Concat para poder concatenar los dos
    psemana426_3=pd.read_excel(r"precios_semanas_20200419_20200426.xlsx",sheet_name=None)
    psemana426_3_df=  pd.concat(psemana426_3, ignore_index=True)
    psemana426_3_df.to_csv(r'allincsv\psemana426_3csv',index=False)


    psemana503_4=pd.read_json(r"precios_semana_20200503.json")
    psemana503_4.to_csv(r'allincsv\psemana503_4csv',index=False)

    psemana508_5=pd.read_csv(r"precios_semana_20200518.txt",encoding='utf-8',sep='|')
    psemana508_5.to_csv(r'allincsv\psemana518_5csv',index=False)

    sucursal_6=pd.read_csv(r"sucursal.csv")
    sucursal_6.to_csv(r'allincsv\sucursal_6csv',index=False)

    
    
        
    return 'Realizado'
#Llamamos a la funcion

Abre_y_Convierte()



#ABRIMOS ARCHIVOS YA CONVERTIDOS
archivo_1=pd.read_csv(r"allincsv\psemana413_1csv")
archivo_2=pd.read_csv(r'allincsv\producto_2csv')
archivo_3=pd.read_csv(r"allincsv\psemana426_3csv")
archivo_4=pd.read_csv(r"allincsv\psemana503_4csv")
archivo_5=pd.read_csv(r'allincsv\psemana518_5csv')
archivo_6=pd.read_csv(r'allincsv\sucursal_6csv')



#Estas son algunas funciones que me ayudaron a limpiar los archivos 
def ModificaValoresDeColumna(dataframe,columna,toreplace,replaceby):
    
    if toreplace == None:
        inreplace = None
        if type(replaceby==str):
            replacedby=str(replaceby)
            return dataframe[columna].replace(to_replace=[inreplace], value=replacedby, inplace=True)


def RemplazaValoresColumna(dataframe,columna_in_str,to_replace_in_str,replaced_by_in_str):
     dataframe[columna_in_str]=dataframe[columna_in_str].astype('string').str.replace(to_replace_in_str,replaced_by_in_str)




#Iniciamos la limpieza de los archivos, al finalizar los guardamos en la carpeta archivos_limpios

#LIMPIAR ARCHIVO 1

archivo_1['precio']=archivo_1['precio'].round(2)


archivo_1.to_csv(r'Archivos_limpios\archivo_1_finish', sep=',',index=False)


#LIMPIANDO EL ARCHIVO 2
ModificaValoresDeColumna(archivo_2,'categoria1',None,'SIN CATEGORIA1')
ModificaValoresDeColumna(archivo_2,'categoria2',None,'SIN CATEGORIA2')
ModificaValoresDeColumna(archivo_2,'categoria3',None,'SIN CATEGORIA3')

archivo_2.to_csv(r'Archivos_limpios\archivo_2_finish', sep=',',index=False)

#LIMPIANDO ARCHIVO 3
archivo_3=archivo_3.fillna(0)
archivo_3['producto_id']=archivo_3['producto_id'].astype('string').str.rstrip('.0')
archivo_3['producto_id']=archivo_3['producto_id'].str.zfill(13)
archivo_3['sucursal_id']=archivo_3['sucursal_id'].astype('string').str.replace('/','-')
archivo_3['sucursal_id']=archivo_3['sucursal_id'].str.rstrip('00:00:00')
archivo_3.reindex(columns = ['precio', 'producto_id', 'sucursal_id'])

archivo_3.to_csv(r'Archivos_limpios\archivo_3_finish', sep=',',index=False)

#LIMPIANDO 4

archivo_4['precio']=archivo_4['precio'].round(2)
archivo_4.to_csv(r'Archivos_limpios\archivo_4_finish', sep=',',index=False)

#LIMPIANDO 5

archivo_5['precio']=archivo_5['precio'].round(2)
archivo_5.to_csv(r'Archivos_limpios\archivo_5_finish', sep=',',index=False)

#LIMPIANDO 6
archivo_6.to_csv(r'Archivos_limpios\archivo_6_finish', sep=',',index=False)




# ESTOS SON ALGUNOS DATOS NECESARIOS PARA ESTABLECER CONEXION SQL
cadena_conexion= 'mysql+pymysql://root:123456789@localhost:3306/demostracion'

conexion= create_engine(cadena_conexion)

#ABRIMOS ARCHIVOS LIMPIOS PARA LA CARGA A SQL
file_1=pd.read_csv(r'Archivos_limpios\archivo_1_finish')

#En este caso omitimos el archivo file_2, que es la tabla de productos. Ya que lo cargaremos manualmente. Esto se debe a un error con el modulo SqlAlchemy que no he podido resolver despues de mucha busqueda. 
#file_2=pd.read_csv(r'Archivos_limpios\archivo_2_finish')
file_3=pd.read_csv(r"Archivos_limpios\archivo_3_finish")
file_4=pd.read_csv(r'Archivos_limpios\archivo_4_finish')
file_5=pd.read_csv(r'Archivos_limpios\archivo_5_finish')
file_6=pd.read_csv(r'Archivos_limpios\archivo_6_finish')




#Esta es una funcion que se encarga de la carga de tablas Dataframe a Tablas MySql
def ImportaTablasAMySql(entry,nombre):  
    
    
    entry.to_sql(name=nombre, con=conexion)
#Llamamos a la funcion que carga las tablas a MySql
ImportaTablasAMySql(file_1,'Precios_Semana_0413')
#ImportaTablasAMySql(file_2,'Producto')
ImportaTablasAMySql(file_3,'Precios_Semana_426')
ImportaTablasAMySql(file_4,'Precios_Semana_503')
ImportaTablasAMySql(file_5,'Precios_Semana_508')
ImportaTablasAMySql(file_6,'Sucursal')


#Esta es una funcion que concatena las tablas de precio_semana. Para obtener una tabla general de Precios_Semana

def CargaIncremental():
    tabla_general=pd.concat([file_1,file_3,file_4,file_5])
    tabla_general.to_csv(r'Archivos_limpios\tabla_General')
    ImportaTablasAMySql(tabla_general,'Tabla_General')

#Llamamos a la funcion para crear la tabla general de precios_semana
CargaIncremental()


