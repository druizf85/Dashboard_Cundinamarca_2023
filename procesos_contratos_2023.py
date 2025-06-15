import pandas as pd
from datetime import date
import funciones.funciones_complementarias as fc
from dotenv import load_dotenv
import os

ruta_archivo = "DB.xlsx"
fc.delete_file(ruta_archivo)

load_dotenv()

username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

base_url1 = os.getenv("BASEURL1PROCESOS")

df = fc.extract_info_api(base_url1, username, password)

df_nombres = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación columnas json CS2')

nombres_nuevos = df_nombres.iloc[:, 0].tolist()
nombres_actuales = df_nombres.iloc[:, 1].tolist()

df.rename(columns=dict(zip(nombres_actuales, nombres_nuevos)), inplace=True)

cols=list(df.columns)
cols=[x.upper().strip() for x in cols]
df.columns=cols


df['FECHA DE FIRMA'] = pd.to_datetime(df['FECHA DE FIRMA'])
df['FECHA DE INICIO DEL CONTRATO'] = pd.to_datetime(df['FECHA DE INICIO DEL CONTRATO'])
df['FECHA DE FIN DEL CONTRATO'] = pd.to_datetime(df['FECHA DE FIN DEL CONTRATO'])

df_final = df[df['ESTADO CONTRATO'].isin(['En ejecución', 'Modificado', 'cedido', 'terminado','Cerrado','Activo','Prorrogado','Suspendido'])]
df_final = df_final[df_final['NOMBRE ENTIDAD'] != 'AGENCIA PÚBLICA DE EMPLEO DE CUNDINAMARCA APEC']
df_final1=df_final

df_final=df_final.reset_index().drop('index', axis=1)
df_final['MODALIDAD DE CONTRATACION'] = df_final['MODALIDAD DE CONTRATACION'].replace(['No definido','No Definido'], "Contratación directa")
df_final['JUSTIFICACION MODALIDAD DE CONTRATACION'] = df_final['JUSTIFICACION MODALIDAD DE CONTRATACION'].replace('No Especificado', "ServiciosProfesionales")

cruce_nombre_entidad = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación Entidades',index_col='NOMBRE ENTIDAD SECOP II PROCESOS')
cruce_nombre_modalidad = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación modalidades',index_col='MODALIDAD DE CONTRATACION')

df_final = pd.merge(df_final, cruce_nombre_entidad['DEPENDENCIA'], left_on='NOMBRE ENTIDAD', right_index=True, how='left')
df_final = pd.merge(df_final, cruce_nombre_modalidad['MODALIDAD GENERAL'], left_on='MODALIDAD DE CONTRATACION', right_index=True, how='left')
df_final['URLPROCESO'] = df_final['URLPROCESO'].astype(str)
df_final['ENLACE DEL PROCESO'] = df_final['URLPROCESO'].apply(fc.extract_full_url)
df_final=df_final.reset_index().drop('index', axis=1)
df_final.sort_values(by='FECHA DE FIRMA', ascending=False, inplace=True)

filtro_contratacion_directa = df_final["MODALIDAD GENERAL"] == "Contratación directa"
contratacion_directa_df = df_final[filtro_contratacion_directa]
contratacion_directa_df.drop_duplicates(subset="ENLACE DEL PROCESO", keep="first", inplace=True)

df_final = pd.concat([contratacion_directa_df, df_final[~filtro_contratacion_directa]])
df_final.drop_duplicates(subset='REFERENCIA DEL CONTRATO', inplace=True)
df_final=df_final.reset_index().drop('index', axis=1)

BDCS2=df_final

base_url2 = os.getenv("BASEURL2PROCESOS")

df = fc.extract_info_api(base_url2, username, password)

df_nombres = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación columnas json')

nombres_nuevos = df_nombres.iloc[:, 0].tolist()
nombres_actuales = df_nombres.iloc[:, 1].tolist()

df.rename(columns=dict(zip(nombres_actuales, nombres_nuevos)), inplace=True)

cols=list(df.columns)
cols=[x.upper().strip() for x in cols]
df.columns=cols

df['FECHA DE PUBLICACION DEL PROCESO'] = pd.to_datetime(df['FECHA DE PUBLICACION DEL PROCESO'])
df['FECHA DE ULTIMA PUBLICACIÓN'] = pd.to_datetime(df['FECHA DE ULTIMA PUBLICACIÓN'])
df['FECHA DE PUBLICACION (FASE SELECCION)'] = pd.to_datetime(df['FECHA DE PUBLICACION (FASE SELECCION)'])

df_publicacion=df.copy()
df.sort_values(by ='FECHA DE PUBLICACION DEL PROCESO',ascending = False,inplace=True)
df.drop_duplicates(subset='ID DEL PORTAFOLIO', keep='first', inplace=True)

df_publicacion.sort_values(by ='FECHA DE PUBLICACION DEL PROCESO',ascending = True,inplace=True)
df_publicacion.drop_duplicates(subset='ID DEL PORTAFOLIO', keep='first', inplace=True)
df_publicacion.set_index('ID DEL PORTAFOLIO',inplace=True)

df = pd.merge(df, df_publicacion['FECHA DE PUBLICACION DEL PROCESO'], left_on='ID DEL PORTAFOLIO', right_index=True, how='left')
df.rename(columns={'FECHA DE PUBLICACION DEL PROCESO_y': 'FECHA DE PUBLICACIÓN INICIAL','FECHA DE PUBLICACION DEL PROCESO_x': 'FECHA DE PUBLICACIÓN FINAL'}, inplace=True)

df_final = df[df['ENTIDAD'] != 'AGENCIA PÚBLICA DE EMPLEO DE CUNDINAMARCA APEC']

cruce_id_proceso = BDCS2
cruce_id_proceso.set_index("PROCESO DE COMPRA",inplace=True)
df_final = pd.merge(df_final, cruce_id_proceso[['FECHA DE FIRMA','FECHA DE INICIO DEL CONTRATO','VALOR DEL CONTRATO','DESTINO GASTO','ID CONTRATO', 'REFERENCIA DEL CONTRATO', 'ESTADO CONTRATO','DOCUMENTO PROVEEDOR', 'PROVEEDOR ADJUDICADO', 'ES GRUPO', 'ES PYME']], left_on='ID DEL PORTAFOLIO', right_index=True, how='left')

filtro = (df_final["ESTADO RESUMEN"] == 'Adjudicado') & (df_final['DOCUMENTO PROVEEDOR'].isna())

df_final.drop(df_final[filtro].index, inplace=True)
df_final['URLPROCESO'] = df_final['URLPROCESO'].astype(str)
df_final['ENLACE DEL PROCESO'] = df_final['URLPROCESO'].apply(fc.extract_url)

cruce_nombre_entidad = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación Entidades',index_col='NOMBRE ENTIDAD SECOP II PROCESOS')
cruce_nombre_modalidad = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación modalidades',index_col='MODALIDAD DE CONTRATACION')

df_final = pd.merge(df_final, cruce_nombre_entidad['DEPENDENCIA'], left_on='ENTIDAD', right_index=True, how='left')
df_final = pd.merge(df_final, cruce_nombre_modalidad['MODALIDAD GENERAL'], left_on='MODALIDAD DE CONTRATACION', right_index=True, how='left')

df=df_final

cruce_modalidad_general = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación estados procesos',index_col="ESTADO RESUMEN")

df_final = pd.merge(df_final, cruce_modalidad_general["ESTADO GENERAL"], left_on="ESTADO RESUMEN", right_index=True, how='left')

cruce_justificacion_general = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación Justificaciones m',index_col="JUSTIFICACIÓN MODALIDAD DE CONTRATACIÓN")

df_final = pd.merge(df_final, cruce_justificacion_general["JUSTIFICACIÓN MODALIDAD GENERAL"], left_on="JUSTIFICACIÓN MODALIDAD DE CONTRATACIÓN", right_index=True, how='left')

cruce_tipo_general = pd.read_excel("Cruces.xlsx", sheet_name='Clasificación Tipos de contrato',index_col="TIPO DE CONTRATO")

df_final = pd.merge(df_final, cruce_tipo_general["TIPO DE CONTRATO GENERAL"], left_on="TIPO DE CONTRATO", right_index=True, how='left')

df_final.loc[df_final['MODALIDAD GENERAL'] == 'RFI', 'ESTADO GENERAL'] = 'RFI'
df_final.loc[df_final['MODALIDAD GENERAL'] == 'RFI', 'JUSTIFICACIÓN MODALIDAD GENERAL'] = 'RFI'
df_final.loc[df_final['MODALIDAD GENERAL'] == 'RFI', "TIPO DE CONTRATO GENERAL"] = 'RFI'
df_final.loc[df_final['MODALIDAD GENERAL'] == 'RFI', "ESTADO CONTRATO"] = 'RFI'
df_final.loc[df_final['ESTADO GENERAL'] == 'En plataforma', "ESTADO CONTRATO"] = 'En proceso de contratación'

today = date.today()
df_final['HOY'] = today
df_final['HOY'] = pd.to_datetime(df_final['HOY'])

df_final=df_final.reset_index().drop('index', axis=1)
df_final.to_excel("DB.xlsx", sheet_name="DB")