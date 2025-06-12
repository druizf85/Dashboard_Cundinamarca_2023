import pandas as pd
from datetime import date
import funciones.funciones_complementarias as fc

ruta_archivo = "BDC.xlsx"
fc.delete_file(ruta_archivo)

base_url1 = "https://www.datos.gov.co/resource/ftd6-xejd.json"

df = fc.extract_info_api(base_url1)
df_nombres = pd.read_excel('Cruces.xlsx', sheet_name='Clasificación columnas json CS2')

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
df_final = df_final[df_final['NOMBRE ENTIDAD'] != 'GOBERNACIÓN DE CUNDINAMARCA  DESPACHO DEL GOBERNADOR']
df_final=df_final.reset_index().drop('index', axis=1)
df_final['MODALIDAD DE CONTRATACION'] = df_final['MODALIDAD DE CONTRATACION'].replace(['No definido','No Definido'], "Contratación directa")
df_final['JUSTIFICACION MODALIDAD DE CONTRATACION'] = df_final['JUSTIFICACION MODALIDAD DE CONTRATACION'].replace('No Especificado', "ServiciosProfesionales")

cruce_nombre_entidad = pd.read_excel('Cruces.xlsx', sheet_name='Clasificación Entidades',index_col='NOMBRE ENTIDAD SECOP II PROCESOS')
cruce_nombre_modalidad = pd.read_excel('Cruces.xlsx', sheet_name='Clasificación modalidades',index_col='MODALIDAD DE CONTRATACION')
cruce_tipo_general = pd.read_excel('Cruces.xlsx', sheet_name='Clasificación Tipos de contrato',index_col="TIPO DE CONTRATO")

df_final = pd.merge(df_final, cruce_nombre_entidad['DEPENDENCIA'], left_on='NOMBRE ENTIDAD', right_index=True, how='left')
df_final = pd.merge(df_final, cruce_nombre_modalidad['MODALIDAD GENERAL'], left_on='MODALIDAD DE CONTRATACION', right_index=True, how='left')
df_final = pd.merge(df_final, cruce_tipo_general["TIPO DE CONTRATO GENERAL"], left_on="TIPO DE CONTRATO", right_index=True, how='left')
df_final['URLPROCESO'] = df_final['URLPROCESO'].astype(str)
df_final['ENLACE DEL PROCESO'] = df_final['URLPROCESO'].apply(fc.extract_full_url)
df_final=df_final.reset_index().drop('index', axis=1)
df_final.sort_values(by='FECHA DE FIRMA', ascending=False, inplace=True)

filtro_contratacion_directa = df_final["MODALIDAD GENERAL"] == "Contratación directa"

contratacion_directa_df = df_final[filtro_contratacion_directa]
contratacion_directa_df.drop_duplicates(subset="ENLACE DEL PROCESO", keep="first", inplace=True)

df_final = pd.concat([contratacion_directa_df, df_final[~filtro_contratacion_directa]])
df_final['CODIGO DE CATEGORIA PRINCIPAL'] = df_final['CODIGO DE CATEGORIA PRINCIPAL'].replace('No Definido', 'V1.80111600')
df_final["ID SEGMENTO"] = df_final["CODIGO DE CATEGORIA PRINCIPAL"].apply(fc.extract_twonumbers_after_dot)
df_final["ID FAMILIA"] = df_final["CODIGO DE CATEGORIA PRINCIPAL"].apply(fc.extract_fournumbers_after_dot)
df_final["ID CLASE"] = df_final["CODIGO DE CATEGORIA PRINCIPAL"].apply(fc.extract_sixnumbers_after_dot)


cruce_nombre_segmento = pd.read_excel('Cruces.xlsx', sheet_name='Clasificador UNSPSC SEGMENTO')
cruce_nombre_segmento['Código Segmento'] = cruce_nombre_segmento['Código Segmento'].astype(str)
cruce_nombre_segmento.set_index('Código Segmento',inplace=True)

cruce_nombre_familia = pd.read_excel('Cruces.xlsx', sheet_name='Clasificador UNSPSC FAMILIA')
cruce_nombre_familia['Código Familia'] = cruce_nombre_familia['Código Familia'].astype(str)
cruce_nombre_familia.set_index('Código Familia',inplace=True)

cruce_nombre_clase = pd.read_excel('Cruces.xlsx', sheet_name='Clasificador UNSPSC CLASE')
cruce_nombre_clase['Código Clase'] = cruce_nombre_clase['Código Clase'].astype(str)
cruce_nombre_clase.set_index('Código Clase',inplace=True)

df_final = pd.merge(df_final, cruce_nombre_segmento['Nombre Segmento'], left_on="ID SEGMENTO", right_index=True, how='left')
df_final = pd.merge(df_final, cruce_nombre_familia['Nombre Familia'], left_on="ID FAMILIA", right_index=True, how='left')
df_final = pd.merge(df_final, cruce_nombre_clase['Nombre Clase'], left_on="ID CLASE", right_index=True, how='left')
df_final.drop_duplicates(subset='REFERENCIA DEL CONTRATO', keep="first", inplace=True)
df_final=df_final.reset_index().drop('index', axis=1)

BDCS2=df_final

base_url2 = "https://www.datos.gov.co/resource/m8fx-7c93.json"

df = fc.extract_info_api(base_url2)

df_nombres = pd.read_excel('Cruces.xlsx', sheet_name='Clasificación columnas json CS1')

nombres_nuevos = df_nombres.iloc[:, 0].tolist()
nombres_actuales = df_nombres.iloc[:, 1].tolist()

df.rename(columns=dict(zip(nombres_actuales, nombres_nuevos)), inplace=True)

cols=list(df.columns)
cols=[x.upper().strip() for x in cols]

df.columns=cols
df = df[df['NOM RAZON SOCIAL CONTRATISTA'] != 'No Definido']
df = df[df['NOMBRE ENTIDAD'] != 'CUNDINAMARCA  UNIDAD ADMINISTRATIVA ESPECIAL DE VIVIENDA SOCIAL']
df= pd.merge(df, cruce_nombre_modalidad['MODALIDAD GENERAL'], left_on='MODALIDAD DE CONTRATACION', right_index=True, how='left')
df= pd.merge(df, cruce_tipo_general["TIPO DE CONTRATO GENERAL"], left_on="TIPO DE CONTRATO", right_index=True, how='left')
df['FECHA DE FIRMA DEL CONTRATO'] = pd.to_datetime(df["FECHA DE FIRMA DEL CONTRATO"])
df["ID SEGMENTO"] = df["ID FAMILIA"].apply(fc.extract_first_two_numbers)
df = pd.merge(df, cruce_nombre_segmento['Nombre Segmento'], left_on="ID SEGMENTO", right_index=True, how='left')
df = pd.merge(df, cruce_nombre_familia['Nombre Familia'], left_on="ID FAMILIA", right_index=True, how='left')
df = pd.merge(df, cruce_nombre_clase['Nombre Clase'], left_on="ID CLASE", right_index=True, how='left')
df['RUTA PROCESO EN SECOP I'] = df['RUTA PROCESO EN SECOP I'].astype(str)
df['ENLACE DEL PROCESO'] = df['RUTA PROCESO EN SECOP I'].apply(fc.extract_full_url)

cruce_nombre_entidad = pd.read_excel('Cruces.xlsx', sheet_name='Clasificación Entidades',index_col='NOMBRE ENTIDAD 1')

df = pd.merge(df, cruce_nombre_entidad['DEPENDENCIA'], left_on='NOMBRE ENTIDAD', right_index=True, how='left')
df.drop_duplicates(subset='NUMERO DE CONTRATO', keep='first', inplace=True)
df=df.reset_index().drop('index', axis=1)

BDCS1=df

BDCS1['SEGMENTO'] = BDCS1["ID SEGMENTO"] + ' - ' + BDCS1['Nombre Segmento']
BDCS1['FAMILIA'] = BDCS1["ID FAMILIA"] + ' - ' + BDCS1['Nombre Familia']
BDCS1['CLASE'] = BDCS1["ID CLASE"] + ' - ' + BDCS1['Nombre Clase']

BDCS2['SEGMENTO'] = BDCS2["ID SEGMENTO"] + ' - ' + BDCS2['Nombre Segmento']
BDCS2['FAMILIA'] = BDCS2["ID FAMILIA"] + ' - ' + BDCS2['Nombre Familia']
BDCS2['CLASE'] = BDCS2["ID CLASE"] + ' - ' + BDCS2['Nombre Clase']

cruce_UNSPSC = pd.read_excel('Cruces.xlsx', sheet_name='UNSPSC')
cruce_UNSPSC['CRUCE CLASE'] = cruce_UNSPSC['CRUCE CLASE'].astype(str)
cruce_UNSPSC.set_index('CRUCE CLASE',inplace=True)

BDCS1 = pd.merge(BDCS1, cruce_UNSPSC['UNSPSC'], left_on="CLASE", right_index=True, how='left')
BDCS2 = pd.merge(BDCS2, cruce_UNSPSC['UNSPSC'], left_on="CLASE", right_index=True, how='left')

BDCS11=BDCS1[['NOMBRE ENTIDAD' , 'DEPENDENCIA','DEPARTAMENTO ENTIDAD','MUNICIPIO ENTIDAD', 'ESTADO DEL PROCESO', 'NUMERO DE PROCESO','NUMERO DE CONTRATO', 'DETALLE DEL OBJETO A CONTRATAR', 'FECHA DE FIRMA DEL CONTRATO', 'IDENTIFICACION DEL CONTRATISTA', 'NOM RAZON SOCIAL CONTRATISTA', 'CUANTIA CONTRATO', 'MODALIDAD GENERAL','TIPO DE CONTRATO GENERAL','SEGMENTO','FAMILIA','CLASE','UNSPSC','ENLACE DEL PROCESO']]
BDCS22=BDCS2[['NOMBRE ENTIDAD','DEPENDENCIA', 'DEPARTAMENTO', 'CIUDAD', 'ESTADO CONTRATO','PROCESO DE COMPRA','REFERENCIA DEL CONTRATO', 'DESCRIPCION DEL PROCESO', 'FECHA DE FIRMA', 'DOCUMENTO PROVEEDOR', 'PROVEEDOR ADJUDICADO', 'VALOR DEL CONTRATO','MODALIDAD GENERAL','TIPO DE CONTRATO GENERAL','SEGMENTO','FAMILIA','CLASE','UNSPSC','ENLACE DEL PROCESO']]

columnas=BDCS11.columns
BDCS22.columns=columnas

INTEGRADO = pd.concat([BDCS11, BDCS22])

INTEGRADO['PLATAFORMA'] = 'Otro'  
INTEGRADO.loc[INTEGRADO['ENLACE DEL PROCESO'].str.startswith('https://www.contratos.gov.co/'), 'PLATAFORMA'] = 'SECOP I'
INTEGRADO.loc[INTEGRADO['ENLACE DEL PROCESO'].str.startswith('https://community.secop.gov.co/'), 'PLATAFORMA'] = 'SECOP II'

reemplazos = {
    'Celebrado': 'En ejecución',
    'terminado': 'Terminado',
    'cedido': 'Cedido'
}

INTEGRADO['ESTADO DEL PROCESO'].replace(reemplazos, inplace=True)

today = date.today()
INTEGRADO['HOY'] = today
INTEGRADO['HOY'] = pd.to_datetime(INTEGRADO['HOY'])

BDCS1=BDCS1.reset_index().drop('index', axis=1)
BDCS2=BDCS2.reset_index().drop('index', axis=1)
INTEGRADO=INTEGRADO.reset_index().drop('index', axis=1)

excel_file_path = 'BDC.xlsx'

with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
    
    BDCS1.to_excel(writer, sheet_name='BDCS1', index=False)
    BDCS2.to_excel(writer, sheet_name='BDCS2', index=False)
    INTEGRADO.to_excel(writer, sheet_name='INTEGRADO', index=False)