#%%
import requests
import pandas as pd
import datetime
import os
import urllib3
import json
import sys
from pathlib import Path
# Agregar raíz del proyecto al path
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from Config.config import conn1
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.chdir(r'D:\ArchivosTrabajo\DESARROLLO\200. Bot_Mantra\Desarrollo\Descargas')

def generar_fechas(min_fecha, max_fecha):
    min_fecha = pd.to_datetime(min_fecha)
    max_fecha = pd.to_datetime(max_fecha)
    fechas = pd.date_range(start=min_fecha, end=max_fecha)
    return fechas.strftime('%Y-%m-%d').tolist()
# Fechas
hoy = datetime.datetime.now()
ayer = hoy - datetime.timedelta(days=2)
ayer = ayer.strftime('%Y-%m-%d')
hoy = hoy.strftime('%Y-%m-%d')
# Modo automático
ls_fechas = generar_fechas(ayer, hoy)
# Modo fijo (puedes cambiar las fechas si quieres)
#ls_fechas = generar_fechas('2026-03-21', '2026-03-30')
print(f"{ls_fechas[0]} -> {ls_fechas[-1]}")
#%%
url = "https://clientgateway.neoauto.com/v1/clientgateway/graphql"
headers = {"Content-Type": "application/json"}
query_token = """
mutation {
  invitedToken(
    invitedTokenInput: {
      clientId: 9
      clientSecret: "da0530b063c207a0760b8ad3b05f149e4292686d"
    }
  ) {
    token
    type
  }
}
"""
response = requests.post(url, headers=headers, json={"query": query_token}, verify=False).json()
token = response['data']['invitedToken']['token']
headers["Authorization"] = f"Bearer {token}"

ls_export = []
for fecha in ls_fechas:
    query = f"""
    query {{
        getSantanderCreditSimulation(date: "{fecha}") {{
            id
            advertisementId
            userId
            name
            documentNumber
            phone
            email
            salary
            laborRegime
            riskGroup
            result
            flow
            source
            type
            createdAt
            brand
            model
            manufactureYear
            modelYear
            vehicleCategory
            vehicleCondition
            publicationType
            fuel
            transmission
            price
            mileage
            url
            scvEvaluations {{
                strategy
                idTransaccion
                decision
            }}
        }}
    }}
    """
    resp = requests.post(url, headers=headers, json={"query": query}, verify=False).json()
    data = resp['data']['getSantanderCreditSimulation']
    if data:
        df = pd.DataFrame(data)
        ls_export.append(df)
if not ls_export:
    print("⚠ No se obtuvieron registros")
    exit()

data_export = pd.concat(ls_export, axis=0)
print(f"\n✅ Total de registros: {len(data_export)}")

# Procesamiento
# Convertir todo a string
for col in data_export.columns:
    if col not in ['id', 'fecha_registro', 'scvEvaluations']:
        data_export[col] = data_export[col].astype(str)
# Renombrar columnas
data_export.rename(columns={
    'advertisementId': 'advertisementid',
    'userId': 'userid',
    'name': 'nombre',
    'documentNumber': 'dni',
    'phone': 'telefono',
    'email': 'correo',
    'salary': 'ganancia',
    'laborRegime': 'laborregime',
    'riskGroup': 'riskgroup',
    'result': 'resultado',
    'flow': 'flow',
    'source': 'fuente',
    'type': 'tipo',
    'brand': 'marca',
    'model': 'modelo',
    'manufactureYear': 'anio_fabricacion',
    'modelYear': 'anio_modelo',
    'vehicleCategory': 'categoria_vehiculo',
    'vehicleCondition': 'condicion_vehiculo',
    'publicationType': 'tipo_publicacion',
    'fuel': 'tipo_combustible',
    'transmission': 'tipo_transmision',
    'price': 'precio',
    'mileage': 'kilometraje',
    'url': 'url_aviso',
    'scvEvaluations' : 'EVALUACIONES_API'
}, inplace=True)

data_export = data_export[(data_export['tipo'] == 'FULL_PROCESS')]
data_export['EVALUACIONES_API'] = data_export['EVALUACIONES_API'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else None)
data_export['telefono'] = ('00000' + data_export['telefono'].str.replace(' ', '').str.replace('+51', '')).str[-9:]
data_export['fecha'] = data_export['createdAt'].astype(str).str[:10]
data_export['fecha_registro'] = pd.to_datetime(data_export['createdAt'], errors='coerce')

#%%
# Leer registros ya existentes
data_export.drop_duplicates(subset=['id', 'dni', 'fecha_registro', 'tipo'], inplace=True)

query_exist = "SELECT id, dni, fecha_registro, tipo FROM [GOCREDITOS_BRONZE].[stga_scp].[MAESTRA_LEADS_NEOAUTO_V3]"
cursor = conn1.cursor()
cursor.execute(query_exist)
existing = set((str(row[0]), str(row[1]), str(row[2]), str(row[3])) for row in cursor.fetchall())
# Filtrar nuevos
df_nuevos = data_export[~data_export.apply(lambda x: (
    str(x['id']), str(x['dni']), str(x['fecha_registro']), str(x['tipo'])
) in existing, axis=1)]
print(f"\n✅ Total de registros nuevos: {len(df_nuevos)}")


if df_nuevos.empty:
    print("✅ No hay nuevos registros para insertar.")
else:
#   #%%
    # RIESGOS
    df_nuevos = df_nuevos.copy()
    df_nuevos["TIPO_DOCUMENTO"] = "DNI"  # O el valor que corresponda en tu caso
    df_nuevos["DOCUMENTO"] = df_nuevos["dni"].astype(str)

    # === 2. Leer base de riesgos ===
    input_path = r"D:\ArchivosTrabajo\DESARROLLO\200. Bot_Mantra\Data\pre_aprobados_riesgos.parquet"
    df_riesgos = pd.read_parquet(input_path, columns=["TIPO_DOCUMENTO", "DOCUMENTO", "SCORE_PROPENCION", "RESOLUCION"])
    df_riesgos["TIPO_DOCUMENTO"] = df_riesgos["TIPO_DOCUMENTO"].astype(str)
    df_riesgos["DOCUMENTO"] = df_riesgos["DOCUMENTO"].astype(str)
    df_riesgos["RESOLUCION"] = df_riesgos["RESOLUCION"].astype(str)

    # === 3. Crear diccionario de cruce ===
    dict_scores = {
        (td, doc): (score, resolucion)
        for td, doc, score, resolucion in zip(
            df_riesgos["TIPO_DOCUMENTO"],
            df_riesgos["DOCUMENTO"],
            df_riesgos["SCORE_PROPENCION"],
            df_riesgos["RESOLUCION"])}
    # === 4. Asignar campo SCORE_PROPENCION y RESOLUCION ===
    df_nuevos[["SCORE_PROPENCION", "RESOLUCION"]] = [dict_scores.get((td, doc), (6, '6. No Bancarizado'))
        for td, doc in zip(df_nuevos["TIPO_DOCUMENTO"], df_nuevos["DOCUMENTO"])]

#    #%%
    # PADRON DE PERSONAS
    input_path = r"D:\ArchivosTrabajo\DESARROLLO\200. Bot_Mantra\Data\padron_personas.parquet"
    df_padron = pd.read_parquet(input_path, columns=["TIPO_DOCUMENTO","NUMERO_DOCUMENTO","NOMBRES"])

    # === 2. Preparar data_export para el cruce ===
    df_padron["TIPO_DOCUMENTO"] = df_padron["TIPO_DOCUMENTO"].astype(str)
    df_padron["NUMERO_DOCUMENTO"] = df_padron["NUMERO_DOCUMENTO"].astype(str)
    df_padron["NOMBRES"] = df_padron["NOMBRES"].astype(str)
    # Filtrar solo DNI
    df_padron = df_padron[df_padron["TIPO_DOCUMENTO"] == "DNI"]

    # === 3. Crear diccionario DNI -> NOMBRE
    dict_padron = df_padron.set_index("NUMERO_DOCUMENTO")["NOMBRES"].to_dict()
    mask = df_nuevos["nombre"].isna() | (df_nuevos["nombre"].astype(str).str.strip().isin(["", "None", "nan"]))
    #data_export["nombre"].astype(str).str.strip().isin(["", "None", "nan"])
    df_nuevos.loc[mask, "nombre"] = (
        df_nuevos.loc[mask, "DOCUMENTO"]
        .map(dict_padron)
        .fillna(df_nuevos.loc[mask, "nombre"]))
    # #%% 
    # output_path = r"D:\ArchivosTrabajo\DESARROLLO\200. Bot_Mantra\Data\neoauto.xlsx"
    # data_export.to_excel(output_path, index=False)

#    #%%
    # INSERT MAESTRA_LEADS_NEOAUTO_V3
    df_nuevos.drop_duplicates(subset=['id', 'dni', 'fecha_registro', 'tipo'], inplace=True)
    # if df_nuevos.empty:
    #     print("✅ No hay nuevos registros para insertar.")
    # else:
    df_nuevos = df_nuevos.where(pd.notnull(df_nuevos), None)
    insert_query = """
    INSERT INTO [GOCREDITOS_BRONZE].[stga_scp].[MAESTRA_LEADS_NEOAUTO_V3] (
        id, advertisementid, userid, nombre, dni, telefono, correo,
        ganancia, laborregime, riskgroup, resultado, flow, fuente, tipo,
        fecha_registro, marca, modelo, anio_fabricacion, anio_modelo,
        categoria_vehiculo, condicion_vehiculo, tipo_publicacion,
        tipo_combustible, tipo_transmision, precio, kilometraje, url_aviso, fecha, score, resolucion, EVALUACIONES_API
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    columnas_sql = [
        'id', 'advertisementid', 'userid', 'nombre', 'dni', 'telefono', 'correo',
        'ganancia', 'laborregime', 'riskgroup', 'resultado', 'flow', 'fuente', 'tipo',
        'fecha_registro', 'marca', 'modelo', 'anio_fabricacion', 'anio_modelo',
        'categoria_vehiculo', 'condicion_vehiculo', 'tipo_publicacion',
        'tipo_combustible', 'tipo_transmision', 'precio', 'kilometraje', 'url_aviso', 'fecha', 'SCORE_PROPENCION', 'RESOLUCION', 'EVALUACIONES_API'
    ]
    data_to_insert = df_nuevos[columnas_sql].values.tolist()
    try:
        cursor.executemany(insert_query, data_to_insert)
        conn1.commit()
        print(f"✅ Se insertaron {len(data_to_insert)} registros nuevos.")
    except Exception as e:
        conn1.rollback()
        print(f"❌ Error al insertar: {e}")

#   #%%
    data_cosmos = df_nuevos[['id', 'TIPO_DOCUMENTO', 'dni', 'EVALUACIONES_API']].copy()
    data_cosmos.rename(columns={'dni': 'NRO_DOCUMENTO', 'id': 'ID_ORIGEN'}, inplace=True)
    data_cosmos["ORIGEN"] = "NEO"
    MAP_COD_TIPO_DOCUMENTO = {"DNI": 171,"CE": 172,"PASAPORTE": 173,"RUC": 174}
    data_cosmos['COD_TIPO_DOCUMENTO'] = (data_cosmos['TIPO_DOCUMENTO'].astype(str).str.strip().str.upper().map(MAP_COD_TIPO_DOCUMENTO))
    data_cosmos['EVALUACIONES_API'] = data_cosmos['EVALUACIONES_API'].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip() else [])
    # Una fila por evaluación
    data_cosmos = data_cosmos.explode('EVALUACIONES_API')
    data_cosmos['ID_TRANSACCION'] = data_cosmos['EVALUACIONES_API'].apply(lambda x: str(x.get('idTransaccion')).strip() if isinstance(x, dict) and x.get('idTransaccion') else None)
    data_cosmos['ESTRATEGIA'] = data_cosmos['EVALUACIONES_API'].apply(lambda x: x.get('strategy') if isinstance(x, dict) else None)
    data_cosmos['DECISION'] = data_cosmos['EVALUACIONES_API'].apply(lambda x: x.get('decision') if isinstance(x, dict) else None)
    # Eliminar JSON original
    data_cosmos.drop(columns=['EVALUACIONES_API'], inplace=True)
    # Eliminar registros sin ID_TRANSACCION
    data_cosmos = data_cosmos[data_cosmos['ID_TRANSACCION'].notna()].copy()
    # Eliminar duplicados
    data_cosmos.drop_duplicates(subset=['ID_TRANSACCION'], keep='first', inplace=True)
    data_cosmos.reset_index(drop=True, inplace=True)
    print(f"✅ Total registros para Cosmos: {len(data_cosmos):,}")

#   #%%
    # Leer registros ya existentes
    query_exist = "SELECT ID_TRANSACCION FROM GOCREDITOS_BRONZE.stga_scp.MAESTRA_LOG_API_COSMOS"
    cursor = conn1.cursor()
    cursor.execute(query_exist)
    existing = set((str(row[0])) for row in cursor.fetchall())
    # Filtrar nuevos
    df_nuevos_cosmos = data_cosmos[~data_cosmos.apply(lambda x: (str(x['ID_TRANSACCION'])) in existing, axis=1)].copy()
    print(f"\n✅ Total de registros Cosmos nuevos: {len(df_nuevos_cosmos)}")

#   #%%
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading
    import requests
    import time
    # CONFIGURACIÓN
    MAX_WORKERS = 50 # MDOFICAR LOS WORKERS EN BASE A LAS CANTIDADES
    url_cosmos = "https://az-eastus-scp-prd-mdscp-apims.azure-api.net/api/log-motor/bd/cosmos"
    thread_local = threading.local()
    def get_session():
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
            thread_local.session.headers.update({"Content-Type": "application/json"})
        return thread_local.session
    df_nuevos_cosmos["RESPUESTA_COSMOS"] = None
    df_nuevos_cosmos["CODIGO_ESTADO"] = None
    df_nuevos_cosmos["LOG_PROCESO"] = None
    t_api = time.time()
    t_bloque = t_api
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futuros = {
            executor.submit(
                get_session().post,
                url_cosmos,
                json={
                    "BD": "Vehicular-Retail",
                    "tipo_consulta": "id",
                    "id_transaccion": row["ID_TRANSACCION"],
                    "tipo_documento": row["COD_TIPO_DOCUMENTO"],
                    "documento": row["NRO_DOCUMENTO"]
                }, verify=False, timeout=60): row["ID_TRANSACCION"]
            for _, row in df_nuevos_cosmos.iterrows()}

        for i, futuro in enumerate(as_completed(futuros), 1):
            id_tx = futuros[futuro]
            try:
                response = futuro.result()
                codigo_estado = getattr(response, "status_code", None)
                try:
                    respuesta = response.json()
                except Exception:
                    respuesta = None
                df_nuevos_cosmos.loc[df_nuevos_cosmos["ID_TRANSACCION"] == id_tx, "RESPUESTA_COSMOS"] = [respuesta]
                df_nuevos_cosmos.loc[df_nuevos_cosmos["ID_TRANSACCION"] == id_tx, "CODIGO_ESTADO"] = response.status_code
                df_nuevos_cosmos.loc[df_nuevos_cosmos["ID_TRANSACCION"] == id_tx, "LOG_PROCESO"] = ( f"Conexion correcta: HTTP {response.status_code}"
                        if response.status_code == 200 else f"HTTP {response.status_code}")
            except Exception as e:
                    df_nuevos_cosmos.loc[df_nuevos_cosmos["ID_TRANSACCION"] == id_tx, "RESPUESTA_COSMOS"] = None,
                    df_nuevos_cosmos.loc[df_nuevos_cosmos["ID_TRANSACCION"] == id_tx, "CODIGO_ESTADO"] = response.status_code,
                    df_nuevos_cosmos.loc[df_nuevos_cosmos["ID_TRANSACCION"] == id_tx, "LOG_PROCESO"] = f"ERROR: {str(e)}"
            if i % 100 == 0 or i == len(futuros):
                print(f"Procesados: {i:,}/{len(futuros):,}")
                print(f"Tiempo: {(time.time() - t_bloque):.1f}s")
                t_bloque = time.time()

    tiempo_api = time.time() - t_api
    print(f"Tiempo API: {(tiempo_api):.2f}s ({tiempo_api/60:.2f} min)")
    print(f"✅ Cosmos terminado: {len(df_nuevos_cosmos):,} registros")

#    #%% 
    # EXTRAER VARIABLES DE RESPUESTA_COSMOS
    import ast
    def parse_json(x):
        if isinstance(x, (dict, list)):
            return x
        if x is None:
            return None
        try:
            return json.loads(x)
        except Exception:
            try:
                return ast.literal_eval(x)
            except Exception:
                return None

    def get_path(obj, path, default=None):
        try:
            for key in path:
                if isinstance(key, int):
                    obj = obj[key]
                else:
                    obj = obj.get(key)
                if obj is None:
                    return default
            return obj
        except (KeyError, IndexError, TypeError):
            return default

    def valor_risk_data(risk_data, variable):
        if not isinstance(risk_data, list):
            return None
        valores = []
        for r in risk_data:
            if not isinstance(r, dict) or r.get("variable") != variable:
                continue
            valor = (r.get("valor_text") if r.get("valor_text") is not None else
                r.get("valor_int") if r.get("valor_int") is not None else
                r.get("valor_decimal") if r.get("valor_decimal") is not None else
                r.get("valor_date"))
            if valor is not None:
                valores.append(valor)
        return max(valores) if valores else None

    def extraer_cosmos(row):
        data = parse_json(row["RESPUESTA_COSMOS"])
        # ESTRUCTURA BASE
        if isinstance(data, list):
            base = data[0] if data else {}
        elif isinstance(data, dict):
            base = data
        else:
            base = {}

        log = base.get("log", {})
        request = log.get("request", {})
        response = log.get("response", {})
        datos_eval = request.get("datos_Evaluacion", {})
        cliente = datos_eval.get("datos_cliente", {})
        canal = datos_eval.get("datos_canal", {})
        financiamiento = datos_eval.get("datos_financiamiento", {})
        cuota_inicial = financiamiento.get("cuota_inicial", {})
        seguros = financiamiento.get("seguros", {})
        seguro_vehicular = seguros.get("seguro_vehicular", {})
        ingreso = datos_eval.get("ingreso_declarado", {})
        titular = ingreso.get("titular", {})
        parametros = request.get("Parametros_Globales", {})

        # RISK DATA V2
        risk_v2 = response.get("risk_data_v2", {})
        flags = risk_v2.get("flags_contingencias", {})
        insumos_mem = risk_v2.get("insumos_calculo_mem", {})
        personas = insumos_mem.get("personas", [])
        persona = personas[0] if personas else {}
        factores = persona.get("factores_conversion", {})
        inputs = persona.get("inputs", {})
        externas = inputs.get("externas", {})
        internas = inputs.get("internas", {})
        resultado_mem = persona.get("resultado", {})
        resultado_global = insumos_mem.get("resultado_global", {})

        deudas = externas.get("deudas", [])
        deuda = lambda i: deudas[i] if i < len(deudas) and isinstance(deudas[i], dict) else {}

        # MODELOS
        modelos = risk_v2.get("modelos", {})
        score_go = modelos.get("santander_score_go", {})
        sgo_inputs = score_go.get("inputs", {})
        sgo_outputs = score_go.get("outputs", {})

        seg_f = modelos.get("segmentacion_f", {})
        seg_f_inputs = seg_f.get("inputs", {})
        seg_f_outputs = seg_f.get("outputs", {})

        seg_gr = modelos.get("segmentacion_gr", {})
        seg_gr_inputs = seg_gr.get("inputs", {})
        seg_gr_outputs = seg_gr.get("outputs", {})

        vintage = modelos.get("segmento_gr_vintage", {})
        vintage_inputs = vintage.get("inputs", {})

        oferta = risk_v2.get("oferta_comercial", {})

        # RISK_DATA
        risk_data = log.get("risk_data", [])

        rd = {
            "ESTADO_CIVIL": valor_risk_data(risk_data, "Estado Civil"),
            "EDAD": valor_risk_data(risk_data, "Edad"),
            "SUNEDU": valor_risk_data(risk_data, "Sunedu"),
            "PROPIEDADES": valor_risk_data(risk_data, "Propiedades"),
            "SANTANDER_GO": valor_risk_data(risk_data, "Santander Go"),
            "DEMOGRAFICO": valor_risk_data(risk_data, "Demografico"),
            "ADVANCE_SCORE": valor_risk_data(risk_data, "Advance Score"),
            "SEGMENTO_V3": valor_risk_data(risk_data, "Segmento v3"),
            "HISTORIAL_CREDITICIO": valor_risk_data(risk_data, "Historial Crediticio"),
            "SEGMENTO_G": valor_risk_data(risk_data, "Segmento G"),
            "SEGMENTO_R": valor_risk_data(risk_data, "Segmento R"),
            "CAMPANA": valor_risk_data(risk_data, "Campaña"),
            "BONO": valor_risk_data(risk_data, "Bono"),
            "SEGURO": valor_risk_data(risk_data, "Seguro"),
            "GPS": valor_risk_data(risk_data, "GPS"),
            "PRICING": valor_risk_data(risk_data, "Pricing"),
            "SEGMENTO_D": valor_risk_data(risk_data, "Segmento D"),
            "SEGMENTO_S": valor_risk_data(risk_data, "Segmento S"),
            "SEGMENTO_F": valor_risk_data(risk_data, "Segmento F"),
            "SGO_ESTIMADOR_INGRESOS_TITULAR": valor_risk_data(risk_data, "SGO - Estimador de ingresos Titular"),
            "SGO_ESTIMADOR_INGRESOS_CONYUGE": valor_risk_data(risk_data, "SGO - Estimador de ingresos Cónyuge"),
            "SGO_CATEGORIA_LABORAL_TITULAR": valor_risk_data(risk_data, "SGO - Categoria laboral Titular"),
            "SGO_DOCUMENTO_MOROSOS": valor_risk_data(risk_data, "SGO - Documento Morosos"),
            "SGO_NRO_VECES_INGRESO_RISK_DATA": valor_risk_data(risk_data, "SGO - Nro Veces Ingreso"),
            "SGO_CUOTA_INICIAL_RISK_DATA": valor_risk_data(risk_data, "SGO - Cuota Inicial"),
            "SGO_PROMEDIO_SALDO_NORMAL_12M": valor_risk_data(risk_data, "SGO - Promedio Saldo Normal 12m"),
            "CONTIENE_TODAS_VARIABLES_SCORECARD": valor_risk_data(risk_data, "Contiene todas las variables del Scorecard"),
            "ADVANCE_INCOME": valor_risk_data(risk_data, "Advance Income"),
            "TEA_PIZARRA": valor_risk_data(risk_data, "TEA-Pizarra"),
            "FLAG_CONTINGENCIA_EQUIFAX_RISK_DATA": valor_risk_data(risk_data, "flag_contingencia_equifax"),
            "FLAG_CONTINGENCIA_EXPERIAN_RISK_DATA": valor_risk_data(risk_data, "flag_contingencia_experian"),
            "FLAG_CONTINGENCIA_EXPERIAN_INC_PRECISE_RISK_DATA": valor_risk_data(risk_data, "flag_contingencia_experian_inc_precise"),
            "SEGMENTO_G_VINTAGE": valor_risk_data(risk_data, "Segmento G vintage"),
            "SEGMENTO_R_VINTAGE": valor_risk_data(risk_data, "Segmento R vintage"),
            "SEGMENTO_IE": valor_risk_data(risk_data, "Segmento IE"),
            "SEGMENTO_PD": valor_risk_data(risk_data, "Segmento PD"),
            "FLAG_PROPIEDADES_EDITADO": valor_risk_data(risk_data, "flag_propiedades_editado"),
            "FLAG_SUNEDU_EDITADO": valor_risk_data(risk_data, "flag_sunedu_editado"),
            "ZONA_GRIS_AGIL_SEGMENTO_G": valor_risk_data(risk_data, "Zona Gris Ágil - Segmento G"),
            "ZONA_GRIS_AGIL_SEGMENTO_R": valor_risk_data(risk_data, "Zona Gris Ágil - Segmento R"),
            "ZONA_GRIS_AGIL_SANTANDER_GO": valor_risk_data(risk_data, "Zona Gris Ágil - Santander Go")
        }

        # RESULTADO
        return {
            # DATOS GENERALES
            "ID_RESPUESTA": base.get("id"),
            "FECHA_RESPUESTA": base.get("fecha"),
            "SCHEMA_VERSION": base.get("schema_version"),
            "ID_TRANSACCION_COSMOS": log.get("id_transaccion"),
            "JSON_VERSION": log.get("json_version"),

            # CLIENTE
            "NRO_DOCUMENTO_COSMOS": cliente.get("numero_documento"),
            "COD_TIPO_DOCUMENTO_COSMOS": cliente.get("tipo_documento"),

            # CANAL
            "CONCESIONARIO": canal.get("concesionario"),
            "SUCURSAL": canal.get("sucursal"),
            "VENDEDOR": canal.get("vendedor"),

            # PRODUCTO / PARAMETROS
            "PRODUCTO": datos_eval.get("producto"),
            "APLICACION": parametros.get("aplicacion"),
            "CANAL": parametros.get("canal"),
            "IDENTIFICADOR": parametros.get("identificador"),
            "TIPO_EVALUACION": parametros.get("tipo_Evaluacion"),
            "TIPO_IDENTIFICADOR": parametros.get("tipo_Identificador"),
            "TIPO_LLAMADA": parametros.get("tipo_llamada"),

            # FINANCIAMIENTO
            "ALIADO": financiamiento.get("aliado"),
            "ANO_FABRICACION": financiamiento.get("ano_fabricacion"),
            "MARCA": financiamiento.get("marca"),
            "MODELO": financiamiento.get("modelo"),
            "MONEDA_FINANCIAMIENTO": financiamiento.get("moneda_financiamiento"),
            "MONTO_A_FINANCIAR": financiamiento.get("monto_a_financiar"),
            "PLAZO": financiamiento.get("plazo"),
            "TIPO_CAMBIO": financiamiento.get("tipo_cambio"),
            "TIPO_GPS": financiamiento.get("tipo_GPS"),
            "TIPO_VEHICULO": financiamiento.get("tipo_vehiculo"),
            "TOTAL_FINANCIAMIENTO": financiamiento.get("total_financiamiento"),
            "VALOR_VEHICULO_USD": financiamiento.get("valor_vehiculo_USD"),
            "VERSION": financiamiento.get("version"),

            # CUOTA INICIAL
            "CUOTA_INICIAL_MONEDA": cuota_inicial.get("moneda"),
            "CUOTA_INICIAL_MONTO": cuota_inicial.get("monto"),
            "CUOTA_INICIAL_PORCENTAJE": cuota_inicial.get("porcentaje"),

            # SEGURO VEHICULAR
            "SEGURO_COMPANIA": seguro_vehicular.get("compañia"),
            "SEGURO_TASA": seguro_vehicular.get("tasa"),

            # INGRESO DECLARADO
            "CATEGORIA_LABORAL": titular.get("categoria_Laboral"),
            "FECHA_INICIO_ACTIVIDAD": titular.get("fecha_Inicio_Actividad"),
            "INGRESO_ANUALIZADO": titular.get("ingreso_Anualizado"),
            "INGRESO_MENSUAL": titular.get("ingreso_Mensual"),
            "INGRESO_MONEDA": titular.get("moneda"),
            "RUC": titular.get("ruc"),

            # CHECKLIST DOCUMENTAL
            **{f"CHECKLIST_{i}_{campo.upper()}":
                get_path(response, ["Checklist_Documental", i - 1, campo])
                for i in range(1, 6)
                for campo in ["codigo", "descripcion"]},

            # POLITICA CREDITOS
            "CFC": get_path(response, ["Politica_Creditos", "Capacidad_Endeudamiento", "CFC"]),
            "PERIODO_RCC": get_path(response, ["Politica_Creditos", "Capacidad_Endeudamiento", "periodo_RCC"]),

            **{f"MAX_END_{i}_{campo.upper()}":
                get_path(response, ["Politica_Creditos", "Capacidad_Endeudamiento", "maximo_Endeudamiento", i - 1, campo])
                for i in range(1, 6)
                for campo in ["CFM", "cuota_Externa", "cuota_Interna", "MCM", "MEM"]},

            # PRICING
            "TEA": get_path(response, ["Pricing", "TEA"]),

            # RESULTADOS RISK DATA V2
            "TEA_CMA": get_path(risk_v2, ["detalle_decision", "tea_cma"]),
            "TEA_MOTOR": get_path(risk_v2, ["detalle_decision", "tea_motor"]),
            "TEA_MOTOR_FORMATEADA": get_path(risk_v2, ["detalle_decision", "tea_motor_formateada"]),
            "TEA_TARIFARIO": get_path(risk_v2, ["detalle_decision", "tea_tarifario"]),

            # FLAGS CONTINGENCIAS
            "FLAG_CONTINGENCIA_EQUIFAX": flags.get("flag_contingencia_equifax"),
            "FLAG_CONTINGENCIA_EXPERIAN_ADV_SCORE": flags.get("flag_contingencia_experian_adv_score"),
            "FLAG_CONTINGENCIA_EXPERIAN_INC_PRECISE": flags.get("flag_contingencia_experian_inc_precise"),

            # INSUMOS MEM
            "MEM_INGRESOS": insumos_mem.get("ingresos"),
            "MEM_ROL": persona.get("rol"),
            "FACTOR_COMERCIAL": factores.get("factor_comercial"),
            "FACTOR_CONSUMO": factores.get("factor_consumo"),
            "FACTOR_HIPOTECA": factores.get("factor_hipoteca"),
            "FACTOR_LINEA": factores.get("factor_linea"),

            # DEUDAS EXTERNAS
            "DEUDA_HIPOTECA_SALDO": deuda(0).get("saldo"),
            "DEUDA_1_TIPO": deuda(0).get("tipo"),
            "DEUDA_CONSUMO_SALDO": deuda(1).get("saldo"),
            "DEUDA_2_TIPO": deuda(1).get("tipo"),
            "DEUDA_LINEA_DISPONIBLE_SALDO": deuda(2).get("saldo"),
            "DEUDA_3_TIPO": deuda(2).get("tipo"),
            "DEUDA_LINEA_OTORGADA_SALDO": deuda(3).get("saldo"),
            "DEUDA_4_TIPO": deuda(3).get("tipo"),
            "DEUDA_CORPORATIVA_SALDO": deuda(4).get("saldo"),
            "DEUDA_5_TIPO": deuda(4).get("tipo"),
            "DEUDA_GRANDE_EMPRESA_SALDO": deuda(5).get("saldo"),
            "DEUDA_6_TIPO": deuda(5).get("tipo"),
            "DEUDA_MEDIANA_EMPRESA_SALDO": deuda(6).get("saldo"),
            "DEUDA_7_TIPO": deuda(6).get("tipo"),
            "DEUDA_PEQUENA_EMPRESA_SALDO": deuda(7).get("saldo"),
            "DEUDA_8_TIPO": deuda(7).get("tipo"),
            "DEUDA_MICROEMPRESA_SALDO": deuda(8).get("saldo"),
            "DEUDA_9_TIPO": deuda(8).get("tipo"),
            "MEM_PERIODO_RCC": externas.get("periodo_rcc"),

            # INTERNAS
            "CUOTA_MENSUAL_INTERNA": internas.get("cuota_mensual"),
            "FECHA_PROCESO_INTERNA": internas.get("fecha_proceso"),
            "SALDO_TOTAL_INTERNO": internas.get("saldo_total"),

            # RESULTADO MEM
            "CUOTA_EXTERNA": resultado_mem.get("cuota_externa"),
            "CUOTA_INTERNA": resultado_mem.get("cuota_interna"),
            "CUOTA_MENSUAL_COMERCIAL": resultado_mem.get("cuota_mensual_comercial"),
            "CUOTA_MENSUAL_CONSUMO": resultado_mem.get("cuota_mensual_consumo"),
            "CUOTA_MENSUAL_HIPOTECA": resultado_mem.get("cuota_mensual_hipoteca"),
            "CUOTA_MENSUAL_LINEA": resultado_mem.get("cuota_mensual_linea"),

            # RESULTADO GLOBAL
            "RESULTADO_GLOBAL_CFM": resultado_global.get("cfm"),
            "RESULTADO_GLOBAL_CUOTA_EXTERNA": resultado_global.get("cuota_externa_total"),
            "RESULTADO_GLOBAL_CUOTA_INTERNA": resultado_global.get("cuota_interna_total"),
            "RESULTADO_GLOBAL_MCM": resultado_global.get("mcm"),
            "RESULTADO_GLOBAL_MEM": resultado_global.get("mem"),

            # MODELO SANTANDER SCORE GO
            "SGO_ANTIGUEDAD_LABORAL": get_path(sgo_inputs, ["antiguedad_laboral", "valor"]),
            "SGO_ANTIGUEDAD_LABORAL_PUNTAJE": get_path(sgo_inputs, ["antiguedad_laboral", "puntaje"]),
            "SGO_CATEGORIA_LABORAL": get_path(sgo_inputs, ["categoria_laboral_titular", "valor"]),
            "SGO_CATEGORIA_LABORAL_PUNTAJE": get_path(sgo_inputs, ["categoria_laboral_titular", "puntaje"]),
            "SGO_INGRESO_CONYUGE": get_path(sgo_inputs, ["income_predictor_conyuge", "valor"]),
            "SGO_INGRESO_CONYUGE_PUNTAJE": get_path(sgo_inputs, ["income_predictor_conyuge", "puntaje"]),
            "SGO_INGRESO_TITULAR": get_path(sgo_inputs, ["income_predictor_titular", "valor"]),
            "SGO_INGRESO_TITULAR_PUNTAJE": get_path(sgo_inputs, ["income_predictor_titular", "puntaje"]),
            "SGO_DOCUMENTOS_MOROSOS": get_path(sgo_inputs, ["nro_documentos_morosos", "valor"]),
            "SGO_NRO_VECES_INGRESO": get_path(sgo_inputs, ["nro_veces_ingreso", "valor"]),
            "SGO_CUOTA_INICIAL": get_path(sgo_inputs, ["porcentaje_cuota_inicial", "valor"]),
            "SGO_PROMEDIO_SALDO_12M": get_path(sgo_inputs, ["promedio_saldo_normal_12m", "valor"]),
            "SGO_SCORE": sgo_outputs.get("score"),

            # SEGMENTACION F
            "SEG_F_ESTUDIOS": seg_f_inputs.get("estudios"),
            "SEG_F_PROPIEDADES": seg_f_inputs.get("propiedades"),
            "SEG_F_SEGMENTO_D": seg_f_inputs.get("segmento_d"),
            "SEG_F_SEGMENTO_S": seg_f_inputs.get("segmento_s"),
            "SEG_F_RESULTADO": seg_f_outputs.get("segmento_f"),

            # SEGMENTACION GR
            "SEG_GR_CUOTA_INICIAL": seg_gr_inputs.get("cuota_inicial"),
            "SEG_GR_ESTADO_CIVIL": seg_gr_inputs.get("estado_civil"),
            "SEG_GR_ESTUDIOS": seg_gr_inputs.get("estudios"),
            "SEG_GR_DEMOGRAFICO": seg_gr_inputs.get("grupo_demografico"),
            "SEG_GR_HISTORIAL": seg_gr_inputs.get("historial_crediticio"),
            "SEG_GR_PROPIEDADES": seg_gr_inputs.get("propiedades"),
            "SEG_GR_RANGO_EDAD": seg_gr_inputs.get("rango_edad"),
            "SEG_GR_SEGMENTO_V3": seg_gr_inputs.get("segmento_v3"),
            "SEG_GR_TOTAL_FINANCIAMIENTO": seg_gr_inputs.get("total_financiamiento"),
            "SEG_GR_SEGMENTO_G": seg_gr_outputs.get("segmento_g"),
            "SEG_GR_SEGMENTO_R": seg_gr_outputs.get("segmento_r"),

            # SEGMENTACION GR VINTAGE
            "VINTAGE_ESTUDIOS": vintage_inputs.get("estudios"),
            "VINTAGE_INGRESO": vintage_inputs.get("ingreso_anualizado_titular"),
            "VINTAGE_DEUDA_CONSUMO": vintage_inputs.get("saldo_deuda_externa_consumo_titular"),
            "VINTAGE_TOTAL_FINANCIAMIENTO": vintage_inputs.get("total_financiamiento"),

            # OFERTA COMERCIAL
            "BONO_COMERCIAL": oferta.get("bono_comercial"),
            "OFERTA_GPS": oferta.get("gps"),
            "OFERTA_SEGURO": oferta.get("seguro_vehicular"),
            "VINCULACION": oferta.get("vinculacion"),

            # RISK_DATA COMO COLUMNAS
            **rd
        }

    # EJECUTAR EXTRACCION
    t_extraccion = time.time()
    extraidos = df_nuevos_cosmos.apply(extraer_cosmos, axis=1, result_type="expand")
    df_nuevos_cosmos = pd.concat([df_nuevos_cosmos, extraidos], axis=1)
    print(f"✅ Extracción terminada: {len(df_nuevos_cosmos):,} registros")
    print(f"Tiempo extracción: {time.time() - t_extraccion:.2f}s")
    df_nuevos_cosmos.drop(columns=["RESPUESTA_COSMOS"], errors="ignore", inplace=True)
#   #%%
    #  Eliminar duplicados
    df_nuevos_cosmos.drop_duplicates(subset=['ID_TRANSACCION'], keep='first', inplace=True)
    df_nuevos_cosmos.reset_index(drop=True, inplace=True)
    # === Cargar a SQL Server ===
    cursor = conn1.cursor()
    cursor.fast_executemany = True
    columnas = df_nuevos_cosmos.columns.tolist()
    df_nuevos_cosmos = df_nuevos_cosmos.where(pd.notnull(df_nuevos_cosmos), None)
    for col in columnas:
        df_nuevos_cosmos[col] = df_nuevos_cosmos[col].astype(str)  
    cols_sql =  ",".join([f"[{col}]" for col in columnas])
    placeholders = ",".join(["?"] * len(columnas))
    insert_sql = f"""
    INSERT INTO GOCREDITOS_BRONZE.stga_scp.MAESTRA_LOG_API_COSMOS ({cols_sql})
    VALUES ({placeholders})
    """
    batch_size = 100
    total = len(df_nuevos_cosmos)

    for inicio in range(0, total, batch_size):
        fin = min(inicio + batch_size, total)
        batch = df_nuevos_cosmos.iloc[inicio:fin].values.tolist()
        cursor.executemany(insert_sql, batch)
        conn1.commit()
        print(f"[{fin:,}/{total:,}] {fin/total:.1%} | Batch: {len(batch):,}")

    print(f"Insertado {len(df_nuevos_cosmos)} registros")
    detalle = f"Se procesó la carga hacia Bronze correctamente."
    print(detalle)
    cursor.close()
    conn1.close()
    print("🔌 Conexión cerrada.")
    # %%