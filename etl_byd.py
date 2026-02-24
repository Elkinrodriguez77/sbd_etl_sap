# etl_byd.py
from dotenv import load_dotenv
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
from lxml import etree
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Float

# ========= CARGA VARIABLES ENTORNO =========
load_dotenv()

BJD_USER = os.getenv("BJD_USER")
BJD_PASS = os.getenv("BJD_PASS")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_DB   = os.getenv("PG_DB")

# Modo automático: calcula mes actual + mes anterior. Si false, usa FECHA_INICIO y FISCAL_PERIODS_TO_RELOAD
MODO_AUTO = os.getenv("MODO_AUTO", "true").lower() in ("true", "1", "yes")

# Fecha mínima (desde cuándo traer info de BYD) - solo si MODO_AUTO=false
FECHA_INICIO = os.getenv("FECHA_INICIO", "2026-01-01T00:00:00")

# Periodos fiscales que se van a limpiar y recargar - solo si MODO_AUTO=false
# Ejemplo en .env: FISCAL_PERIODS_TO_RELOAD=12.2025,01.2026,02.2026
FISCAL_PERIODS_TO_RELOAD = os.getenv("FISCAL_PERIODS_TO_RELOAD", "")
FISCAL_PERIODS_LIST = [
    p.strip() for p in FISCAL_PERIODS_TO_RELOAD.split(",") if p.strip()
]


def _calcular_ventana_auto() -> tuple[str, str, list[str]]:
    """
    Calcula FECHA_INICIO, FECHA_FIN y periodos fiscales para mes actual + mes anterior.
    El histórico anterior queda intacto.
    """
    hoy = datetime.now()
    mes_actual = hoy.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    mes_anterior = mes_actual - relativedelta(months=1)

    fecha_inicio = mes_anterior.strftime("%Y-%m-%dT00:00:00")
    # Formato MM.YYYY para filtro fiscal (igual que FISCAL_PERIODS_TO_RELOAD)
    fiscal_fin = mes_actual.strftime("%m.%Y")

    # Formato MM.YYYY para FiscalMonthYear (delete/insert en PG)
    periodos_mm_yyyy = [
        mes_anterior.strftime("%m.%Y"),
        mes_actual.strftime("%m.%Y"),
    ]
    # SAP puede devolver YYYY-MM; incluir ambos para DELETE seguro
    periodos_yyyy_mm = [
        mes_anterior.strftime("%Y-%m"),
        mes_actual.strftime("%Y-%m"),
    ]
    return fecha_inicio, fiscal_fin, periodos_mm_yyyy, periodos_yyyy_mm


if MODO_AUTO:
    FECHA_INICIO, FISCAL_FIN, FISCAL_PERIODS_LIST, FISCAL_PERIODS_ALT = _calcular_ventana_auto()
    FECHA_FIN = ""  # no usamos datetime lt (causa 400 en SAP)
    print(f"📅 Modo auto: mes actual + anterior → {FISCAL_PERIODS_LIST}")
else:
    FECHA_FIN = ""
    FISCAL_FIN = ""
    # En modo manual, generar formato alternativo para DELETE (MM.YYYY -> YYYY-MM)
    FISCAL_PERIODS_ALT = []
    for p in FISCAL_PERIODS_LIST:
        parts = p.split(".")
        if len(parts) == 2:
            FISCAL_PERIODS_ALT.append(f"{parts[1]}-{parts[0]}")  # YYYY-MM

# ========= CONFIG BYD VENTAS =========
base_url = (
    "https://my336154.sapbydesign.com/sap/byd/odata/"
    "cc_home_analytics.svc/"
    "RPZE627541F6012E1EBC362E8QueryResults"
)

SELECT_FIELDS = (
    "C1CINHUUIDsDOC_INV_DATE,"
    "C1CUSTOMERsCITY_NAME,"
    "CACCPERIOD,"
    "CCINHUUID,"
    "CCUSTOMER,"
    "CFISCALDDATES6F44DC8D81C7C41F,"
    "CPRODUCT,"
    "CPROFITCTR,"
    "CSALESUNIT,"
    "CZCE03SBUDES,"
    "KCZ38704318CAF9C0490E065D,"   # ventas
    "KCZ206B9BDD38BC08F314528E,"   # costo
    "KCZ80E56A9357921903E24583,"   # cantidad
    "TCUSTOMER,"
    "T1CUSTOMERsREGION_CODE,"
    "TCOUNTRY_CODE,"
    "T1CINIUUIDsIP_PR_RC_UUID,"
    "TRESPEMP"
)

ns = {
    "atom": "http://www.w3.org/2005/Atom",
    "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
    "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
}

# ========= FUNCIONES GENERALES =========
def get_engine():
    conn_str = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=300)

# ========= EXTRACCIÓN VENTAS (igual que antes) =========
def construir_url(skip: int = 0, top: int = 10000) -> str:
    filtro = (
        f"(CPOSTDATE ge datetime'{FECHA_INICIO}') and "
        f"(CDSR_PROC_CATID ne 'CA_2')"
    )
    # SAP ByDesign da 400 con datetime lt; usamos campo fiscal (Date) en su lugar
    if FISCAL_FIN:
        filtro += f" and (CFISCALDDATES6F44DC8D81C7C41F le '{FISCAL_FIN}')"

    url = (
        f"{base_url}"
        f"?$select={SELECT_FIELDS}"
        f"&$filter={filtro}"
        f"&$top={top}"
    )
    if skip > 0:
        url += f"&$skip={skip}"
    return url

def extraer_batch(url: str) -> pd.DataFrame:
    resp = requests.get(url, auth=(BJD_USER, BJD_PASS), timeout=300)
    resp.raise_for_status()
    root = etree.fromstring(resp.content)

    rows = []
    for entry in root.findall("atom:entry", ns):
        props = entry.find("atom:content/m:properties", ns)
        if props is None:
            continue
        row = {
            "Invoice_Date":       props.findtext('d:C1CINHUUIDsDOC_INV_DATE', default='', namespaces=ns),
            "Customer":           props.findtext('d:CCUSTOMER',                 default='', namespaces=ns),
            "City":               props.findtext('d:C1CUSTOMERsCITY_NAME',       default='', namespaces=ns),
            "Accounting_Period":  props.findtext('d:CACCPERIOD',                 default='', namespaces=ns),
            "Invoice":            props.findtext('d:CCINHUUID',                  default='', namespaces=ns),
            "FiscalMonthYear":    props.findtext('d:CFISCALDDATES6F44DC8D81C7C41F', default='', namespaces=ns),
            "Product":            props.findtext('d:CPRODUCT',                   default='', namespaces=ns),
            "Profit_Center":      props.findtext('d:CPROFITCTR',                 default='', namespaces=ns),
            "Sales_Unit":         props.findtext('d:CSALESUNIT',                 default='', namespaces=ns),
            "E03_SBU_Name":       props.findtext('d:CZCE03SBUDES',               default='', namespaces=ns),
            "VENTAS_US":          props.findtext('d:KCZ38704318CAF9C0490E065D',  default='0', namespaces=ns),
            "COSTO_US":           props.findtext('d:KCZ206B9BDD38BC08F314528E',  default='0', namespaces=ns),
            "Cantidad_FacUS":     props.findtext('d:KCZ80E56A9357921903E24583',  default='0', namespaces=ns),
            "Customer_Name":      props.findtext('d:TCUSTOMER',                  default='', namespaces=ns),
            "State":              props.findtext('d:T1CUSTOMERsREGION_CODE',     default='', namespaces=ns),
            "Country_Region":     props.findtext('d:TCOUNTRY_CODE',              default='', namespaces=ns),
            "Ship_To":            props.findtext('d:T1CINIUUIDsIP_PR_RC_UUID',   default='', namespaces=ns),
            "Person_Responsible": props.findtext('d:TRESPEMP',                   default='', namespaces=ns),
        }
        rows.append(row)

    return pd.DataFrame(rows)

def extraer_ventas() -> pd.DataFrame:
    all_batches = []
    skip = 0
    batch_size = 10000

    rango = f"{FECHA_INICIO} hasta FiscalMonthYear<={FISCAL_FIN}" if FISCAL_FIN else f"{FECHA_INICIO} (sin límite fiscal)"
    print(f"Extrayendo rango: {rango}")

    while True:
        print(f"Página {skip//batch_size + 1} (skip={skip})...")
        url = construir_url(skip=skip, top=batch_size)
        df_batch = extraer_batch(url)

        if df_batch.empty:
            print("  Sin datos (fin).")
            break

        print(f"  {len(df_batch)} filas en este batch.")
        all_batches.append(df_batch)

        if len(df_batch) < batch_size:
            print("  Último batch de este rango.")
            break

        skip += batch_size

    df_ventas_1 = pd.concat(all_batches, ignore_index=True) if all_batches else pd.DataFrame()

    if not df_ventas_1.empty:
        sample_fiscal = df_ventas_1["FiscalMonthYear"].dropna().unique()[:5].tolist()
        print(f"   Periodos en respuesta API (muestra): {sample_fiscal}")

    # Filtrar solo periodos objetivo (SAP puede devolver formatos distintos)
    if not df_ventas_1.empty and (FISCAL_PERIODS_LIST or FISCAL_PERIODS_ALT):
        periodos_validos = set(FISCAL_PERIODS_LIST + FISCAL_PERIODS_ALT)
        antes = len(df_ventas_1)
        df_ventas_1 = df_ventas_1[df_ventas_1["FiscalMonthYear"].astype(str).str.strip().isin(periodos_validos)]
        if len(df_ventas_1) < antes:
            print(f"   Filtro periodo: {antes} → {len(df_ventas_1)} filas (solo {list(periodos_validos)})")

    if df_ventas_1.empty:
        print("⚠️ No hay datos en este rango. Retornando DataFrame vacío.")
        return df_ventas_1
    # ==========================

    for col in ["VENTAS_US", "COSTO_US", "Cantidad_FacUS"]:
        df_ventas_1[col] = pd.to_numeric(df_ventas_1[col], errors="coerce")

    df_ventas_1['periodo_data'] = 'Dic 2025 en adelante'

    print("🔄 Cambiando signo VENTAS_US y COSTO_US a negativos...")
    df_ventas_1['VENTAS_US'] = df_ventas_1['VENTAS_US'] * -1
    df_ventas_1['COSTO_US']   = df_ventas_1['COSTO_US'] * -1

    print(f"\nTotal de registros obtenidos: {len(df_ventas_1)}")
    return df_ventas_1

def cargar_a_postgres(df_ventas_1: pd.DataFrame) -> None:
    engine = get_engine()

    df_ventas_1['Ship_To']            = df_ventas_1['Ship_To'].astype(str).str[:60]
    df_ventas_1['Customer_Name']      = df_ventas_1['Customer_Name'].astype(str).str[:50]
    df_ventas_1['Person_Responsible'] = df_ventas_1['Person_Responsible'].astype(str).str[:50]
    df_ventas_1['City']               = df_ventas_1['City'].astype(str).str[:100]
    df_ventas_1['Product']            = df_ventas_1['Product'].astype(str).str[:100]

    dtype_map = {
        "Invoice_Date":       String(25),
        "Customer":           String(10),
        "City":               String(100),
        "Accounting_Period":  String(10),
        "Invoice":            String(15),
        "FiscalMonthYear":    String(10),
        "Product":            String(100),
        "Profit_Center":      String(15),
        "Sales_Unit":         String(20),
        "E03_SBU_Name":       String(20),
        "VENTAS_US":          Float,
        "COSTO_US":           Float,
        "Cantidad_FacUS":     Float,
        "Customer_Name":      String(50),
        "State":              String(20),
        "Country_Region":     String(10),
        "Ship_To":            String(60),
        "Person_Responsible": String(50),
        "periodo_data":       String(50)
    }

    with engine.connect() as conn:
        table_exists = conn.dialect.has_table(conn, "sap_byd_ventas")

    with engine.begin() as conn:
        if table_exists and (FISCAL_PERIODS_LIST or FISCAL_PERIODS_ALT):
            # SAP puede guardar MM.YYYY o YYYY-MM; borrar ambos formatos
            periodos_borrar = list(dict.fromkeys(FISCAL_PERIODS_LIST + FISCAL_PERIODS_ALT))
            print("🧹 Eliminando registros por FiscalMonthYear en sap_byd_ventas...")
            print(f"   Periodos a borrar (formatos MM.YYYY y YYYY-MM): {periodos_borrar}")

            placeholders = ", ".join([f":p{i}" for i in range(len(periodos_borrar))])
            delete_sql = text(f"""
                DELETE FROM sap_byd_ventas
                WHERE "FiscalMonthYear" IN ({placeholders})
            """)
            params = {f"p{i}": p for i, p in enumerate(periodos_borrar)}
            result = conn.execute(delete_sql, params)
            print(f"   Registros borrados: {result.rowcount}")
        elif table_exists:
            print("⚠️ La tabla existe pero no se definieron periodos en FISCAL_PERIODS_TO_RELOAD; no se borra nada.")
        else:
            print("🆕 Primera carga: se creará la tabla")

        if_exists_mode = 'append' if table_exists else 'fail'

        df_ventas_1.to_sql(
            name='sap_byd_ventas',
            con=conn,
            if_exists=if_exists_mode,
            index=False,
            dtype=dtype_map,
            method='multi',
            chunksize=1000
        )

    print(f"✅ Cargados {len(df_ventas_1)} registros de ventas")
    engine.dispose()
    print("🔌 Conexión cerrada (ventas)")

# ========= ODATA ORDENES =========
ODATA_ORDENES_URL = (
    "https://my336154.sapbydesign.com/sap/byd/odata/"
    "cc_home_analytics.svc/"
    "RPZA64281B20A8D0329C26607QueryResults"
    "?$select=CBP_INT_ID,TBP_INT_ID,CYPCJYMI4Y_ZBRAND,"
    "CIPY_BUY_CTYNM_N,CIPY_PRD_REC_ADR_CITY,TDBA_DISTRCHN_CD,"
    "CZCE03SBUDES,CIPY_EMP_RSP_PTY,TIPY_EMP_RSP_PTY,"
    "CFISCALDDATES6F44DC8D81C7C41F,CIPR_PRODUCT,TIPR_PRODUCT,"
    "CDOC_ID,CITM_UUID,TITM_UUID,CIPY_PRD_REC_PTY,TIPY_PRD_REC_PTY,"
    "TIPY_BUY_REGCD_N,KCIAV_INV_AMT_RC,KCZE2B935894219245A1E8E77,"
    "KCZB8AFD00C36F845A6715442,KCZA9D6BCB37DDC4CFD5A793B,"
    "KCZF90BF9555FB749DF9AC0DD,KCZB6013F9BB1AA840E860741,"
    "KCZ99DCD133A13F9D1408CD69,KCZF14889264B14B86AC3BFDE,"
    "KCZ5F74283906DBF4CD07A5CA,KCZFA7C12055DC98403D10CAA,"
    "KCZC4CE47BAD42C81EA5B0D0F,KCZ998098F004AB32E2511CF5,"
    "KCZ95857413FCAF0B77113DCF"
    "&$filter=(CFISCALDDATES6F44DC8D81C7C41F ge '2025-01') "
    "and (CDOC_CREATED_DT ge datetime'2025-01-01T00:00:00')"
    "&$top=15000"
)

def extraer_ordenes() -> pd.DataFrame:
    print("Extrayendo OData de órdenes...")
    resp = requests.get(ODATA_ORDENES_URL, auth=(BJD_USER, BJD_PASS), timeout=300)
    resp.raise_for_status()

    df = pd.read_xml(
        resp.content,
        xpath=".//atom:entry/atom:content/m:properties",
        namespaces=ns
    )

    rename_map = {
        "CBP_INT_ID": "Customer",
        "TBP_INT_ID": "Account Name",
        "CYPCJYMI4Y_ZBRAND": "Brand",
        "CDOC_ID": "Sales Order",
        "TITM_UUID": "Sales Order Item",
        "TIPY_PRD_REC_PTY": "Ship-To",
        "CIPY_PRD_REC_ADR_CITY": "City of Ship-to",
        "CFISCALDDATES6F44DC8D81C7C41F": "FiscalMonthYear",
        "KCZ5F74283906DBF4CD07A5CA": "Valor Confirinv",
        "KCZFA7C12055DC98403D10CAA": "Valor OrdAbiertas (Con Inv)",
        "KCZ95857413FCAF0B77113DCF": "Valor Solicitado",
        "KCZ99DCD133A13F9D1408CD69": "Qty Solicitada",
        "KCZA9D6BCB37DDC4CFD5A793B": "Qty Confirinv",
        "KCZF90BF9555FB749DF9AC0DD": "Qty En Preparacion",
        "KCZE2B935894219245A1E8E77": "Qty BO",
        "KCIAV_INV_AMT_RC": "Invoiced Amount",
        "KCZF14889264B14B86AC3BFDE": "Valor BO",
        "TIPY_BUY_REGCD_N": "State",
        "CIPY_BUY_CTYNM_N": "City of Ship-to1",
        "CIPR_PRODUCT": "Product",
        "CIPY_PRD_REC_PTY": "Ship-To ID",
        "KCZC4CE47BAD42C81EA5B0D0F": "Valor Preparacion",
        "KCZB8AFD00C36F845A6715442": "Valor Confirmado",
        "KCZ998098F004AB32E2511CF5": "Qty Confirmada",
        "KCZB6013F9BB1AA840E860741": "Qty Facturada",
    }
    df = df.rename(columns=rename_map)

    # Si en el futuro quieres dejar solo el subset de columnas, se puede hacer aquí.
    print(f"Órdenes: {len(df)} filas extraídas")
    return df

def cargar_ordenes(df: pd.DataFrame) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name="sap_byd_ordenes",
            con=conn,
            if_exists="replace",   # siempre reemplaza
            index=False,
            method="multi",
            chunksize=1000
        )
    engine.dispose()
    print(f"✅ Órdenes cargadas en sap_byd_ordenes ({len(df)} filas)")

# ========= ODATA COSTO PRODUCTO =========
ODATA_COSTO_URL = (
    "https://my336154.sapbydesign.com/sap/byd/odata/"
    "cc_home_analytics.svc/"
    "RPZ2A3214DFBC04E0DEE943B3QueryResults"
    "?$select=CMATERIAL,TMATERIAL,CPERMEST,TPERMEST,CSETOFBKS,FCVALPCOMP"
    "&$filter=CPERMEST eq '250' and CSETOFBKS eq 'ZC01'"
    "&$top=18000"
)

def extraer_costo_producto() -> pd.DataFrame:
    print("Extrayendo OData de costo_producto...")
    resp = requests.get(ODATA_COSTO_URL, auth=(BJD_USER, BJD_PASS), timeout=300)
    resp.raise_for_status()

    df = pd.read_xml(
        resp.content,
        xpath=".//atom:entry/atom:content/m:properties",
        namespaces=ns
    )

    # Reordenar / seleccionar columnas como en M
    cols = ["CMATERIAL", "TMATERIAL", "CPERMEST", "TPERMEST", "FCVALPCOMP"]
    df = df[cols]

    rename_map = {
        "CMATERIAL": "Material"
        # el resto de columnas se dejan con el mismo nombre
    }
    df = df.rename(columns=rename_map)

    print(f"Costo producto: {len(df)} filas extraídas")
    return df

def cargar_costo_producto(df: pd.DataFrame) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name="sap_byd_costo_producto",
            con=conn,
            if_exists="replace",   # siempre reemplaza
            index=False,
            method="multi",
            chunksize=1000
        )
    engine.dispose()
    print(f"✅ Costo producto cargado en sap_byd_costo_producto ({len(df)} filas)")


## nuevos requerimientos para data Jaime ======

# ========= ODATA 3PL Y ENTREGA DE MERCANCÍA =========

URL_BASE_3PL = (
    "https://my336154.sapbydesign.com/sap/byd/odata/"
    "cc_home_analytics.svc/"
    "RPZ8FD31E1E09C6489CFC1FE8QueryResults"
)
SELECT_3PL = "CRELEASE_STATUS,TRELEASE_STATUS,CBUSINEERENCEF1ACB9534604A4D9,CRELEASERENCE79EA4F7FDF174CDF,CSTATUSERENCE8E2BEDA58262A7C0,TSTATUSERENCE8E2BEDA58262A7C0,CIDCONTERENCEFD3F50267033877F,CPRODUCERENCE961D56D7A61936A0,KCREQUESERENCEE7C71585BF4BFCEE,CBUSINEERENCEBC7B6311A522DAAC"
FILTER_3PL = "(CBUSINEERENCEBC7B6311A522DAAC eq '114') and (CRELEASERENCE79EA4F7FDF174CDF ge datetime'2025-12-01T00:00:00')"


URL_BASE_ENTREGA = (
    "https://my336154.sapbydesign.com/sap/byd/odata/"
    "cc_home_analytics.svc/"
    "RPZ4E72B90D164D5C8BA4A7E9QueryResults"
)
SELECT_ENTREGA = "CID_TRANSPORTADORA_01,CID_UBICACION_01,CID_VERIFICACION_01,CID_VEHICULO_01,CID_FECHAPRIMERACITA,CID_CAJAS_01,CID_CITAS_ADICIONAL_01,CID_CITAS_01,CID_CONDUCTOR_01,CID_CSAP_01,CID_CUMPLIMIENTO_01,CID_DIAS_ENTREGA_01,CID_DIAS_SBD_01,CID_ENTREGA_01,CID_ESTADO_01,CID_FE_01,CID_FECHA_ENTREGA_01,CID_FECHATRANSDESTINO,CID_FGUIA_01,CID_GUIA_01,CID_HORA_ENTREGA_01,CID_HUACALES_01,CID_INDICADOR_01,CID_MENTREGA_01,CID_MOTIVOATRASO,CID_NOMBRE_ENTREGA_01,CID_NOMBRE_RECIBE_01,CID_NOVEDAD_01,CIBR_SLO_UUID,CID_PLACAS_01,CID_PROM_SERVICIO_01,CID_RCSAP_01,CDOC_INV_DATE"
FILTER_ENTREGA = "(CDOC_INV_DATE ge datetime'2025-12-01T00:00:00')"


def extraer_odata_paginado(nombre_proceso: str, url_base: str, select: str, filter_str: str, batch_size: int = 5000) -> pd.DataFrame:
    """
    Función genérica para extraer todos los registros de SAP ByD paginando de a batch_size.
    """
    print(f"\nExtrayendo OData paginado: {nombre_proceso}...")
    all_batches = []
    skip = 0

    while True:
        
        url = f"{url_base}?$select={select}&$filter={filter_str}&$top={batch_size}&$skip={skip}"
        print(f"  -> Obteniendo batch (skip={skip})...")
        
        resp = requests.get(url, auth=(BJD_USER, BJD_PASS), timeout=300)
        resp.raise_for_status()

        try:
        
            df_batch = pd.read_xml(
                resp.content,
                xpath=".//atom:entry/atom:content/m:properties",
                namespaces=ns
            )
        except ValueError:
            print("  Sin más datos (fin de la lectura XML).")
            break

        if df_batch.empty:
            break

        all_batches.append(df_batch)
        print(f"     {len(df_batch)} filas en este batch.")

      
        if len(df_batch) < batch_size:
            break
            
        skip += batch_size

    if all_batches:
        df_final = pd.concat(all_batches, ignore_index=True)
        print(f"✅ Total extraído para {nombre_proceso}: {len(df_final)} filas")
        return df_final
    else:
        print(f"⚠️ No se encontraron datos para {nombre_proceso}.")
        return pd.DataFrame()

def cargar_tabla_simple(df: pd.DataFrame, nombre_tabla: str) -> None:
    """
    Carga un DataFrame directamente a una tabla nueva en PostgreSQL (Reemplaza la existente).
    """
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name=nombre_tabla,
            con=conn,
            if_exists="replace",  # Crea la tabla desde cero cada vez
            index=False,
            method="multi",
            chunksize=1000
        )
    engine.dispose()
    print(f"✅ Datos cargados en PostgreSQL -> Tabla: {nombre_tabla} ({len(df)} filas)")


# ========= MAIN =========
if __name__ == "__main__":
    # 1) Ventas (como estaba)
    df_ventas = extraer_ventas()
    if not df_ventas.empty:
        cargar_a_postgres(df_ventas)
    else:
        print("⚠️ No se encontraron datos de ventas para cargar.")

    # 2) Órdenes
    df_ordenes = extraer_ordenes()
    if not df_ordenes.empty:
        cargar_ordenes(df_ordenes)
    else:
        print("⚠️ OData de órdenes sin datos.")

    # 3) Costo producto
    df_costo = extraer_costo_producto()
    if not df_costo.empty:
        cargar_costo_producto(df_costo)
    else:
        print("⚠️ OData de costo producto sin datos.")

    # ================= NUEVO: 3PL y Entrega de Mercancía =================
    
    # 4) 3PL
    df_3pl = extraer_odata_paginado("3PL", URL_BASE_3PL, SELECT_3PL, FILTER_3PL)
    if not df_3pl.empty:
        cargar_tabla_simple(df_3pl, "sap_byd_3pl")
    else:
        print("⚠️ OData de 3PL sin datos.")

    # 5) Entrega de Mercancía
    df_entrega = extraer_odata_paginado("Entrega de Mercancía", URL_BASE_ENTREGA, SELECT_ENTREGA, FILTER_ENTREGA)
    if not df_entrega.empty:
        cargar_tabla_simple(df_entrega, "sap_byd_entrega_mercancia")
    else:
        print("⚠️ OData de Entrega de Mercancía sin datos.")