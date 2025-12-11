# etl_byd.py
from dotenv import load_dotenv
import os
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

# Fecha mínima (desde cuándo traer info)
FECHA_INICIO = os.getenv("FECHA_INICIO", "2025-12-01T00:00:00")

# ========= CONFIG BYD =========
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

# ========= EXTRACCIÓN =========
def construir_url(skip: int = 0, top: int = 10000) -> str:
    # Solo desde FECHA_INICIO (sin fecha fin)
    filtro = (
        f"(CPOSTDATE ge datetime'{FECHA_INICIO}') and "
        f"(CDSR_PROC_CATID ne 'CA_2')"
    )

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

    print(f"Extrayendo rango desde: {FECHA_INICIO} (sin fecha fin)...")

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

    for col in ["VENTAS_US", "COSTO_US", "Cantidad_FacUS"]:
        df_ventas_1[col] = pd.to_numeric(df_ventas_1[col], errors="coerce")

    # Etiqueta de periodo
    df_ventas_1['periodo_data'] = 'Dic 1 en adelante'

    # Cambiar signos
    print("🔄 Cambiando signo VENTAS_US y COSTO_US a negativos...")
    df_ventas_1['VENTAS_US'] = df_ventas_1['VENTAS_US'] * -1
    df_ventas_1['COSTO_US']   = df_ventas_1['COSTO_US'] * -1

    print(f"\nTotal de registros obtenidos: {len(df_ventas_1)}")
    return df_ventas_1

# ========= CARGA A POSTGRES =========
def cargar_a_postgres(df_ventas_1: pd.DataFrame) -> None:
    connection_string = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=300)

    # Ajuste de longitudes
    df_ventas_1['Ship_To']            = df_ventas_1['Ship_To'].astype(str).str[:60]
    df_ventas_1['Customer_Name']      = df_ventas_1['Customer_Name'].astype(str).str[:50]
    df_ventas_1['Person_Responsible'] = df_ventas_1['Person_Responsible'].astype(str).str[:50]
    df_ventas_1['City']               = df_ventas_1['City'].astype(str).str[:100]
    df_ventas_1['Product']            = df_ventas_1['Product'].astype(str).str[:100]

    dtype_map = {
        "Invoice_Date":       String(25),   # si luego lo cambias a timestamp, ajusta aquí
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

    # Verificar existencia de tabla
    with engine.connect() as conn:
        table_exists = conn.dialect.has_table(conn, "sap_byd_ventas")

    with engine.begin() as conn:
        if table_exists:
            # Borrado lógico desde FECHA_INICIO
            print(f"🧹 Eliminando registros desde {FECHA_INICIO} en sap_byd_ventas...")
            delete_sql = text("""
                DELETE FROM sap_byd_ventas
                WHERE "Invoice_Date" >= :fecha_inicio
            """)
            result = conn.execute(delete_sql, {"fecha_inicio": FECHA_INICIO})
            print(f"   Registros borrados: {result.rowcount}")

            if_exists_mode = 'append'
        else:
            print("🆕 Primera carga: se crea la tabla")
            if_exists_mode = 'fail'

        # Carga
        df_ventas_1.to_sql(
            name='sap_byd_ventas',
            con=conn,
            if_exists=if_exists_mode,
            index=False,
            dtype=dtype_map,
            method='multi',
            chunksize=1000
        )

    print(f"✅ Cargados {len(df_ventas_1)} registros de este rango de fechas")
    engine.dispose()
    print("🔌 Conexión cerrada")

# ========= MAIN =========
if __name__ == "__main__":
    df = extraer_ventas()
    if not df.empty:
        cargar_a_postgres(df)
    else:
        print("⚠️ No se encontraron datos para cargar.")