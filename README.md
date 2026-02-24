# ETL BYD — Carga Incremental

ETL que extrae datos de **SAP Business ByDesign** (OData) y los carga en **PostgreSQL** para análisis y reportes. Diseñado para cargas incrementales que preservan el histórico y evitan bloqueos en la API de origen.

---

## Índice

- [Visión general](#visión-general)
- [Fuentes de datos](#fuentes-de-datos)
- [Lógica de negocio](#lógica-de-negocio)
- [Modo de operación](#modo-de-operación)
- [Configuración](#configuración)
- [Ejecución](#ejecución)

---

## Visión general

El proceso realiza tres cargas independientes en secuencia:

| # | Flujo        | Tabla destino         | Estrategia de carga |
|---|--------------|------------------------|---------------------|
| 1 | **Ventas**   | `sap_byd_ventas`       | Incremental por periodo fiscal |
| 2 | **Órdenes**  | `sap_byd_ordenes`      | Reemplazo completo |
| 3 | **Costo producto** | `sap_byd_costo_producto` | Reemplazo completo |

---

## Fuentes de datos

Todas las fuentes son servicios **OData** expuestos por SAP Business ByDesign:

1. **Ventas facturadas** — Consulta analítica de facturación con filtros por fecha de documento (`CPOSTDATE`).
2. **Órdenes de venta** — Consulta de órdenes con estado, cantidades y valores por periodo fiscal.
3. **Costo de producto** — Maestro de materiales con costos estándar (centro de costos y set de libros fijos).

---

## Lógica de negocio

### 1. Ventas (`sap_byd_ventas`)

**Objetivo:** Mantener un histórico de ventas facturadas actualizado, recargando solo los periodos recientes para evitar consultas pesadas que bloquean la API.

**Campos principales extraídos:**
- Identificadores: factura, cliente, producto, centro de beneficio
- Geografía: ciudad, estado, país
- Métricas: ventas (USD), costo (USD), cantidad facturada
- Periodo fiscal (`FiscalMonthYear`) para la lógica incremental

**Transformaciones:**
- Exclusión de documentos con categoría de proceso `CA_2`
- Inversión de signo en ventas y costos (convención contable)
- Truncado de textos para cumplir límites de columnas en PostgreSQL

**Estrategia incremental:**
- Se eliminan los registros de los periodos fiscales a recargar (soporta formatos `MM.YYYY` y `YYYY-MM` por compatibilidad con SAP)
- Se insertan los datos nuevos del mismo rango
- El resto del histórico permanece intacto

**Nota técnica:** SAP ByDesign devuelve 400 con filtros `datetime lt` en OData. Por eso se usa el campo fiscal `CFISCALDDATES` (formato `MM.YYYY`) para limitar el rango en lugar de fecha fin.

---

### 2. Órdenes (`sap_byd_ordenes`)

**Objetivo:** Snapshot actual de órdenes de venta con estados y valores.

**Campos principales:**
- Cliente, marca, orden, ítem
- Ship-to (ciudad, estado)
- Valores: solicitado, confirmado, facturado, backorder
- Cantidades: solicitada, confirmada, facturada, en preparación, BO

**Estrategia:** Reemplazo completo de la tabla en cada ejecución.

---

### 3. Costo de producto (`sap_byd_costo_producto`)

**Objetivo:** Maestro de costos estándar por material.

**Campos principales:**
- Material (código y descripción)
- Centro de costos y set de libros
- Costo de valoración (`FCVALPCOMP`)

**Filtros fijos:** Centro de costos `250`, set de libros `ZC01`.

**Estrategia:** Reemplazo completo de la tabla en cada ejecución.

---

## Modo de operación

### Modo automático (recomendado)

Con `MODO_AUTO=true` (por defecto):

1. Se calcula la **ventana de carga**: mes actual + mes anterior.
2. Se extraen solo los datos de esos 2 meses desde la API.
3. Se borran y recargan únicamente esos periodos fiscales en `sap_byd_ventas`.
4. El histórico anterior no se modifica.

**Ejemplo:** Si hoy es marzo 2026:
- Se consultan: enero y febrero 2026
- Se recargan periodos: `01.2026`, `02.2026`
- Diciembre 2025 y anteriores se mantienen sin cambios

**Ventajas:**
- Menor volumen por consulta → menos riesgo de bloqueos en la API
- Ejecución más rápida
- Histórico preservado
- Sin mantenimiento manual de fechas

### Modo manual

Con `MODO_AUTO=false` se usan las variables de `.env`:

- `FECHA_INICIO` — Fecha mínima para el filtro de facturación (ej: `2026-01-01T00:00:00`)
- `FISCAL_PERIODS_TO_RELOAD` — Periodos fiscales a borrar y recargar (formato `MM.YYYY`, separados por comas, ej: `01.2026,02.2026`)

En modo manual no se aplica filtro de fecha fin en la API; se trae todo desde `FECHA_INICIO` en adelante. Útil para cargas puntuales o recargas históricas específicas.

---

## Configuración

Las variables se definen en `.env` (no se sube a git). Usa `.env.example` como plantilla.

| Variable | Descripción |
|----------|-------------|
| `BJD_USER` | Usuario para SAP ByDesign |
| `BJD_PASS` | Contraseña para SAP ByDesign |
| `PG_HOST` | Host de PostgreSQL |
| `PG_PORT` | Puerto (por defecto 5432) |
| `PG_USER` | Usuario de base de datos |
| `PG_PASS` | Contraseña de base de datos |
| `PG_DB` | Nombre de la base de datos |
| `MODO_AUTO` | `true` (automático, recomendado) o `false` (manual) |
| `FECHA_INICIO` | Solo si `MODO_AUTO=false`. Fecha mínima (ej: `2026-01-01T00:00:00`) |
| `FISCAL_PERIODS_TO_RELOAD` | Solo si `MODO_AUTO=false`. Periodos a recargar (ej: `01.2026,02.2026`) |

---

## Ejecución

### Local

```bash
pip install -r requirements.txt
cp .env.example .env
# Editar .env con credenciales
python etl_byd.py
```

### GitHub Actions

El workflow `.github/workflows/etl_byd.yml` ejecuta el ETL según un cron (varias veces al día) y mediante `workflow_dispatch`. Usa `MODO_AUTO=true` por defecto; las credenciales (BJD_USER, BJD_PASS, PG_*) se configuran como secrets del repositorio. No se requieren secrets para `FECHA_INICIO` ni `FISCAL_PERIODS_TO_RELOAD`.

---

## Requisitos

- Python 3.11+
- Dependencias en `requirements.txt` (pandas, requests, sqlalchemy, python-dotenv, lxml, etc.)
