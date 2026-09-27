# Modelo Predictivo SICOP

Un pipeline de aprendizaje automático para predecir resultados de compras públicas utilizando datos del **Sistema de Información de Compras y Contrataciones Públicas (SICOP)** — el Observatorio de Compra Pública de Costa Rica.

## Descripción General

Este proyecto implementa un pipeline completo de ETL (Extracción, Transformación, Carga) que:
1. **Descarga** datos mensuales de compras públicas desde la API de SICOP
2. **Transforma** archivos CSV sin procesar en una capa de staging estructurada
3. **Cura** datos en un esquema analítico listo para producción
4. **Entrena** un modelo de aprendizaje automático para predecir resultados de compras

Todo el pipeline está impulsado por **DuckDB**, una base de datos SQL incrustada optimizada para cargas de trabajo analíticas.

---

## Fuente de Datos

**SICOP (Sistema de Información de Compras y Contrataciones Públicas)**
- **Proveedor:** Observatorio de Compra Pública de Costa Rica
- **Acceso:** https://dlsaobservatorioprod.blob.core.windows.net/fs-synapse-observatorio-produccion/Zip/{yyyymm}.zip
- **Frecuencia de Actualización:** Diaria (mes vigente) y archivos mensuales históricos
- **Formato de Datos:** Archivos ZIP con CSV separados por punto y coma

### Cobertura de Datos

El pipeline procesa **8 archivos CSV** organizados como dimensiones y hechos de compras públicas:

| Archivo | Tabla | Tipo | Descripción |
|---------|-------|------|-------------|
| InstitucionesRegistradas.csv | `dim_instituciones` | Dimensión | Instituciones públicas participantes en compras |
| Proveedores.csv | `dim_proveedores` | Dimensión | Proveedores/vendedores registrados |
| DetalleCarteles.csv | `fact_carteles` | Hecho | Anuncios de licititation (carteles) |
| DetalleLineaCartel.csv | `fact_lineas_carteles` | Hecho | Líneas individuales por licitación |
| Ofertas.csv | `fact_ofertas` | Hecho | Ofertas/propuestas de proveedores |
| LineasOfertadas.csv | `fact_lineas_ofertas` | Hecho | Líneas en ofertas |
| ProcedimientoAdjudicacion.csv | `fact_adjudicaciones` | Hecho | Decisiones de adjudicación |
| LineasAdjudicadas.csv | `fact_lineas_adjudicadas` | Hecho | Líneas adjudicadas |

---
## Estructura del Proyecto

```
Modelo_predictivo_SICOP/
├── etl/
│   ├── descarga.py                    # Descarga desde API de SICOP
│   ├── transforma.py                  # Normaliza CSV → Parquet
│   ├── carga_staging.py               # Carga Parquet → staging de DuckDB
│   ├── construir_modelo_final.py      # Transforma staging → esquema final
│   ├── esquema_staging.sql            # DDL para tablas de staging
│   └── csv_config.py                  # Mapeos de columnas por CSV
├── modelo/
│   └── entrena_y_predice.py           # Entrenamiento de modelo ML y predicción
├── data/
│   ├── raw/                           # CSVs descargados (por mes)
│   ├── staging/                       # Archivos Parquet (por mes)
│   └── sicop.duckdb                   # Base de datos DuckDB
├── requirements.txt                   # Dependencias de Python
├── .gitignore                         # Ignora *.duckdb, .venv, etc.
└── README.md                          # Este archivo
```

---

## Diagrama de Arquitectura

```
┌─────────────────────────────────────┐
│   API Pública de SICOP (ZIP Mensual)│
│  Azure Blob Storage (YYYYMM.zip)    │
└──────────────┬──────────────────────┘
               │ descarga.py
               ▼
┌─────────────────────────────────────┐
│  Capa Raw (data/raw/YYYYMM)         │
│  8 archivos CSV                     │
└──────────────┬──────────────────────┘
               │ transforma.py
               │ (Normaliza columnas)
               ▼
┌─────────────────────────────────────┐
│ Datos Staging (data/staging/YYYYMM) │
│ 8 archivos Parquet (todo VARCHAR)   │
└──────────────┬──────────────────────┘
               │ carga_staging.py
               ▼
┌─────────────────────────────────────┐
│   Esquema Staging de DuckDB         │
│  (staging.dim_*, staging.fact_*)    │
│   Todas las columnas: VARCHAR       │
└──────────────┬──────────────────────┘
               │ construir_modelo_final.py
               │ (Tipado, deduplicación, join)
               ▼
┌─────────────────────────────────────┐
│   Esquema Final de DuckDB           │
│  (final.dim_*, final.fact_*)        │
│   Tipado, desduplicado, listo para ML
└──────────────┬──────────────────────┘
               │ entrena_y_predice.py
               ▼
┌─────────────────────────────────────┐
│  Modelo ML y Predicciones           │
│  (clasificador scikit-learn)        │
└─────────────────────────────────────┘
```

---

## Pipeline ETL

### Fase 1: Extracción (`descarga.py`)

**Propósito:** Descarga datos mensuales de compras públicas desde la API de SICOP.

**Modos:**
- **Modo normal (por defecto):** Descarga solo el mes vigente (zona horaria de Costa Rica)
  ```bash
  python etl/descarga.py
  ```
- **Modo backfill:** Descarga un rango de meses históricos
  ```bash
  python etl/descarga.py --backfill --inicio 202301 --fin 202312
  ```

**Salida:** Directorios `data/raw/{yyyymm}/` con archivos CSV extraídos

**Características Principales:**
- Calcula automáticamente el mes vigente en zona horaria de Costa Rica (TZ: America/Costa_Rica)
- Lógica de reintento de 3 intentos para resiliencia de red
- Extrae ZIP en memoria para reducir E/S de disco

---

### Fase 2: Transformación (`transforma.py`)

**Propósito:** Normaliza y reestructura archivos CSV sin procesar en un formato de staging consistente.

**Comando:**
```bash
# Transforma solo el mes vigente
python etl/transforma.py

# Transforma todos los meses en data/raw/
python etl/transforma.py --backfill
```

**Salida:** Archivos `data/staging/{yyyymm}/{tabla}.parquet`

**Características Principales:**
- **Coincidencia flexible de columnas:** Maneja variaciones de encabezados entre años (p. ej., "TAMAÑO_PROVEEDOR" vs "Tamano_Proveedor")
  - La comparación usa nombres normalizados (minúsculas, sin acentos)
- **Procesamiento selectivo de archivos:** Solo procesa archivos CSV listados en `csv_config.py`; ignora otros
- **Columnas faltantes manejadas con elegancia:** Llena columnas faltantes con NULL en lugar de fallar; imprime advertencias
- **Manejo de codificación:** Lee UTF-8 con soporte de BOM, maneja varios formatos de comillas/separadores

**Filosofía de Diseño:**
- Transformación mínima en esta etapa — todos los datos permanecen como texto (VARCHAR)
- Flexible para manejar cambios de esquema año a año
- Agrega una columna `periodo` para filtrado por mes en capas posteriores

---

### Fase 3: Carga de Staging (`carga_staging.py`)

**Propósito:** Carga archivos parquet en el esquema `staging` de DuckDB.

**Comando:**
```bash
# Carga solo el mes vigente
python etl/carga_staging.py

# Carga todos los meses (para configuración inicial)
python etl/carga_staging.py --backfill
```

**Base de Datos:** `data/sicop.duckdb`

**Esquema:** `staging.*`

**Características Principales:**
- **Operaciones idempotentes:** Elimina los datos del mes existente antes de recargar (sin duplicados)
- **Sin deduplicación:** Staging es una copia fiel de datos sin procesar
  - La deduplicación ocurre en la capa final (p. ej., retiene el nombre más reciente de proveedor)
- **Creación automática de esquema:** Crea esquema `staging` si no existe

---

### Fase 4: Construir Esquema Final (`construir_modelo_final.py`)

**Propósito:** Crea un esquema analítico curado, tipado y listo para producción a partir de datos de staging.

**Comando:**
```bash
python etl/construir_modelo_final.py
```

**Esquema:** `final.*`

**Características Principales:**

#### Manejo Flexible de Fechas
- Soporta formatos de fecha mixtos (timestamps ISO con segundos, DD/MM/AAAA)
- La macro `fecha_flexible()` maneja ambos automáticamente
- Convierte fechas que no se pueden procesar a NULL silenciosamente (usar para verificación de calidad de datos)

#### Deduplicación
- Las dimensiones (instituciones, proveedores) retienen solo el registro más reciente por entidad
- Las tablas de hechos se desduplican por mes (el período más reciente gana)

#### Calidad de Datos y Tipado
- Todas las columnas numéricas/fecha usan `TRY_CAST` para conversión segura
- Los resultados NULL indican desajustes de formato (indicador de calidad de datos)
- Límite de 500 caracteres en descripciones de productos para evitar campos de texto demasiado grandes

#### Tablas de Dimensión
- **`final.dim_instituciones`**: Instituciones públicas (única por cédula, registro más reciente)
- **`final.dim_proveedores`**: Proveedores (única por cédula, registro más reciente)
- **`final.dim_productos`**: Códigos de productos y categorías
  - Códigos de 16 dígitos extraídos de 4 fuentes
  - Descripción más frecuente por código
  - Segmentada por categoría UNSPSC (10–95)

#### Tablas de Hechos
- **`final.fact_lineas_carteles`**: Líneas de licitación con bandera booleana `adjudicada`
  - Preserva contexto completo de licitación (monto estimado, clasificación)
  - Vincula a líneas adjudicadas mediante subconsulta EXISTS
- **`final.fact_lineas_ofertas`**: Ofertas de proveedores por línea
- **`final.fact_lineas_adjudicadas`**: Decisiones de adjudicación y montos adjudicados

---

## Esquema de Base de Datos

### Esquema Final (`final.*`)

Tablas curadas, tipadas y desduplicas.

```
final.dim_instituciones
├── cedula (VARCHAR)
├── nombre_institucion (VARCHAR)
├── zona_geo_inst (VARCHAR)
└── fecha_ingreso (DATE)

final.dim_proveedores
├── cedula_proveedor (VARCHAR)
├── nombre_proveedor (VARCHAR)
├── tipo_proveedor (VARCHAR)
├── tamano_proveedor (VARCHAR)
├── zona_geo_prov (VARCHAR)
└── fecha_registro (DATE)

final.dim_productos
├── cod_producto (BIGINT)
├── descripcion_producto (VARCHAR)
├── segmento (INTEGER, categoría UNSPSC)
└── nombre_segmento (VARCHAR)

final.fact_lineas_carteles
├── nro_sicop (VARCHAR)
├── numero_linea (VARCHAR)
├── numero_partida (VARCHAR)
├── cedula_institucion (VARCHAR)
├── fecha_publicacion (DATE)
├── nro_procedimiento (VARCHAR)
├── tipo_procedimiento (VARCHAR)
├── modalidad_procedimiento (VARCHAR)
├── cartel_stat (VARCHAR)
├── cartel_nm (VARCHAR)
├── fecha_apertura (DATE)
├── clasificacion_cartel (VARCHAR)
├── monto_estimado_cartel (DOUBLE)
├── cod_producto (VARCHAR)
├── cantidad_solicitada (DOUBLE)
├── precio_unitario_estimado (DOUBLE)
├── tipo_moneda (VARCHAR)
├── tipo_cambio_crc (DOUBLE)
├── monto_linea (DOUBLE)
├── desc_linea (VARCHAR)
└── adjudicada (BOOLEAN)

final.fact_lineas_ofertas
├── nro_sicop (VARCHAR)
├── nro_oferta (VARCHAR)
├── nro_linea (VARCHAR)
├── cedula_proveedor (VARCHAR)
├── fecha_oferta (DATE)
├── tipo_oferta (VARCHAR)
├── cod_producto (VARCHAR)
├── cantidad_ofertada (DOUBLE)
├── precio_unitario_ofertado (DOUBLE)
├── tipo_moneda (VARCHAR)
├── tipo_cambio_crc (DOUBLE)
└── monto_linea (DOUBLE)

final.fact_lineas_adjudicadas
├── nro_sicop (VARCHAR)
├── nro_linea (VARCHAR)
├── nro_oferta (VARCHAR)
├── cedula_institucion (VARCHAR)
├── numero_procedimiento (VARCHAR)
├── descr_procedimiento (VARCHAR)
├── fecha_adjudicacion (DATE)
├── monto_adjudicado_linea (DOUBLE)
├── cedula_proveedor (VARCHAR)
├── cod_producto (VARCHAR)
├── cantidad_adjudicada (DOUBLE)
├── precio_unitario_adjudicado (DOUBLE)
├── tipo_moneda (VARCHAR)
└── tipo_cambio_crc (DOUBLE)
```

### Esquema Staging (`staging.*`)

Todas las columnas son VARCHAR (texto). Cada tabla incluye una columna `periodo` (formato YYYYMM).

```
staging.dim_instituciones
├── cedula
├── nombre_institucion
├── zona_geo_inst
├── fecha_ingreso
└── periodo

staging.dim_proveedores
├── cedula_proveedor
├── nombre_proveedor
├── tipo_proveedor
├── tamano_proveedor
├── zona_geo_prov
├── fecha_registro
└── periodo

staging.fact_carteles
├── nro_sicop
├── cedula_institucion
├── fecha_publicacion
├── nro_procedimiento
├── tipo_procedimiento
├── modalidad_procedimiento
├── cartel_stat
├── cartel_nm
├── fechah_apertura
├── clas_obj
├── monto_est
└── periodo

staging.fact_lineas_carteles
├── nro_sicop
├── numero_linea
├── numero_partida
├── cantidad_solicitada
├── precio_unitario_estimado
├── tipo_moneda
├── tipo_cambio_crc
├── codigo_identificacion
├── monto_reservado
├── desc_linea
└── periodo

staging.fact_ofertas
├── nro_sicop
├── nro_oferta
├── cedula_proveedor
├── fecha_presenta_oferta
├── tipo_oferta
└── periodo

staging.fact_lineas_ofertas
├── nro_oferta
├── nro_linea
├── codigo_producto_cl
├── cantidad_ofertada
├── precio_unitario_ofertado
├── tipo_moneda
├── tipo_cambio_crc
└── periodo

staging.fact_adjudicaciones
├── nro_sicop
├── cedula
├── numero_procedimiento
├── descr_procedimiento
├── linea
├── prod_id
├── fecha_adjud_firme
├── monto_adju_linea
└── periodo

staging.fact_lineas_adjudicadas
├── nro_sicop
├── nro_oferta
├── nro_linea
├── codigo_producto
├── cedula_proveedor
├── cantidad_adjudicada
├── precio_unitario_adjudicado
├── tipo_moneda
├── tipo_cambio_crc
└── periodo
```

---

## Instalación y Configuración

### Requisitos Previos
- Python 3.8+
- Ambiente virtual (recomendado)

### Instalar Dependencias
```bash
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### Dependencias
```
requests        # Solicitudes HTTP a la API de SICOP
pandas          # Manipulación de datos y análisis de CSV
pyarrow         # E/S de archivos Parquet
duckdb          # Base de datos SQL incrustada
scikit-learn    # Modelos de aprendizaje automático
joblib          # Serialización de modelos
```

---

## Inicio Rápido

### Pipeline ETL Completo (Primera Ejecución)

```bash
# 1. Descargar datos (ejemplo: enero 2023 a diciembre 2024)
python etl/descarga.py --backfill --inicio 202301 --fin 202412

# 2. Transformar CSV a parquet
python etl/transforma.py --backfill

# 3. Cargar en esquema staging de DuckDB
python etl/carga_staging.py --backfill

# 4. Construir esquema analítico final
python etl/construir_modelo_final.py

# 5. Entrenar modelo y generar predicciones
python modelo/entrena_y_predice.py
```

### Actualizaciones Incrementales (Diaria/Mensual)

```bash
# Descargar solo el mes vigente
python etl/descarga.py

# Transformar y cargar el mes vigente
python etl/transforma.py
python etl/carga_staging.py

# Reconstruir esquema final (lee todos los datos de staging)
python etl/construir_modelo_final.py

# Reentrenar modelo con nuevos datos
python modelo/entrena_y_predice.py
```

---

## Configuración

### Mapeo de Columnas CSV (`etl/csv_config.py`)

Define qué columnas extraer de cada CSV y a qué tabla de staging mapean:

```python
CSV_CONFIG = {
    "Proveedores.csv": {
        "tabla": "dim_proveedores",
        "columnas": [
            "CEDULA_PROVEEDOR",
            "NOMBRE_PROVEEDOR",
            "TIPO_PROVEEDOR",
            # ...
        ]
    },
    # ... más mapeos
}
```

**Para agregar o eliminar columnas:**
1. Edita `etl/csv_config.py`
2. Reejecutar `construir_modelo_final.py` para reflejar cambios de esquema

### Ruta de Base de Datos

Predeterminado: `data/sicop.duckdb`

Para cambiar, actualiza la variable `RUTA_DUCKDB` en:
- `etl/carga_staging.py`
- `etl/construir_modelo_final.py`
- `modelo/entrena_y_predice.py`

---

## Consultar la Base de Datos

### CLI Interactivo de DuckDB
```bash
duckdb data/sicop.duckdb
```

### Ejemplo en Python
```python
import duckdb

con = duckdb.connect("data/sicop.duckdb")

# Consultar esquema final
licitaciones = con.execute("""
    SELECT 
        nro_sicop, cartel_nm, fecha_publicacion, monto_estimado_cartel,
        COUNT(*) as num_lineas
    FROM final.fact_lineas_carteles
    WHERE fecha_publicacion >= DATE '2024-01-01'
    GROUP BY nro_sicop, cartel_nm, fecha_publicacion, monto_estimado_cartel
    ORDER BY monto_estimado_cartel DESC
    LIMIT 10
""").fetch_all()

con.close()
```

---

## Calidad de Datos y Solución de Problemas

### Problema: Columnas faltantes o NULL después de la transformación

**Causa:** El nombre de columna cambió en el CSV fuente (desajuste de encabezado)

**Solución:**
1. Verifica los encabezados del CSV sin procesar
2. Actualiza `csv_config.py` con el nombre de columna correcto
3. Reejecutar `transforma.py` y `carga_staging.py`

### Problema: El análisis de fechas falla (valores NULL)

**Causa:** Formato de fecha no reconocido por la macro `fecha_flexible`

**Solución:**
1. Verifica el CSV sin procesar para el formato de fecha real
2. Agrega una nueva rama de formato a la macro `fecha_flexible` en `construir_modelo_final.py`:
   ```sql
   CREATE OR REPLACE MACRO fecha_flexible(valor) AS
       COALESCE(
           TRY_CAST(LEFT(valor, 10) AS DATE),
           TRY_STRPTIME(valor, '%d/%m/%Y')::DATE,
           TRY_STRPTIME(valor, '%d-%m-%Y')::DATE  -- Agrega nuevo formato aquí
       )
   ```
3. Reejecutar `construir_modelo_final.py`

### Problema: Problemas de memoria o rendimiento

**Consejos de Optimización de DuckDB:**
- Filtra por rango de fechas en consultas
- Usa tablas de staging directamente si necesitas datos sin procesar (sin tipos, más rápido)
- Aumenta threads de DuckDB: `SET threads = 4;`

---
<!-- This content will not appear in the rendered Markdown
## Licencia

[Agrega tu licencia aquí, p. ej., MIT, Apache 2.0, etc.]

## Contribuir

[Agrega pautas de contribución aquí]

## Contacto

[Agrega información de autor/contacto aquí]
 -->
