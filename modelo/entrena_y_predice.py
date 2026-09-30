"""Entrena (opcionalmente) el clasificador de probabilidad de adjudicación
por proveedor, y genera predicciones para las ofertas de líneas de cartel
todavía abiertas.
 
Por defecto solo genera predicciones con el último modelo guardado -- eso
mantiene el job diario liviano. Reentrena desde cero los lunes, si se pasa
--forzar (útil tras un backfill o un cambio en el modelo), o si el archivo
del modelo simplemente no existe todavía (primera corrida). El .joblib se
persiste entre corridas subiéndolo al mismo Release que sicop.duckdb -- ver
el workflow actualizacion_diaria.yml.
 
Variables de entrada (revisadas para reducir la cardinalidad de la rama
categórica frente a la versión anterior, que usaba cod_producto completo):
- categóricas: tamano_proveedor (dim_proveedores), segmento (dim_productos,
  ~56 valores en vez de miles de códigos de producto), tipo_procedimiento
  (fact_lineas_carteles).
- numéricas: proveedores_adjudicados_distintos (dim_instituciones),
  porcentaje_exito (dim_proveedores), radio_competitividad y
  cantidad_ofertada (fact_lineas_ofertas), productos_distintos_ofertados
  (dim_proveedores), cantidad_solicitada (fact_lineas_carteles).
 
Todas estas viven en tablas distintas, así que las dos consultas SQL unen
fact_lineas_ofertas con fact_lineas_carteles, dim_proveedores, dim_productos
y dim_instituciones -- no hace falta ninguna transformación en Python, todo
el join queda resuelto en DuckDB antes de que pandas reciba el resultado.
"""
import argparse
import os
from datetime import datetime
from zoneinfo import ZoneInfo
 
import duckdb
import joblib
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
 
RUTA_DUCKDB = "data/sicop.duckdb"
RUTA_MODELO = "modelo/clasificador_adjudicacion.joblib"
 
COLUMNAS_CATEGORICAS = ["tamano_proveedor", "segmento", "tipo_procedimiento"]
COLUMNAS_NUMERICAS = [
    "proveedores_adjudicados_distintos",
    "porcentaje_exito",
    "radio_competitividad",
    "productos_distintos_ofertados",
    "cantidad_solicitada",
    "cantidad_ofertada",
]
 
# Las cuatro tablas de enriquecimiento (fact_lineas_carteles, dim_proveedores,
# dim_productos, dim_instituciones) se unen igual en entrenamiento y en
# predicción, así que quedan como un solo bloque de JOINs reutilizado --
# solo cambia el SELECT final y si se filtra por líneas pendientes.
JOINS_ENRIQUECIMIENTO = """
    FROM final.fact_lineas_ofertas o
    JOIN final.fact_lineas_carteles c
        ON c.nro_sicop = o.nro_sicop AND c.numero_linea = o.nro_linea
    LEFT JOIN final.dim_proveedores p ON p.cedula_proveedor = o.cedula_proveedor
    LEFT JOIN final.dim_productos pr ON pr.cod_producto = TRY_CAST(o.cod_producto AS BIGINT)
    LEFT JOIN final.dim_instituciones i ON i.cedula = c.cedula_institucion
"""
 
CONSULTA_ENTRENAMIENTO = f"""
    SELECT
        p.tamano_proveedor,
        pr.segmento,
        c.tipo_procedimiento,
        i.proveedores_adjudicados_distintos,
        p.porcentaje_exito,
        o.radio_competitividad,
        p.productos_distintos_ofertados,
        c.cantidad_solicitada,
        o.cantidad_ofertada,
        (a.nro_oferta IS NOT NULL) AS fue_adjudicado
    {JOINS_ENRIQUECIMIENTO}
    LEFT JOIN final.fact_lineas_adjudicadas a
        ON a.nro_sicop = o.nro_sicop AND a.nro_linea = o.nro_linea
       AND a.nro_oferta = o.nro_oferta AND a.cedula_proveedor = o.cedula_proveedor
"""
 
CONSULTA_PENDIENTES = f"""
    SELECT
        o.nro_sicop,
        o.nro_linea,
        o.nro_oferta,
        o.cedula_proveedor,
        p.tamano_proveedor,
        pr.segmento,
        c.tipo_procedimiento,
        i.proveedores_adjudicados_distintos,
        p.porcentaje_exito,
        o.radio_competitividad,
        p.productos_distintos_ofertados,
        c.cantidad_solicitada,
        o.cantidad_ofertada
    {JOINS_ENRIQUECIMIENTO}
    WHERE c.adjudicada = false
"""
 
 
def debe_reentrenar(forzar: bool) -> bool:
    if forzar:
        return True
    if not os.path.exists(RUTA_MODELO):
        # No hay modelo persistido todavía (primera corrida, o el archivo
        # se perdió por algún motivo) -- entrenar sin importar el día.
        return True
    hoy = datetime.now(ZoneInfo("America/Costa_Rica"))
    return hoy.weekday() == 0  # 0 = lunes
 
 
def construir_pipeline() -> Pipeline:
    # RandomForestClassifier no acepta NaN de forma nativa, así que hay que
    # imputar los dos grupos de columnas antes de que lleguen al bosque:
    # - categóricas: los nulos se rellenan con un valor fijo ("desconocido")
    #   antes de codificar, para que nunca lleguen strings vacíos al encoder.
    # - numéricas: los nulos se rellenan con la mediana de esa columna --
    #   más robusta que la media frente a valores atípicos (montos muy altos).
    transformador_categoricas = Pipeline([
        ("imputar", SimpleImputer(strategy="constant", fill_value="desconocido")),
        ("codificar", OneHotEncoder(handle_unknown="ignore")),
    ])
    transformador_numericas = SimpleImputer(strategy="median")
 
    preprocesador = ColumnTransformer([
        ("categoricas", transformador_categoricas, COLUMNAS_CATEGORICAS),
        ("numericas", transformador_numericas, COLUMNAS_NUMERICAS),
    ])
    return Pipeline([
        ("preprocesamiento", preprocesador),
        ("clasificador", RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1)),
    ])
 
 
def entrenar(con) -> None:
    df = con.execute(CONSULTA_ENTRENAMIENTO).df()
    y = df.pop("fue_adjudicado")
    X = df[COLUMNAS_CATEGORICAS + COLUMNAS_NUMERICAS]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    pipeline = construir_pipeline()
    pipeline.fit(X_train, y_train)
    print(f"Exactitud en validación: {pipeline.score(X_test, y_test):.3f}")
    joblib.dump(pipeline, RUTA_MODELO)
 
 
def predecir(con) -> None:
    pipeline = joblib.load(RUTA_MODELO)
    pendientes = con.execute(CONSULTA_PENDIENTES).df()
    if pendientes.empty:
        print("No hay ofertas pendientes por predecir.")
        return
    X = pendientes[COLUMNAS_CATEGORICAS + COLUMNAS_NUMERICAS]
    pendientes["probabilidad_adjudicacion"] = pipeline.predict_proba(X)[:, 1]
    resultado = pendientes[["nro_sicop", "nro_linea", "nro_oferta", "cedula_proveedor", "probabilidad_adjudicacion"]]
    con.execute("CREATE OR REPLACE TABLE final.predicciones_adjudicacion AS SELECT * FROM resultado")
    print(f"{len(resultado)} predicciones guardadas en final.predicciones_adjudicacion")
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--forzar", action="store_true")
    args = parser.parse_args()
 
    con = duckdb.connect(RUTA_DUCKDB)
    if debe_reentrenar(args.forzar):
        entrenar(con)
    predecir(con)
    con.close()