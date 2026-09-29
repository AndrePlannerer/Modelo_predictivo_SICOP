"""Entrena (opcionalmente) el clasificador de probabilidad de adjudicación
por proveedor, y genera predicciones para las ofertas de líneas de cartel
todavía abiertas.

Por defecto solo genera predicciones con el último modelo guardado -- eso
mantiene el job diario liviano. Reentrena desde cero los lunes, si se pasa
--forzar (útil tras un backfill o un cambio en el modelo), o si el archivo
del modelo simplemente no existe todavía (primera corrida). El .joblib se
persiste entre corridas subiéndolo al mismo Release que sicop.duckdb -- ver
el workflow actualizacion_diaria.yml.

Con el esquema final, fact_lineas_ofertas y fact_lineas_adjudicadas ya
comparten columnas limpias (nro_sicop, nro_linea, nro_oferta,
cedula_proveedor) -- no hace falta derivar nada a mano.
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

COLUMNAS_CATEGORICAS = ["cod_producto", "tipo_moneda", "tipo_oferta"]
COLUMNAS_NUMERICAS = ["cantidad_ofertada", "precio_unitario_ofertado", "monto_oferta_linea_crc"]

CONSULTA_ENTRENAMIENTO = """
    SELECT
        o.*,
        (a.nro_oferta IS NOT NULL) AS fue_adjudicado
    FROM final.fact_lineas_ofertas o
    LEFT JOIN final.fact_lineas_adjudicadas a
        USING (nro_sicop, nro_linea, nro_oferta, cedula_proveedor)
"""

CONSULTA_PENDIENTES = """
    SELECT o.*
    FROM final.fact_lineas_ofertas o
    JOIN final.fact_lineas_carteles c
        ON c.nro_sicop = o.nro_sicop AND c.numero_linea = o.nro_linea
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