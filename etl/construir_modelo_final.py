"""Construye el esquema `final` -- curado, tipado, y listo para el
dashboard y el modelo de ML -- a partir del esquema `staging`.

Se reconstruye completo desde cero en cada corrida (CREATE OR REPLACE),
leyendo todo el staging acumulado (no solo el mes vigente). Con el volumen
de datos de este proyecto, DuckDB hace esto en segundos, así que es más
simple que mantener un "final" incremental.

Tres decisiones de diseño importantes:

1. TRY_CAST en vez de CAST: si un valor no se puede convertir, el resultado
   es NULL en vez de que falle toda la carga. Ojo con esto: TRY_CAST no
   avisa cuando falla, solo devuelve NULL en silencio -- si ves NULL
   inesperados en una columna, es la señal de que el formato de origen no
   es el que se está asumiendo.

2. Fechas con formato mixto: en los CSV reales aparecen fechas como
   "2017-11-08 00:00:00.0000000" (ISO con fracción de segundo, típico de
   exportaciones de SQL Server) y como "25/11/2025" (DD/MM/AAAA). Como no
   interesa la hora, la macro `fecha_flexible` recorta el string a los
   primeros 10 caracteres (que cubre cualquier variante de ISO, sin
   importar cuántos dígitos de fracción traiga) y, si eso no calza, intenta
   explícitamente el formato DD/MM/AAAA con strptime. Si aparece un tercer
   formato en el futuro, es una rama más dentro del COALESCE de la macro.

3. fact_lineas_carteles NO se filtra a solo líneas pendientes. Se conserva
   todo el histórico y se agrega una columna `adjudicada` (booleana) en su
   lugar. Así no se pierde el contexto del cartel (monto estimado,
   clasificación, etc.) para las líneas que sí se adjudicaron, que es
   justamente lo que hace falta para entrenar el modelo de clasificación.

4. dim_productos se arma juntando las 4 columnas de código de producto que
   aparecen sueltas en staging (codigo_identificacion, codigo_producto_cl,
   codigo_producto, prod_id) -- se filtran a solo códigos de 16 dígitos, y
   la descripción de cada uno se toma como la más frecuente entre todas las
   veces que ese código aparece en fact_lineas_carteles.desc_linea (es la
   única fuente que trae texto descriptivo). El segmento (primeros 2
   dígitos del código) se resuelve contra el catálogo UNSPSC estándar.

"""
import duckdb

RUTA_DUCKDB = "data/sicop.duckdb"


def construir(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS final")

    # Macro reutilizable para las dos variantes de fecha que aparecen en los
    # CSV. LEFT(valor, 10) toma solo "AAAA-MM-DD" de cualquier timestamp ISO
    # (con o sin fracción de segundo, sin importar cuántos dígitos traiga);
    # si eso falla (p. ej. porque venía como "25/11/2025"), se intenta ese
    # formato explícitamente. Nunca lanza error -- si ninguna de las dos
    # formas calza, el resultado es NULL.
    con.execute("""
        CREATE OR REPLACE MACRO fecha_flexible(valor) AS
            COALESCE(
                TRY_CAST(LEFT(valor, 10) AS DATE),
                TRY_STRPTIME(valor, '%d/%m/%Y')::DATE
            )
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final.dim_instituciones AS
        SELECT
            cedula,
            nombre_institucion,
            zona_geo_inst,
            fecha_flexible(fecha_ingreso) AS fecha_ingreso
        FROM staging.dim_instituciones
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cedula ORDER BY periodo DESC) = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final.dim_proveedores AS
        SELECT
            cedula_proveedor,
            nombre_proveedor,
            tipo_proveedor,
            tamano_proveedor,
            zona_geo_prov,
            fecha_flexible(fecha_registro) AS fecha_registro
        FROM staging.dim_proveedores
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cedula_proveedor ORDER BY periodo DESC) = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final.dim_productos AS
        WITH codigos_crudos AS (
            -- Unión de las 4 fuentes de código de producto en staging.
            SELECT SUBSTRING(TRIM(codigo_identificacion), 1, 16) AS cod_producto,
                   TRIM(desc_linea)                              AS descr
            FROM   staging.fact_lineas_carteles
 
            UNION ALL
 
            SELECT SUBSTRING(TRIM(codigo_producto_cl), 1, 16), NULL
            FROM   staging.fact_lineas_ofertas
 
            UNION ALL
 
            SELECT SUBSTRING(TRIM(codigo_producto), 1, 16), NULL
            FROM   staging.fact_lineas_adjudicadas
 
            UNION ALL
 
            SELECT SUBSTRING(TRIM(prod_id), 1, 16), NULL
            FROM   staging.fact_adjudicaciones
        ),
        limpio AS (
            -- Solo códigos válidos de exactamente 16 dígitos
            SELECT cod_producto, NULLIF(descr, '') AS descr
            FROM   codigos_crudos
            WHERE  cod_producto ~ '^[0-9]{16}$'
        ),
        codigos AS (
            SELECT DISTINCT cod_producto
            FROM   limpio
        ),
        conteo AS (
            -- Frecuencia de cada descripción por código
            SELECT cod_producto, descr, COUNT(*) AS n
            FROM   limpio
            WHERE  descr IS NOT NULL
            GROUP  BY cod_producto, descr
        ),
        moda_producto AS (
            -- Descripción más frecuente por código de 16 dígitos
            -- (empate -> orden alfabético, resultado determinístico)
            SELECT cod_producto,
                   descr AS descripcion_producto,
                   ROW_NUMBER() OVER (PARTITION BY cod_producto
                                      ORDER BY n DESC, descr) AS rk
            FROM   conteo
        ),
        segmentos (segmento, nombre_segmento) AS (
            VALUES
            (10, 'Animales vivos, accesorios y suministros'),
            (11, 'Material mineral, textil y vegetal'),
            (12, 'Productos químicos incluyendo bioquímicos'),
            (13, 'Resinas, caucho y espuma'),
            (14, 'Papel, materiales de oficina y artículos de arte'),
            (15, 'Combustibles, lubricantes y aceites'),
            (20, 'Equipos de minería y cantería'),
            (21, 'Equipos de granja y jardín y silvicultura'),
            (22, 'Equipos de construcción y mantenimiento'),
            (23, 'Maquinaria industrial y equipos de manufactura'),
            (24, 'Materiales y accesorios de manejo de materiales'),
            (25, 'Vehículos comerciales, militares y de uso personal'),
            (26, 'Componentes y suministros de potencia generación y transmisión'),
            (27, 'Herramientas y maquinaria general'),
            (30, 'Estructuras, edificaciones, fabricaciones y acondicionamiento de espacios'),
            (31, 'Materiales de manufactura y procesamiento'),
            (32, 'Componentes electrónicos'),
            (39, 'Iluminación, distribución eléctrica y accesorios'),
            (40, 'Equipos de distribución y condicionamiento de fluidos'),
            (41, 'Instrumentos de laboratorio, medición y observación'),
            (42, 'Equipo médico, accesorios e insumos'),
            (43, 'Tecnología de información, telecomunicaciones y radiodifusión'),
            (44, 'Suministros de oficina, accesorios y consumibles'),
            (45, 'Imprenta, equipos fotográficos y audiovisuales'),
            (46, 'Seguridad, protección y defensa'),
            (47, 'Limpieza y mantenimiento de instalaciones y productos'),
            (48, 'Equipos y suministros industriales'),
            (49, 'Deportes, recreación, entretenimiento y educación'),
            (50, 'Productos alimenticios, bebidas y tabaco'),
            (51, 'Medicamentos y productos farmacéuticos'),
            (52, 'Ropa, calzado y accesorios de uso personal'),
            (53, 'Artículos domésticos, personales y de consumo'),
            (54, 'Artículos de uso público y eventos'),
            (55, 'Publicaciones, grabaciones y medios de información'),
            (56, 'Mobiliario y decoración'),
            (60, 'Instrumentos musicales, artes y manualidades'),
            (64, 'Artículos de colección y bellas artes'),
            (70, 'Servicios de agricultura, pesca, silvicultura y caza'),
            (71, 'Servicios de minería y petróleo y gas'),
            (72, 'Servicios de construcción y mantenimiento de edificios'),
            (73, 'Servicios de manufactura industrial'),
            (76, 'Servicios de limpieza industrial'),
            (77, 'Servicios medioambientales'),
            (78, 'Servicios de transporte, almacenamiento y correo'),
            (80, 'Servicios profesionales de gestión y administración'),
            (81, 'Servicios de ingeniería, investigación y tecnología'),
            (82, 'Servicios editoriales y gráficos'),
            (83, 'Servicios de salud pública'),
            (84, 'Servicios financieros y de seguros'),
            (85, 'Servicios de salud y asistencia social'),
            (86, 'Servicios de educación y formación'),
            (90, 'Servicios de viaje, alimentación y alojamiento'),
            (91, 'Servicios personales y domésticos'),
            (92, 'Defensa, orden público y seguridad'),
            (93, 'Servicios políticos y de asuntos cívicos'),
            (94, 'Organizaciones, asociaciones y afiliaciones'),
            (95, 'Tierras, edificios, estructuras y vías')
        )
        SELECT c.cod_producto::BIGINT                    AS cod_producto,
               LEFT(mp.descripcion_producto, 500)        AS descripcion_producto,
               SUBSTRING(c.cod_producto, 1, 2)::INTEGER  AS segmento,
               s.nombre_segmento
        FROM   codigos c
        LEFT JOIN moda_producto mp ON mp.cod_producto = c.cod_producto AND mp.rk = 1
        LEFT JOIN segmentos     s  ON s.segmento = SUBSTRING(c.cod_producto, 1, 2)::INTEGER
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final.fact_lineas_carteles AS
        SELECT
            c.nro_sicop,
            l.numero_linea,
            l.numero_partida,
            c.cedula_institucion,
            fecha_flexible(c.fecha_publicacion) AS fecha_publicacion,
            c.nro_procedimiento,
            c.tipo_procedimiento,
            c.modalidad_procedimiento,
            c.cartel_stat,
            c.cartel_nm,
            fecha_flexible(c.fechah_apertura) AS fecha_apertura,
            c.clas_obj AS clasificacion_cartel,
            TRY_CAST(c.monto_est AS DOUBLE) AS monto_estimado_cartel,
            l.codigo_identificacion AS cod_producto,
            TRY_CAST(l.cantidad_solicitada AS DOUBLE) AS cantidad_solicitada,
            TRY_CAST(l.precio_unitario_estimado AS DOUBLE) AS precio_unitario_estimado,
            l.tipo_moneda,
            TRY_CAST(l.tipo_cambio_crc AS DOUBLE) AS tipo_cambio_crc,
            -- monto_linea siempre en colones: si ya es CRC no se convierte (factor 1)
            -- , si no, se multiplica por el tipo de cambio.
            TRY_CAST(l.monto_reservado AS DOUBLE)
                * CASE WHEN l.tipo_moneda = 'CRC' THEN 1
                       ELSE TRY_CAST(l.tipo_cambio_crc AS DOUBLE) END AS monto_linea_crc,
            l.desc_linea,
            EXISTS (
                SELECT 1 FROM staging.fact_lineas_adjudicadas a
                WHERE a.nro_sicop = c.nro_sicop AND a.nro_linea = l.numero_linea
            ) AS adjudicada
        FROM staging.fact_carteles c
        JOIN staging.fact_lineas_carteles l USING (nro_sicop)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.nro_sicop, l.numero_linea ORDER BY l.periodo DESC
        ) = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final.fact_lineas_ofertas AS
        SELECT
            o.nro_sicop,
            o.nro_oferta,
            lo.nro_linea,
            o.cedula_proveedor,
            fecha_flexible(o.fecha_presenta_oferta) AS fecha_oferta,
            o.tipo_oferta,
            lo.codigo_producto_cl AS cod_producto,
            TRY_CAST(lo.cantidad_ofertada AS DOUBLE) AS cantidad_ofertada,
            TRY_CAST(lo.precio_unitario_ofertado AS DOUBLE) AS precio_unitario_ofertado,
            lo.tipo_moneda,
            TRY_CAST(lo.tipo_cambio_crc AS DOUBLE) AS tipo_cambio_crc,
            (TRY_CAST(lo.cantidad_ofertada AS DOUBLE) * TRY_CAST(lo.precio_unitario_ofertado AS DOUBLE))
                * CASE WHEN lo.tipo_moneda = 'CRC' THEN 1
                       ELSE TRY_CAST(lo.tipo_cambio_crc AS DOUBLE) END AS monto_oferta_linea_crc
        FROM staging.fact_ofertas o
        JOIN staging.fact_lineas_ofertas lo USING (nro_oferta)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY o.nro_oferta, lo.nro_linea ORDER BY lo.periodo DESC
        ) = 1
    """)

    con.execute("""
        CREATE OR REPLACE TABLE final.fact_lineas_adjudicadas AS
        SELECT
            a.nro_sicop,
            a.linea AS nro_linea,
            la.nro_oferta,
            a.cedula AS cedula_institucion,
            a.numero_procedimiento,
            a.descr_procedimiento,
            fecha_flexible(a.fecha_adjud_firme) AS fecha_adjudicacion,
            TRY_CAST(a.monto_adju_linea AS DOUBLE) AS monto_adjudicado_linea,
            (TRY_CAST(la.precio_unitario_adjudicado AS DOUBLE) * TRY_CAST(la.cantidad_adjudicada AS DOUBLE)
                * CASE WHEN la.tipo_moneda = 'CRC' THEN 1 
                    ELSE TRY_CAST(la.tipo_cambio_crc AS DOUBLE) END) AS monto_adjudicado_linea_crc,
            la.cedula_proveedor,
            la.codigo_producto AS cod_producto,
            TRY_CAST(la.cantidad_adjudicada AS DOUBLE) AS cantidad_adjudicada,
            TRY_CAST(la.precio_unitario_adjudicado AS DOUBLE) AS precio_unitario_adjudicado,
            la.tipo_moneda,
            TRY_CAST(la.tipo_cambio_crc AS DOUBLE) AS tipo_cambio_crc
        FROM staging.fact_adjudicaciones a
        JOIN staging.fact_lineas_adjudicadas la
            ON la.nro_sicop = a.nro_sicop AND la.nro_linea = a.linea
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY a.nro_sicop, a.linea, la.nro_oferta ORDER BY la.periodo DESC
        ) = 1
    """)

    print(
        "esquema final reconstruido: dim_instituciones, dim_proveedores, "
        "fact_lineas_carteles, fact_lineas_ofertas, fact_lineas_adjudicadas"
    )


if __name__ == "__main__":
    con = duckdb.connect(RUTA_DUCKDB)
    construir(con)
    con.close()