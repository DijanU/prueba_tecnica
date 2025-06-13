import csv
from fastapi import FastAPI
import sqlite3

app = FastAPI()

importaciones = []


## lector csv upgradeado por chat
with open('polizas_full.csv', newline='', encoding='utf-8') as csvfile:
    lector = csv.DictReader(csvfile)
    for fila in lector:
        fila = {clave.lstrip('\ufeff'): valor for clave, valor in fila.items()}
        

        fila['tipo_cambio_dolar'] = float(fila.get('tipo_cambio_dolar', 0) or 0)
        fila['cantidad_fraccion'] = float(fila.get('cantidad_fraccion', 0) or 0)
        fila['tasa_dai'] = float(fila.get('tasa_dai', 0) or 0)
        fila['valor_dai'] = float(fila.get('valor_dai', 0) or 0)
        fila['valor_cif_uds'] = float(fila.get('valor_cif_uds', 0) or 0)
        fila['tasa_cif_cantidad_fraccion'] = float(fila.get('tasa_cif_cantidad_fraccion', 0) or 0)

        importaciones.append(fila)


#---- DB section

#db connection
conn = sqlite3.connect("importaciones.db")
cursor = conn.cursor()

#clean db por si no es la primera vez que se corre
cursor.execute("DROP TABLE IF EXISTS importaciones_detalle")
cursor.execute("DROP TABLE IF EXISTS declaraciones")

cursor.execute('''
CREATE TABLE IF NOT EXISTS declaraciones (
    correlativo TEXT PRIMARY KEY,
    fecha_declaracion TEXT,
    aduana TEXT,
    tipo_regimen TEXT,
    tipo_cambio_dolar REAL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS importaciones_detalle (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    correlativo TEXT,
    sac TEXT,
    descripcion TEXT,
    pais TEXT,
    tipo_unidad_medida TEXT,
    cantidad_fraccion REAL,
    tasa_dai REAL,
    valor_dai REAL,
    valor_cif_uds REAL,
    tasa_cif_cantidad_fraccion REAL,
    FOREIGN KEY (correlativo) REFERENCES declaraciones(correlativo)
)
''')


correlativos_insertados = set()

for fila in importaciones:
    if fila['correlativo'] not in correlativos_insertados:
        cursor.execute('''
            INSERT OR IGNORE INTO declaraciones (
                correlativo, fecha_declaracion, aduana, tipo_regimen, tipo_cambio_dolar
            ) VALUES (
                :correlativo, :fecha_declaracion, :aduana, :tipo_regimen, :tipo_cambio_dolar
            )
        ''', fila)
        correlativos_insertados.add(fila['correlativo'])


    cursor.execute('''
        INSERT INTO importaciones_detalle (
            correlativo, sac, descripcion, pais, tipo_unidad_medida,
            cantidad_fraccion, tasa_dai, valor_dai, valor_cif_uds, tasa_cif_cantidad_fraccion
        ) VALUES (
            :correlativo, :sac, :descripcion, :pais, :tipo_unidad_medida,
            :cantidad_fraccion, :tasa_dai, :valor_dai, :valor_cif_uds, :tasa_cif_cantidad_fraccion
        )
    ''', fila)

conn.commit()
conn.close()

#---- Endpoints

@app.get("/{n}")
def listar_importaciones(n: int):
    limit = 2000
    offset = limit * (n - 1)
    conn = sqlite3.connect("importaciones.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT d.correlativo, d.fecha_declaracion, d.aduana, d.tipo_regimen, d.tipo_cambio_dolar,
               i.id, i.sac, i.descripcion, i.pais, i.tipo_unidad_medida,
               i.cantidad_fraccion, i.tasa_dai, i.valor_dai, i.valor_cif_uds, i.tasa_cif_cantidad_fraccion
        FROM declaraciones d
        JOIN importaciones_detalle i ON d.correlativo = i.correlativo
        ORDER BY i.id
        LIMIT ? OFFSET ?
    """, (limit, offset))
    columns = [col[0] for col in cursor.description]
    results = cursor.fetchall()
    conn.close()
    data = [dict(zip(columns, fila)) for fila in results]
    return {"datos": data}


@app.get("/importaciones/{identifier}")
def importacion_correlativo(identifier: str):
    conn = sqlite3.connect("importaciones.db")
    cursor = conn.cursor()
    try:
        if len(identifier) == 8:
            cursor.execute("""
                SELECT d.correlativo, d.fecha_declaracion, d.aduana, d.tipo_regimen, d.tipo_cambio_dolar,
                       i.id, i.sac, i.descripcion, i.pais, i.tipo_unidad_medida,
                       i.cantidad_fraccion, i.tasa_dai, i.valor_dai, i.valor_cif_uds, i.tasa_cif_cantidad_fraccion
                FROM declaraciones d
                JOIN importaciones_detalle i ON d.correlativo = i.correlativo
                WHERE d.correlativo = ?
            """, (identifier,))
        else:
            cursor.execute("""
                SELECT d.correlativo, d.fecha_declaracion, d.aduana, d.tipo_regimen, d.tipo_cambio_dolar,
                       i.id, i.sac, i.descripcion, i.pais, i.tipo_unidad_medida,
                       i.cantidad_fraccion, i.tasa_dai, i.valor_dai, i.valor_cif_uds, i.tasa_cif_cantidad_fraccion
                FROM declaraciones d
                JOIN importaciones_detalle i ON d.correlativo = i.correlativo
                WHERE i.sac = ?
            """, (identifier,))
        columns = [col[0] for col in cursor.description]
        results = cursor.fetchall()
        data = [dict(zip(columns, fila)) for fila in results]
        return {"importaciones": data}
    finally:
        conn.close()


@app.get("/estadisticas/por-pais")
def estadisticas_por_pais():
    conn = sqlite3.connect("importaciones.db")
    cursor = conn.cursor()
    cursor.execute("""SELECT pais,
                   COUNT(*) as total_importaciones,
                   SUM(valor_cif_uds) AS total_valor_cif
                   FROM importaciones_detalle
                   GROUP BY pais
                   ORDER BY total_valor_cif DESC
                   """,)
    column = [col[0] for col in cursor.description]
    results = cursor.fetchall()
    data = [dict(zip(column, fila)) for fila in results]
    conn.close()
    return {"importaciones":data}


@app.get("/estadisticas/por-aduana")
def estadisticas_por_aduana():
    conn = sqlite3.connect("importaciones.db")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT d.aduana,
               COUNT(*) AS total_importaciones,
               SUM(id.valor_cif_uds) AS total_valor_cif
        FROM importaciones_detalle id
        JOIN declaraciones d ON id.correlativo = d.correlativo
        GROUP BY d.aduana
        ORDER BY total_valor_cif DESC
    """)
    column = [col[0] for col in cursor.description]
    results = cursor.fetchall()
    data = [dict(zip(column, fila)) for fila in results]
    conn.close()
    return {"importaciones": data}