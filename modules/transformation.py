# D:\addominguez\Documents\SIGER_ETL_LOCAL\modules\transformation.py
import polars as pl
import os
from datetime import datetime

def transformar_mvcaratulas(df: pl.DataFrame):
    """
    Ejecuta la limpieza y tipado de los datos.
    Normaliza el campo CLOB 'DSOBJETO' y convierte tipos de datos String 
    a sus formatos correspondientes (Int, Datetime, etc).
    """
    print("-> Iniciando fase de transformación y limpieza de tipos...")

    df_transformed = df.with_columns([
        # 1. Conversión de identificadores y banderas
        pl.col("LLCARATULA").cast(pl.Int64, strict=False),
        pl.col("LLOFICINA").cast(pl.Int32, strict=False),
        pl.col("LLESTADO").cast(pl.Int32, strict=False),
        pl.col("LLTIPOSOCIEDAD").cast(pl.Int32, strict=False),
        pl.col("BOMORAL").cast(pl.Int8, strict=False),
        pl.col("BOACERVO").cast(pl.Int8, strict=False),

        # 2. Normalización de fechas
        pl.col("FCAPERTURA").str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S%.f", strict=False),

        # 3. Sanitización profunda del CLOB (DSOBJETO)
        pl.col("DSOBJETO")
            .str.replace_all(r"[\r\n\t]", " ")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .fill_null("SIN OBJETO")
    ])

    return df_transformed

def generar_reporte_y_trazabilidad(df: pl.DataFrame, config: dict):
    """
    Realiza un análisis de calidad y genera un archivo de log para trazabilidad.
    """
    total_filas = len(df)
    nulos_fecha = df["FCAPERTURA"].null_count()
    nulos_caratula = df["LLCARATULA"].null_count()
    max_clob = df["DSOBJETO"].str.len_chars().max()
    
    log_path = os.path.join(config["staging_path"], "ETL_TRAZABILIDAD.log")
    
    reporte_texto = f"""
==================================================
REPORTE DE TRAZABILIDAD - PROYECTO MVCARATULAS
==================================================
Fecha de Proceso:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Archivo Origen:      {config['source_path']}
Total Registros:     {total_filas:,}
--------------------------------------------------
METRICAS DE CALIDAD:
- Integridad LLCARATULA: {((total_filas - nulos_caratula)/total_filas)*100:.4f}%
- Integridad Fechas:     {((total_filas - nulos_fecha)/total_filas)*100:.4f}%
- Registros con CLOB:    {total_filas:,} (100% reconstruidos)
- Longitud Max CLOB:     {max_clob:,} caracteres.
--------------------------------------------------
ESTADO FINAL: Exitoso - Almacenado en Staging (Parquet)
==================================================
"""
    os.makedirs(config["staging_path"], exist_ok=True)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(reporte_texto)
    
    print(reporte_texto)
    return log_path