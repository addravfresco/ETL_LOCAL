# D:\addominguez\Documents\SIGER_ETL_LOCAL\modules\extraction.py
import polars as pl
import os

def reconstruir_archivo_fisico(config: dict, pipes_esperados: int = 23):
    ruta_origen = config["source_path"]
    ruta_tmp = os.path.join(config["staging_path"], "MVCARATULAS_CLEAN.tmp")
    
    os.makedirs(config["staging_path"], exist_ok=True)
    print(f"-> Iniciando reconstrucción física. Destino: {ruta_tmp}")
    
    registro_acumulado = ""
    contador_filas = 0

    with open(ruta_origen, 'r', encoding='utf-8', errors='ignore') as f_in, \
         open(ruta_tmp, 'w', encoding='utf-8') as f_out:
        
        next(f_in) # Saltar encabezado
        for linea in f_in:
            registro_acumulado += linea.replace('\n', ' ').replace('\r', ' ')
            if registro_acumulado.count(config["delimiter"]) >= pipes_esperados:
                f_out.write(registro_acumulado.strip() + "\n")
                registro_acumulado = ""
                contador_filas += 1
                if contador_filas % 250000 == 0:
                    print(f"   Registros reconstruidos y escritos: {contador_filas}...")

    print(f"-> ✅ Archivo reconstruido con {contador_filas} filas.")
    return ruta_tmp

def extraer_datos_limpios(config: dict):
    ruta_limpia = reconstruir_archivo_fisico(config)
    esquema = {col: pl.String for col in config["columns_ordered"]}
    
    print("-> Cargando archivo reconstruido a DataFrame (Modo Tolerante)...")
    df = pl.read_csv(
        ruta_limpia,
        separator=config["delimiter"],
        has_header=False,
        schema=esquema,
        quote_char=None, # Importante para evitar errores de comillas
        truncate_ragged_lines=True,
        low_memory=True
    )
    return df