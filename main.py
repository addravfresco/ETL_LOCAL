# D:\addominguez\Documents\SIGER_ETL_LOCAL\main.py
import os
from config.settings import ETL_CONFIG
from modules import extraction, transformation

def ejecutar_etl():
    """
    Coordina la extracción y transformación de MVCARATULAS.
    Garantiza la trazabilidad del proceso mediante un log de auditoría.
    """
    print("--- INICIANDO PROCESO E-T (MVCARATULAS) ---")
    
    try:
        # 1. Extracción con reconstrucción de registros
        df_crudo = extraction.extraer_datos_limpios(ETL_CONFIG)
        
        # 2. Transformación de tipos y limpieza de CLOB
        df_limpio = transformation.transformar_mvcaratulas(df_crudo)
        
        # 3. Generación de Trazabilidad y Reporte
        transformation.generar_reporte_y_trazabilidad(df_limpio, ETL_CONFIG)
        
        # 4. Almacenamiento en archivo de Tipo Parquet para agilizar carga
        os.makedirs(ETL_CONFIG["staging_path"], exist_ok=True)
        ruta_parquet = os.path.join(ETL_CONFIG["staging_path"], "MVCARATULAS_PROCESADO.parquet")
        df_limpio.write_parquet(ruta_parquet)

        # 5. Exportar muestra de validación (CSV de 100 líneas)
        ruta_muestra = os.path.join(ETL_CONFIG["staging_path"], "MUESTRA_100_REGISTROS.csv")
        df_limpio.head(100).write_csv(ruta_muestra, separator=",")
        print(f"-> Muestra para inspección visual generada en: {ruta_muestra}")
        
        print(f"✅ Proceso finalizado. Datos listos en: {ruta_parquet}")

    except Exception as e:
        print(f"❌ ERROR CRÍTICO EN EL PIPELINE: {e}")

if __name__ == "__main__":
    ejecutar_etl()
