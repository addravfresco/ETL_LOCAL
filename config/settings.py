# D:\addominguez\Documents\SIGER_ETL_LOCAL\config\settings.py
import os

BASE_DIR = "D:/addominguez/Documents/SIGER_ETL_LOCAL/"

ETL_CONFIG = {
    "source_path": "D:/SIGER/MVCARATULAS.csv",
    "staging_path": os.path.join(BASE_DIR, "data/staging/"),
    "delimiter": "|",
    "columns_ordered": [
        "LLCARATULA", "LLOFICINA", "CRFME", "FCAPERTURA", "DSRFC", 
        "DSANTREG", "DSDENSOCIAL", "DSDIRECCION", "LLMUNICIPIO", "DSMUNICIPIO", 
        "LLESTADO", "DSESTADO", "DSDURACION", "LLGIRO", "DSGIRO", 
        "LLTIPOSOCIEDAD", "DSTIPOSOCIEDAD", "DSOBJETO", "LLNACIONALIDAD", 
        "DSNACIONALIDAD", "DSCURP", "BOMORAL", "LLESTATUSCARATULA", "BOACERVO"
    ]
}