SIGER_ETL_LOCAL/
├── config/
│   └── settings.py
├── data/
│   └── staging/
│       ├── MVCARATULAS_CLEAN.tmp      (Archivo intermedio)
│       ├── MVCARATULAS_PROCESADO.parquet (Dato final E-T)
│       └── ETL_AUDIT_LOG.txt          (Trazabilidad)
├── modules/
│   ├── extraction.py
│   └── transformation.py
└── main.py