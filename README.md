# NYC Taxi ETL Pipeline

Pipeline ETL para procesar datos históricos de viajes de NYC Taxi & Limousine Commission usando Apache Spark, basado en una arquitectura Data Lakehouse con capas Raw, Bronze, Silver, Gold y carga final en SQLite.

## Requisitos

- **Java JDK 17+**
- **Apache Spark 4.x** (usado: Spark 4.1.2)
- **Hadoop WinUtils** (para Windows, instalado en `C:\hadoop`)
- **Python 3.10+** (usado: Python 3.13)

### Variables de Entorno

```powershell
JAVA_HOME = C:\Program Files\Java\jdk-<version>
SPARK_HOME = C:\spark
HADOOP_HOME = C:\hadoop
```

Añadir al `PATH`:
- `%SPARK_HOME%\bin`
- `%HADOOP_HOME%\bin`

## Instalación

```powershell
cd proyecto

# Crear entorno virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Iniciar Jupyter
jupyter notebook
```

## Estructura del Proyecto

```
proyecto/
├── config/
│   └── etl_config.yaml           # Configuración global del pipeline
├── data/
│   ├── raw/                      # Parquets originales descargados
│   │   ├── yellow/               # Particionado por year/month
│   │   ├── green/
│   │   ├── fhvhv/
│   │   └── bad_parquet/          # Archivos dañados de Apache Parquet Testing
│   ├── bronze/                   # Esquema canónico unificado
│   ├── silver/                   # Datos transformados con campos derivados
│   ├── gold/                     # Agregaciones analíticas (3 tablas)
│   ├── quarantine/               # Archivos rechazados con clasificación
│   └── audit/                    # Inventarios, métricas de calidad
├── notebooks/
│   ├── 01_extraccion.ipynb           # Fase 1: Extracción e inventario
│   ├── 02_diagnostico_reconstruccion.ipynb  # Fase 2: Diagnóstico y Bronze
│   ├── 03_transformacion_validacion.ipynb   # Fase 3-4-5: Transformación y calidad
│   ├── 04_carga_base_datos.ipynb     # Fase 6-7: Gold y carga SQLite
│   └── 05_reporte_calidad_conclusiones.ipynb
├── src/
│   ├── config_loader.py          # Carga de configuración YAML
│   ├── extract.py                # Descarga e inventario de archivos
│   ├── schema_recovery.py        # Diagnóstico de esquemas y construcción Bronze
│   ├── transformations.py        # Transformación Bronze → Silver
│   ├── quality_rules.py          # Reglas de validación de calidad
│   ├── load.py                   # Capa Gold y carga a SQLite
│   └── utils.py                  # Funciones auxiliares
├── requirements.txt
├── README.md
└── Documento_Tecnico.md
```

## Ejecución del Pipeline

Los notebooks deben ejecutarse en orden secuencial. Abrir en Jupyter y ejecutar todas las celdas de cada notebook:

| Notebook | Fase | Descripción |
|---|---|---|
| `01_extraccion.ipynb` | Fase 1 | Descarga archivos Parquet de NYC TLC, genera inventario técnico y procesa archivos dañados (bad_parquet) |
| `02_diagnostico_reconstruccion.ipynb` | Fase 2 | Diagnostica esquemas, detecta columnas faltantes/extra, homologa a esquema canónico y construye capa Bronze |
| `03_transformacion_validacion.ipynb` | Fase 3-5 | Procesa cuarentena, transforma Bronze a Silver con campos derivados, aplica reglas de calidad |
| `04_carga_base_datos.ipynb` | Fase 6-7 | Construye capa Gold (3 agregaciones), carga las 6 tablas en SQLite |
| `05_reporte_calidad_conclusiones.ipynb` | Reporte | Consultas de verificación y reporte final |

## Configuración (`etl_config.yaml`)

Parámetros principales editables:

```yaml
data_sources:
  tlc_nyc:
    services: ["yellow", "green", "fhvhv"]  # Tipos de servicio
    years: [2024, 2025, 2026]                # Años a procesar
    base_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"

spark:
  config:
    spark.driver.memory: "4g"                # Memoria del driver
    spark.sql.shuffle.partitions: "8"        # Paralelismo controlado
    spark.sql.adaptive.enabled: "true"       # Optimización adaptativa
```

## Fuente de Datos

- **NYC TLC Trip Record Data**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Apache Parquet Testing** (archivos dañados): https://github.com/apache/parquet-testing/tree/master/bad_data
- URL base de descarga: `https://d37ci6vzurychx.cloudfront.net/trip-data/`

## Base de Datos

SQLite embebido (no requiere instalación ni servidor). La base se crea automáticamente en:

```
data/etl_results.db
```

### Tablas generadas

| Tabla | Registros | Descripción |
|---|---|---|
| `gold_trips_clean` | 3,371,576 | Viajes válidos a nivel granular |
| `gold_daily_revenue` | 2,564 | Resumen diario por servicio |
| `gold_location_performance` | 149,335 | Rendimiento por ruta |
| `quality_rejected_records` | 2,593 | Registros que violaron reglas |
| `quality_metrics_summary` | 84 | Métricas de calidad por servicio/mes |
| `audit_file_inventory` | 84 | Inventario de archivos procesados |

### Consultar desde línea de comandos

```powershell
# Usando sqlite3 (incluido en Python)
python -c "import sqlite3; conn = sqlite3.connect('data/etl_results.db'); print(conn.execute('SELECT COUNT(*) FROM gold_trips_clean').fetchone()[0])"
```

## Validación de Resultados

Ejemplo de consultas de verificación ejecutadas exitosamente:

### Q1: Revenue por servicio
| service_type | total_trips | total_revenue |
|---|---|---|
| yellow | 3,371,576 | $98,928,891.74 |

### Q3: Top 5 rutas por revenue
| pickup_loc | dropoff_loc | total_trips | total_revenue | avg_duration |
|---|---|---|---|---|
| 132 | 265 | 6,463 | $846,401 | 45.2 min |
| 132 | 230 | 6,327 | $609,466 | 62.7 min |
| 138 | 230 | 5,917 | $483,183 | 42.5 min |
| 237 | 236 | 22,974 | $366,989 | 7.6 min |
| 132 | 164 | 3,443 | $335,150 | 52.9 min |

## Parámetros del Spark

Configuraciones aplicadas para optimizar rendimiento con memoria limitada (4 GB):

- `spark.sql.adaptive.enabled`: true (optimización dinámica de consultas)
- `spark.sql.adaptive.coalescePartitions.enabled`: true (fusión de particiones pequeñas)
- `spark.sql.shuffle.partitions`: 8 (reduce paralelismo para evitar OOM)
- `spark.sql.sources.partitionOverwriteMode`: dynamic (escritura precisa de particiones)
- `spark.sql.parquet.enableVectorizedReader`: true (lectura vectorizada)
- `spark.driver.memory`: 4g (límite de heap para el driver JVM)
