# Módulo 8 - Tarea 1 · Pipeline Apache Beam (HRL)

Este pipeline estandariza, filtra y enriquece datos de interacción de fans (HRL) leyendo **N** archivos JSON y un CSV con datos de países. Genera un archivo **JSON Lines (.jsonl)** listo para análisis.

## Operaciones
1. **Estandarización `RaceID`** al formato `<string><numero>` en minúsculas. Ej.: `Cup 25` → `cup25`, `race 11` → `race11`.
2. **Filtrado**: se excluyen registros con `DeviceType == "Other"`.
3. **Enriquecimiento**: se reemplaza `ViewerLocationCountry` por `LocationData` con:
   - `country`, `capital`, `continent`, `official language`, `currency` (tomados del CSV).

Si un país no está en el CSV, se rellena `LocationData` con el nombre del país y el resto vacío.

## Estructuras
**JSON (input):**
- `FanID` (str)
- `RaceID` (str)
- `Timestamp` (str)
- `ViewerLocationCountry` (str)
- `DeviceType` (str)
- `EngagementMetric secondswatched` (int)
- `PredictionClicked` (bool)
- `MerchandisingClicked` (bool)

**CSV (input):**
- `Country`, `Capital`, `Continent`, `Main Official Language`, `Currency` *(otros campos se ignoran)*

**JSON (output):**
- `FanID`, `RaceID` *(estandarizado)*, `Timestamp`, `DeviceType`,
  `EngagementMetric secondswatched`, `PredictionClicked`, `MerchandisingClicked`,
  `LocationData` *(objeto con: `country`, `capital`, `continent`, `official language`, `currency`)*.

## Requisitos
- Python 3.9+
- Apache Beam (DirectRunner) — ver `requirements.txt`.

## Instalación
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Datos de ejemplo (del ZIP)
- JSONs:
  - `/mnt/data/datos_ejemplo/datos de ejemplo/raw data/cup25_fan_engagement-000-of-001.json`
  - `/mnt/data/datos_ejemplo/datos de ejemplo/raw data/league04_fan_engagement-000-of-001.json`
  - `/mnt/data/datos_ejemplo/datos de ejemplo/raw data/race11_fan_engagement-000-of-001.json`
- CSV:
  - `/mnt/data/datos_ejemplo/datos de ejemplo/enrichment data/country_data_v2.csv`

> **Nota:** Use comillas si las rutas tienen espacios.

## Ejecución (DirectRunner)
Ejemplo usando *glob* para N JSONs:
```bash
python pipeline.py       --json_inputs "/mnt/data/datos_ejemplo/datos de ejemplo/raw data/*_fan_engagement-000-of-001.json"       --csv_path "/mnt/data/datos_ejemplo/datos de ejemplo/enrichment data/country_data_v2.csv"       --output_path "/mnt/data/datos_ejemplo/salida/hrl_enriched"
```

O pasando múltiples patrones `--json_inputs`:
```bash
python pipeline.py       --json_inputs "/mnt/data/datos_ejemplo/datos de ejemplo/raw data/cup25_fan_engagement-000-of-001.json"       --json_inputs "/mnt/data/datos_ejemplo/datos de ejemplo/raw data/league04_fan_engagement-000-of-001.json"       --json_inputs "/mnt/data/datos_ejemplo/datos de ejemplo/raw data/race11_fan_engagement-000-of-001.json"       --csv_path "/mnt/data/datos_ejemplo/datos de ejemplo/enrichment data/country_data_v2.csv"       --output_path "/mnt/data/datos_ejemplo/salida/hrl_enriched"
```

Esto generará un único archivo `hrl_enriched-00000-of-00001.jsonl` en el directorio `salida/`.

## Opciones adicionales
- `--csv_delimiter` para cambiar el delimitador del CSV (por defecto `,`).
- Puede añadir *PipelineOptions* de Beam al final (p. ej., `--runner=DirectRunner`). Para Dataflow, agregue las opciones de GCP habituales.

## Manejo de casos borde
- Si el CSV no cuenta con un país, `LocationData.country` se rellena con el valor original y los demás campos quedan vacíos.
- Claves extra en los JSONs de entrada se descartan para adherirse al esquema esperado.
