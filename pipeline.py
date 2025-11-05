#!/usr/bin/env python3
"""
Pipeline de Apache Beam para estandarizar, filtrar y enriquecer
datos de interacción de fans (HRL) a partir de archivos JSON y un CSV.

- Estandariza `RaceID` al formato <string><numero> en minúsculas (ej: "Cup 25" -> "cup25").
- Filtra registros con `DeviceType == "Other"`.
- Enriquecer usando CSV a partir de `ViewerLocationCountry`, reemplazándolo por `LocationData` anidado
  con los campos: country, capital, continent, official language, currency.

Salida: JSON Lines (.jsonl), 1 shard.
"""
import argparse
import json
import re
from typing import Dict, Iterable, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches

COUNTRY_FIELDS_MAP = {
    "Country": "country",
    "Capital": "capital",
    "Continent": "continent",
    "Main Official Language": "official language",
    "Currency": "currency",
}

RACEID_RE = re.compile(r"([A-Za-z]+)\s*0*?(\d+)$")

def normalize_race_id(value: str) -> str:
    if not value:
        return value
    v = value.strip().lower().replace("_", " ")
    # si ya es como 'cup25' intentar parsear componiendo
    m = RACEID_RE.search(v.replace(" ", "")) or re.search(r"([A-Za-z]+)\s*(\d+)$", v, re.I)
    if m:
        letters = m.group(1)
        digits = m.group(2)
        try:
            # int(...) para remover ceros a la izquierda
            digits = str(int(digits))
        except Exception:
            pass
        return f"{letters.lower()}{digits}"
    # fallback: compactar espacios
    return re.sub(r"\s+", "", v)

class ParseJsonFiles(beam.DoFn):
    """Lee contenidos de archivos JSON que pueden venir como arreglo o line-delimited JSON."""
    def process(self, file: beam.io.fileio.ReadableFile) -> Iterable[dict]:
        data = file.read_utf8()
        s = data.strip()
        # Si parece un array JSON
        if s.startswith("["):
            try:
                arr = json.loads(s)
                for obj in arr:
                    if isinstance(obj, dict):
                        yield obj
            except Exception:
                # fallback: intentar por líneas
                for line in s.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        else:
            # Line-delimited JSON
            for line in s.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue

def parse_csv_to_kv(lines: List[str], delimiter: str = ",") -> Dict[str, dict]:
    import csv
    from io import StringIO
    headers = [
        "Country",
        "Capital",
        "GDP (Nominal, 2024, in billions USD)",
        "Population (2024, in millions)",
        "Pop. Growth Rate (2024, %)",
        "Life Expectancy (2024, years)",
        "Median Age (2024, years)",
        "Urban Population (2022, %)",
        "Continent",
        "Main Official Language",
        "Currency",
    ]
    sio = StringIO("\n".join(lines))
    reader = csv.reader(sio, delimiter=delimiter)
    out = {}
    for row in reader:
        if not row or all(not c.strip() for c in row):
            continue
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        d = dict(zip(headers, row))
        key = d.get("Country", "").strip()
        if not key:
            continue
        filtered = { k2: d.get(k1, "").strip() for k1, k2 in COUNTRY_FIELDS_MAP.items() }
        out[key] = filtered
    return out

def enrich_record(rec: dict, country_index: Dict[str, dict]) -> dict:
    rec = dict(rec)  # copia superficial
    # Estandarización RaceID
    rec["RaceID"] = normalize_race_id(rec.get("RaceID", ""))
    # Enriquecimiento
    country_key = rec.pop("ViewerLocationCountry", "").strip()
    payload = country_index.get(country_key)
    if not payload:
        payload = {
            "country": country_key or "",
            "capital": "",
            "continent": "",
            "official language": "",
            "currency": "",
        }
    rec["LocationData"] = payload
    # Filtrado de campos extra
    allowed = {
        "FanID",
        "RaceID",
        "Timestamp",
        "DeviceType",
        "EngagementMetric secondswatched",
        "PredictionClicked",
        "MerchandisingClicked",
        "LocationData",
    }
    clean = {k: v for k, v in rec.items() if k in allowed}
    return clean

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json_inputs",
        action="append",
        required=True,
        help="Ruta(s) glob de archivos JSON de entrada. Puede repetirse. Ej: --json_inputs 'raw/*.json'",
    )
    parser.add_argument(
        "--csv_path",
        required=True,
        help="Ruta del CSV de países para enriquecimiento.",
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="Prefijo de salida (sin extensión). Se generará .jsonl con 1 shard.",
    )
    parser.add_argument(
        "--csv_delimiter",
        default=",",
        help="Delimitador del CSV (por defecto ',').",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=options) as p:
        # CSV -> índice (side input)
        csv_lines = (
            p
            | "ReadCSVAll" >> beam.io.ReadFromText(known_args.csv_path, skip_header_lines=1)
        )
        country_index = beam.pvalue.AsSingleton(
            csv_lines
            | "CSVToList" >> beam.combiners.ToList()
            | "ListToDict" >> beam.Map(lambda lines: parse_csv_to_kv(lines, delimiter=known_args.csv_delimiter))
        )

        # Ingesta JSON
        file_patterns = known_args.json_inputs
        matched = None
        for i, pattern in enumerate(file_patterns):
            pc = p | f"Match-{i}" >> MatchFiles(pattern)
            matched = pc if matched is None else (matched, pc) | f"FlattenMatches-{i}" >> beam.Flatten()

        raw_records = (
            matched
            | "ReadMatches" >> ReadMatches()
            | "ParseJsonFiles" >> beam.ParDo(ParseJsonFiles())
        )

        # Filtrado DeviceType != "Other"
        filtered = raw_records | "FilterDeviceType" >> beam.Filter(lambda r: r.get("DeviceType") != "Other")

        # Enriquecer + estandarizar
        enriched = filtered | "Enrich" >> beam.Map(lambda r, idx: enrich_record(r, idx), idx=country_index)

        # Serializar JSON Lines
        serialized = enriched | "ToJSON" >> beam.Map(lambda r: json.dumps(r, ensure_ascii=False))

        _ = serialized | "WriteOut" >> beam.io.WriteToText(
            known_args.output_path,
            file_name_suffix=".jsonl",
            num_shards=1,
        )

if __name__ == "__main__":
    run()
