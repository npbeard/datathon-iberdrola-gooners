from __future__ import annotations

import json
import math
import re
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm
from shapely.geometry import Point


DATOS_GOB_BASE = (
    "https://raw.githubusercontent.com/Admindatosgobes/Laboratorio-de-Datos/main/"
    + quote("Data Science/Ruta a la electrificación de la Movilidad/Datos/")
)
DEFAULT_NAP_XML_URL = "https://infocar.dgt.es/datex2/v3/miterd/EnergyInfrastructureTablePublication/electrolineras.xml"
DEFAULT_MITMA_TRAFFIC_URL = "https://mapas.fomento.gob.es/arcgis/rest/services/MapaTrafico/Mapa2019web/MapServer/2/query"
DEFAULT_GASOLINERAS_JSON_URL = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/"
DEFAULT_GASOLINERAS_XLS_URL = "https://geoportalgasolineras.es/resources/files/preciosEESS_es.xls"
DEFAULT_INE_MUNICIPAL_POP_URL = "https://servicios.ine.es/wstempus/js/en/DATOS_TABLA/78750"
DEFAULT_INE_HOTEL_OVERNIGHT_URL = "https://servicios.ine.es/wstempus/js/en/DATOS_TABLA/2039"
DEFAULT_INE_PROVINCIAL_OVERNIGHT_URL = "https://servicios.ine.es/wstempus/js/en/DATOS_TABLA/48427"
DEFAULT_EDISTRIBUCION_URL = (
    "https://www.edistribucion.com/content/dam/edistribucion/conexion-a-la-red/"
    "descargables/nodos/demanda/202603/2026_03_04_R1299_demanda.csv"
)
DEFAULT_IDE_URL = "https://www.i-de.es/documents/d/guest/2026_02_04_r1-001_demanda"


def download_file(url: str, output_path: Path, timeout: int = 180) -> Path:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    content = response.content
    if content.lstrip().startswith(b"<!DOCTYPE html") and b"Service unavailable" in content[:2000]:
        raise ValueError(f"Remote source unavailable for {url}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(content)
    return output_path


def download_arcgis_geojson_paginated(
    query_url: str,
    output_path: Path,
    out_fields: Iterable[str],
    where: str = "1=1",
    batch_size: int = 1000,
    timeout: int = 180,
) -> Path:
    features: List[Dict[str, object]] = []
    offset = 0
    requested_fields = ",".join(out_fields)

    while True:
        response = requests.get(
            query_url,
            params={
                "where": where,
                "outFields": requested_fields,
                "f": "geojson",
                "outSR": 4326,
                "resultOffset": offset,
                "resultRecordCount": batch_size,
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("features", [])
        if not batch:
            break
        features.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({"type": "FeatureCollection", "features": features}), encoding="utf-8")
    return output_path


def _strip_namespace(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _find_text(element: ET.Element, tag_name: str) -> Optional[str]:
    for child in element.iter():
        if _strip_namespace(child.tag) == tag_name and child.text:
            value = child.text.strip()
            if value:
                return value
    return None


def _find_texts(element: ET.Element, tag_name: str) -> List[str]:
    values: List[str] = []
    for child in element.iter():
        if _strip_namespace(child.tag) == tag_name and child.text:
            value = child.text.strip()
            if value:
                values.append(value)
    return values


def parse_nap_charging_xml(xml_path: Path) -> pd.DataFrame:
    rows: List[Dict] = []
    context = ET.iterparse(xml_path, events=("end",))

    for _, elem in context:
        if _strip_namespace(elem.tag) != "energyInfrastructureSite":
            continue

        site_id = elem.attrib.get("id", "")
        site_name = _find_text(elem, "value")
        latitude = _find_text(elem, "latitude")
        longitude = _find_text(elem, "longitude")
        operator_name = None
        operator_element = None
        for child in elem.iter():
            if _strip_namespace(child.tag) == "operator":
                operator_element = child
                break
        if operator_element is not None:
            operator_name = _find_text(operator_element, "value")

        address_lines = _find_texts(elem, "value")
        max_powers_kw: List[float] = []
        for value in _find_texts(elem, "maxPowerAtSocket"):
            try:
                max_powers_kw.append(float(value) / 1000.0)
            except ValueError:
                continue

        connector_types = _find_texts(elem, "connectorType")
        rows.append(
            {
                "site_id": site_id,
                "site_name": site_name or site_id,
                "latitude": float(latitude) if latitude else np.nan,
                "longitude": float(longitude) if longitude else np.nan,
                "operator_name": operator_name,
                "address_text": " | ".join(address_lines),
                "connector_count": len(connector_types),
                "max_power_kw": max(max_powers_kw) if max_powers_kw else np.nan,
                "total_power_kw": sum(max_powers_kw) if max_powers_kw else np.nan,
                "source_dataset": "NAP-DGT/MITERD DATEX II",
            }
        )
        elem.clear()

    return pd.DataFrame(rows)


def _roads_for_matching(roads_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    roads = roads_gdf[["carretera", "geometry"]].dropna(subset=["geometry"]).copy()
    if roads.crs is None:
        roads = roads.set_crs("EPSG:4326")
    elif roads.crs.to_string() != "EPSG:4326":
        roads = roads.to_crs("EPSG:4326")
    return roads.to_crs("EPSG:3857")


def spatially_match_chargers_to_roads(
    chargers_df: pd.DataFrame,
    roads_gdf: gpd.GeoDataFrame,
    max_distance_km: float = 5.0,
) -> pd.DataFrame:
    if chargers_df.empty:
        return chargers_df.copy()

    chargers = chargers_df.dropna(subset=["latitude", "longitude"]).copy()
    if chargers.empty:
        chargers["carretera"] = pd.Series(dtype="object")
        chargers["distance_to_route_km"] = pd.Series(dtype="float64")
        chargers["is_interurban_match"] = pd.Series(dtype="bool")
        return chargers

    chargers_gdf = gpd.GeoDataFrame(
        chargers,
        geometry=gpd.points_from_xy(chargers["longitude"], chargers["latitude"]),
        crs="EPSG:4326",
    ).to_crs("EPSG:3857")
    roads_metric = _roads_for_matching(roads_gdf)

    matched = gpd.sjoin_nearest(
        chargers_gdf,
        roads_metric,
        how="left",
        distance_col="distance_to_route_m",
    )
    matched["distance_to_route_km"] = matched["distance_to_route_m"] / 1000.0
    matched["is_interurban_match"] = matched["distance_to_route_km"] <= max_distance_km
    matched.loc[~matched["is_interurban_match"], "carretera"] = pd.NA

    return pd.DataFrame(matched.drop(columns=["geometry", "index_right", "distance_to_route_m"], errors="ignore"))


def summarize_interurban_baseline(matched_df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    if matched_df.empty:
        return pd.DataFrame(columns=["carretera", "existing_station_count"]), 0

    interurban = matched_df[matched_df["is_interurban_match"]].copy()
    total_existing = int(interurban["site_id"].nunique())

    if interurban.empty:
        return pd.DataFrame(columns=["carretera", "existing_station_count"]), total_existing

    by_route = (
        interurban.dropna(subset=["carretera"])
        .groupby("carretera", as_index=False)
        .agg(existing_station_count=("site_id", "nunique"))
    )
    return by_route, total_existing


def build_charging_baseline_from_nap(
    roads_gdf: gpd.GeoDataFrame,
    xml_path: Path,
    matched_output_path: Optional[Path] = None,
    summary_output_path: Optional[Path] = None,
    max_distance_km: float = 5.0,
) -> Dict[str, object]:
    chargers = parse_nap_charging_xml(xml_path)
    matched = spatially_match_chargers_to_roads(chargers, roads_gdf, max_distance_km=max_distance_km)
    by_route, total_existing = summarize_interurban_baseline(matched)

    if matched_output_path is not None:
        matched_output_path.parent.mkdir(parents=True, exist_ok=True)
        matched.to_csv(matched_output_path, index=False)
    if summary_output_path is not None:
        summary_output_path.parent.mkdir(parents=True, exist_ok=True)
        by_route.to_csv(summary_output_path, index=False)

    return {
        "matched": matched,
        "by_route": by_route,
        "total_existing_stations_baseline": total_existing,
    }


def enrich_route_summary_with_baseline(route_summary: pd.DataFrame, baseline_by_route: pd.DataFrame) -> pd.DataFrame:
    enriched = route_summary.copy()
    if baseline_by_route is None or baseline_by_route.empty:
        enriched["existing_station_count"] = 0
    else:
        enriched = enriched.merge(baseline_by_route, how="left", on="carretera")
        enriched["existing_station_count"] = enriched["existing_station_count"].fillna(0).astype(int)

    enriched["coverage_gap_score"] = enriched["route_score"] / (1 + enriched["existing_station_count"])
    return enriched.sort_values("coverage_gap_score", ascending=False).reset_index(drop=True)


def load_mitma_traffic_segments(path: Path) -> gpd.GeoDataFrame:
    if not path.exists():
        return gpd.GeoDataFrame(
            columns=["traffic_route_name", "traffic_length_km", "traffic_imd_total", "traffic_imd_pesado", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )

    traffic = gpd.read_file(path)
    if traffic.empty:
        return gpd.GeoDataFrame(
            columns=["traffic_route_name", "traffic_length_km", "traffic_imd_total", "traffic_imd_pesado", "geometry"],
            geometry="geometry",
            crs="EPSG:4326",
        )
    if traffic.crs is None:
        traffic = traffic.set_crs("EPSG:4326")
    elif traffic.crs.to_string() != "EPSG:4326":
        traffic = traffic.to_crs("EPSG:4326")

    rename_map = {
        "Nombre": "traffic_route_name",
        "Longitud": "traffic_length_km",
        "IMD_total": "traffic_imd_total",
        "IMD_pesado": "traffic_imd_pesado",
    }
    traffic = traffic.rename(columns={key: value for key, value in rename_map.items() if key in traffic.columns})
    for column in ["traffic_route_name", "traffic_length_km", "traffic_imd_total", "traffic_imd_pesado"]:
        if column not in traffic.columns:
            traffic[column] = np.nan if column != "traffic_route_name" else ""

    traffic["traffic_length_km"] = pd.to_numeric(traffic["traffic_length_km"], errors="coerce").fillna(0.0)
    traffic["traffic_imd_total"] = pd.to_numeric(traffic["traffic_imd_total"], errors="coerce").fillna(0.0)
    traffic["traffic_imd_pesado"] = pd.to_numeric(traffic["traffic_imd_pesado"], errors="coerce").fillna(0.0)
    return traffic[
        ["traffic_route_name", "traffic_length_km", "traffic_imd_total", "traffic_imd_pesado", "geometry"]
    ].dropna(subset=["geometry"])


def summarize_traffic_by_route(
    roads_gdf: gpd.GeoDataFrame,
    traffic_gdf: gpd.GeoDataFrame,
    max_distance_km: float = 5.0,
) -> pd.DataFrame:
    if traffic_gdf is None or traffic_gdf.empty:
        return pd.DataFrame(
            columns=["carretera", "traffic_imd_total", "traffic_imd_pesado", "traffic_heavy_share", "traffic_match_count"]
        )

    roads_metric = _roads_for_matching(roads_gdf)
    traffic_metric = traffic_gdf.to_crs("EPSG:3857")
    matched = gpd.sjoin_nearest(
        traffic_metric,
        roads_metric,
        how="left",
        distance_col="distance_to_route_m",
    )
    matched["distance_to_route_km"] = matched["distance_to_route_m"] / 1000.0
    matched = matched[matched["distance_to_route_km"] <= max_distance_km].copy()
    if matched.empty:
        return pd.DataFrame(
            columns=["carretera", "traffic_imd_total", "traffic_imd_pesado", "traffic_heavy_share", "traffic_match_count"]
        )

    matched["traffic_weight"] = matched["traffic_length_km"].replace(0, np.nan).fillna(1.0)
    grouped = (
        matched.groupby("carretera", as_index=False)
        .apply(
            lambda group: pd.Series(
                {
                    "traffic_imd_total": float(np.average(group["traffic_imd_total"], weights=group["traffic_weight"])),
                    "traffic_imd_pesado": float(np.average(group["traffic_imd_pesado"], weights=group["traffic_weight"])),
                    "traffic_match_count": int(len(group)),
                }
            )
        )
        .reset_index(drop=True)
    )
    grouped["traffic_heavy_share"] = np.where(
        grouped["traffic_imd_total"] > 0,
        grouped["traffic_imd_pesado"] / grouped["traffic_imd_total"],
        0.0,
    )
    return grouped


def build_mitma_traffic_inputs(
    roads_gdf: gpd.GeoDataFrame,
    traffic_geojson_path: Path,
    summary_output_path: Path,
    max_distance_km: float = 5.0,
) -> Dict[str, object]:
    traffic_gdf = load_mitma_traffic_segments(traffic_geojson_path)
    by_route = summarize_traffic_by_route(roads_gdf, traffic_gdf, max_distance_km=max_distance_km)
    summary_output_path.parent.mkdir(parents=True, exist_ok=True)
    by_route.to_csv(summary_output_path, index=False)
    return {"traffic_segments": traffic_gdf, "by_route": by_route}


def enrich_route_summary_with_traffic(route_summary: pd.DataFrame, traffic_by_route: pd.DataFrame) -> pd.DataFrame:
    enriched = route_summary.copy()
    for column in ["traffic_imd_total", "traffic_imd_pesado", "traffic_heavy_share"]:
        if column not in enriched.columns:
            enriched[column] = 0.0
    if "traffic_match_count" not in enriched.columns:
        enriched["traffic_match_count"] = 0
    if traffic_by_route is None or traffic_by_route.empty:
        return enriched

    grouped = traffic_by_route.rename(
        columns={
            "traffic_imd_total": "traffic_imd_total_new",
            "traffic_imd_pesado": "traffic_imd_pesado_new",
            "traffic_heavy_share": "traffic_heavy_share_new",
            "traffic_match_count": "traffic_match_count_new",
        }
    )
    enriched = enriched.merge(grouped, how="left", on="carretera")
    enriched["traffic_imd_total"] = enriched["traffic_imd_total_new"].fillna(enriched["traffic_imd_total"]).fillna(0.0)
    enriched["traffic_imd_pesado"] = enriched["traffic_imd_pesado_new"].fillna(enriched["traffic_imd_pesado"]).fillna(0.0)
    enriched["traffic_heavy_share"] = enriched["traffic_heavy_share_new"].fillna(enriched["traffic_heavy_share"]).fillna(0.0)
    enriched["traffic_match_count"] = (
        enriched["traffic_match_count_new"].fillna(enriched["traffic_match_count"]).fillna(0).astype(int)
    )
    enriched = enriched.drop(
        columns=[
            "traffic_imd_total_new",
            "traffic_imd_pesado_new",
            "traffic_heavy_share_new",
            "traffic_match_count_new",
        ],
        errors="ignore",
    )
    return enriched


def parse_geoportal_gasolineras_xls(xls_path: Path) -> pd.DataFrame:
    if not xls_path.exists():
        return pd.DataFrame(
            columns=[
                "provincia",
                "municipio",
                "localidad",
                "codigo_postal",
                "direccion",
                "margen",
                "longitude",
                "latitude",
                "rotulo",
                "tipo_venta",
                "rem",
                "horario",
                "tipo_servicio",
                "source_dataset",
            ]
        )

    raw = pd.read_excel(xls_path, header=3)
    rename_map = {
        "Provincia": "provincia",
        "Municipio": "municipio",
        "Localidad": "localidad",
        "Código postal": "codigo_postal",
        "Dirección": "direccion",
        "Margen": "margen",
        "Longitud": "longitude",
        "Latitud": "latitude",
        "Rótulo": "rotulo",
        "Tipo venta": "tipo_venta",
        "Rem.": "rem",
        "Horario": "horario",
        "Tipo servicio": "tipo_servicio",
    }
    stations = raw.rename(columns={key: value for key, value in rename_map.items() if key in raw.columns}).copy()
    for column in rename_map.values():
        if column not in stations.columns:
            stations[column] = np.nan
    stations["longitude"] = pd.to_numeric(stations["longitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    stations["latitude"] = pd.to_numeric(stations["latitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    stations = stations.dropna(subset=["longitude", "latitude"]).copy()
    stations["source_dataset"] = "Geoportal Gasolineras"
    return stations[
        [
            "provincia",
            "municipio",
            "localidad",
            "codigo_postal",
            "direccion",
            "margen",
            "longitude",
            "latitude",
            "rotulo",
            "tipo_venta",
            "rem",
            "horario",
            "tipo_servicio",
            "source_dataset",
        ]
    ].reset_index(drop=True)


def parse_geoportal_gasolineras_json(json_path: Path) -> pd.DataFrame:
    empty = pd.DataFrame(
        columns=[
            "provincia",
            "municipio",
            "localidad",
            "codigo_postal",
            "direccion",
            "margen",
            "longitude",
            "latitude",
            "rotulo",
            "tipo_venta",
            "rem",
            "horario",
            "tipo_servicio",
            "source_dataset",
        ]
    )
    if not json_path.exists():
        return empty

    payload = json.loads(json_path.read_text(encoding="utf-8-sig"))
    rows = payload.get("ListaEESSPrecio", [])
    if not rows:
        return empty
    stations = pd.DataFrame(rows)
    rename_map = {
        "Provincia": "provincia",
        "Municipio": "municipio",
        "Localidad": "localidad",
        "C.P.": "codigo_postal",
        "Dirección": "direccion",
        "Margen": "margen",
        "Longitud (WGS84)": "longitude",
        "Latitud": "latitude",
        "Rótulo": "rotulo",
        "Tipo Venta": "tipo_venta",
        "Remisión": "rem",
        "Horario": "horario",
        "Tipo Servicio": "tipo_servicio",
        "IDMunicipio": "municipality_id",
    }
    stations = stations.rename(columns={key: value for key, value in rename_map.items() if key in stations.columns}).copy()
    for column in rename_map.values():
        if column not in stations.columns:
            stations[column] = np.nan
    stations["longitude"] = pd.to_numeric(stations["longitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    stations["latitude"] = pd.to_numeric(stations["latitude"].astype(str).str.replace(",", ".", regex=False), errors="coerce")
    stations = stations.dropna(subset=["longitude", "latitude"]).copy()
    stations["source_dataset"] = "MITERD REST Carburantes"
    return stations[
        [
            "provincia",
            "municipio",
            "localidad",
            "codigo_postal",
            "direccion",
            "margen",
            "longitude",
            "latitude",
            "rotulo",
            "tipo_venta",
            "rem",
            "horario",
            "tipo_servicio",
            "municipality_id",
            "source_dataset",
        ]
    ].reset_index(drop=True)


def parse_ine_municipal_population_json(json_path: Path, target_year: int = 2025) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["municipality_code", "municipality_name", "population_year", "municipal_population"])
    if not json_path.exists():
        return empty

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return empty
    if not isinstance(payload, list):
        return empty

    rows: List[Dict[str, object]] = []
    year_str = str(target_year)
    for item in payload:
        name = str(item.get("Nombre", ""))
        if ", " not in name:
            continue
        place_label, sex_label = name.rsplit(", ", 1)
        if sex_label.strip().lower() != "total":
            continue
        if place_label.strip().lower().startswith("national total"):
            continue

        match = re.match(r"(?P<code>\d{5})\s+(?P<name>.+)", place_label.strip())
        if not match:
            continue

        values = item.get("Data", [])
        population_value = None
        for observation in values:
            if str(observation.get("NombrePeriodo", "")) == year_str:
                population_value = observation.get("Valor")
                break
        if population_value in (None, ""):
            continue
        rows.append(
            {
                "municipality_code": match.group("code"),
                "municipality_name": match.group("name").strip(),
                "population_year": target_year,
                "municipal_population": float(population_value),
            }
        )

    if not rows:
        return empty
    return pd.DataFrame(rows).drop_duplicates(subset=["municipality_code"]).reset_index(drop=True)


def _normalize_lookup_text(value: str) -> str:
    normalized = unicodedata.normalize("NFD", str(value or ""))
    normalized = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    normalized = re.sub(r"[^A-Z0-9]+", " ", normalized.upper()).strip()
    return re.sub(r"\s+", " ", normalized)


def parse_ine_provincial_overnight_stays_json(
    json_path: Path,
    target_year: Optional[int] = None,
) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["province_name", "tourism_year", "provincial_overnight_stays"])
    if not json_path.exists():
        return empty

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return empty
    if not isinstance(payload, list):
        return empty

    rows: List[Dict[str, object]] = []
    for item in payload:
        name = str(item.get("Nombre", "")).strip()
        match = re.match(r"Overnight stays\. Total\. (?P<province>.+?)\. Base data\.", name)
        if not match:
            continue
        province_name = match.group("province").strip()
        if province_name.lower() == "national total":
            continue

        observations = item.get("Data", [])
        candidate_values = [
            (int(observation.get("Anyo", 0)), observation.get("Valor"))
            for observation in observations
            if observation.get("Valor") not in (None, "")
        ]
        if target_year is not None:
            candidate_values = [entry for entry in candidate_values if entry[0] == int(target_year)]
        if not candidate_values:
            continue

        tourism_year, tourism_value = max(candidate_values, key=lambda entry: entry[0])
        rows.append(
            {
                "province_name": province_name,
                "tourism_year": int(tourism_year),
                "provincial_overnight_stays": float(tourism_value),
            }
        )

    if not rows:
        return empty
    return pd.DataFrame(rows).drop_duplicates(subset=["province_name"]).reset_index(drop=True)


def build_geoportal_gasolineras_baseline(
    roads_gdf: gpd.GeoDataFrame,
    source_path: Path,
    matched_output_path: Optional[Path] = None,
    max_distance_km: float = 8.0,
) -> pd.DataFrame:
    if source_path.suffix.lower() == ".json":
        stations = parse_geoportal_gasolineras_json(source_path)
    else:
        stations = parse_geoportal_gasolineras_xls(source_path)
    matched = spatially_match_chargers_to_roads(stations, roads_gdf, max_distance_km=max_distance_km)
    if matched_output_path is not None:
        matched_output_path.parent.mkdir(parents=True, exist_ok=True)
        matched.to_csv(matched_output_path, index=False)
    return matched


def _monthly_ev_count_from_remote_parquet(year: int, month: int) -> int:
    url = f"{DATOS_GOB_BASE}{year}_{month:02d}.parquet"
    df = pd.read_parquet(
        url,
        columns=["FEC_MATRICULA", "COD_TIPO", "COD_PROPULSION_ITV", "CLAVE_TRAMITE"],
    )
    df["ANO"] = df["FEC_MATRICULA"].str[-4:].astype("Int64").fillna(0)
    filtered = df[
        (df["ANO"] >= 2015)
        & (df["COD_TIPO"] == "40")
        & (df["CLAVE_TRAMITE"].isin(["1", "5", "B"]))
        & (df["COD_PROPULSION_ITV"] == "2")
    ]
    return int(len(filtered))


def build_ev_projection_from_official_repo(
    monthly_output_path: Path,
    projection_output_path: Path,
    target_year: int = 2027,
) -> Dict[str, object]:
    monthly_rows: List[Dict[str, object]] = []
    for year in range(2015, 2024):
        for month in range(1, 13):
            monthly_rows.append(
                {
                    "date": pd.Timestamp(year=year, month=month, day=1),
                    "electric_turismo_registrations": _monthly_ev_count_from_remote_parquet(year, month),
                }
            )

    monthly_df = pd.DataFrame(monthly_rows).sort_values("date").reset_index(drop=True)
    series = monthly_df.set_index("date")["electric_turismo_registrations"]

    model = sm.tsa.statespace.SARIMAX(
        np.log(series),
        order=(1, 0, 2),
        seasonal_order=(1, 0, 1, 12),
    )
    fitted = model.fit(disp=False)

    horizon_months = max(12, (target_year - 2023) * 12)
    forecast = fitted.get_forecast(horizon_months).summary_frame(alpha=0.5)
    forecast["forecast_mean"] = np.exp(forecast["mean"])
    forecast["forecast_ci_lower"] = np.exp(forecast["mean_ci_lower"])
    forecast["forecast_ci_upper"] = np.exp(forecast["mean_ci_upper"])
    forecast["date"] = pd.date_range("2024-01-01", periods=horizon_months, freq="MS")
    forecast["year"] = forecast["date"].dt.year
    forecast["month"] = forecast["date"].dt.month

    historical_stock = int(round(monthly_df["electric_turismo_registrations"].sum()))
    projected_registrations_target_year = int(round(forecast.loc[forecast["year"] == target_year, "forecast_mean"].sum()))
    projected_total = historical_stock + int(
        round(forecast.loc[forecast["year"] <= target_year, "forecast_mean"].sum())
    )
    monthly_output_path.parent.mkdir(parents=True, exist_ok=True)
    projection_output_path.parent.mkdir(parents=True, exist_ok=True)
    monthly_df.to_csv(monthly_output_path, index=False)
    pd.DataFrame(
        [
            {
                "target_year": target_year,
                "total_ev_projected_2027": projected_total,
                "projected_new_registrations_target_year": projected_registrations_target_year,
                "historical_registrations_through_2023": historical_stock,
                "method": (
                    "Cumulative EV stock proxy built from official datos.gob.es EV turismo matriculations "
                    "plus SARIMA(1,0,2)(1,0,1,12) forecasted registrations through the target year"
                ),
                "source_repo": "Admindatosgobes/Laboratorio-de-Datos",
            }
        ]
    ).to_csv(projection_output_path, index=False)

    return {
        "monthly_history": monthly_df,
        "forecast_monthly": forecast[
            ["date", "year", "month", "forecast_mean", "forecast_ci_lower", "forecast_ci_upper"]
        ].copy(),
        "projected_total": projected_total,
    }


def build_ev_projection_from_monthly_history(
    monthly_history_path: Path,
    projection_output_path: Path,
    target_year: int = 2027,
) -> Dict[str, object]:
    monthly_df = pd.read_csv(monthly_history_path, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    series = monthly_df.set_index("date")["electric_turismo_registrations"]

    model = sm.tsa.statespace.SARIMAX(
        np.log(series),
        order=(1, 0, 2),
        seasonal_order=(1, 0, 1, 12),
    )
    fitted = model.fit(disp=False)

    last_year = int(monthly_df["date"].dt.year.max())
    horizon_months = max(12, (target_year - last_year) * 12)
    forecast = fitted.get_forecast(horizon_months).summary_frame(alpha=0.5)
    forecast["forecast_mean"] = np.exp(forecast["mean"])
    forecast["forecast_ci_lower"] = np.exp(forecast["mean_ci_lower"])
    forecast["forecast_ci_upper"] = np.exp(forecast["mean_ci_upper"])
    forecast["date"] = pd.date_range(f"{last_year + 1}-01-01", periods=horizon_months, freq="MS")
    forecast["year"] = forecast["date"].dt.year
    forecast["month"] = forecast["date"].dt.month

    historical_stock = int(round(monthly_df["electric_turismo_registrations"].sum()))
    projected_registrations_target_year = int(round(forecast.loc[forecast["year"] == target_year, "forecast_mean"].sum()))
    projected_total = historical_stock + int(
        round(forecast.loc[forecast["year"] <= target_year, "forecast_mean"].sum())
    )

    pd.DataFrame(
        [
            {
                "target_year": target_year,
                "total_ev_projected_2027": projected_total,
                "projected_new_registrations_target_year": projected_registrations_target_year,
                "historical_registrations_through_2023": historical_stock,
                "method": (
                    "Cumulative EV stock proxy built from cached official datos.gob.es EV turismo "
                    "matriculations plus SARIMA(1,0,2)(1,0,1,12) forecasted registrations through the target year"
                ),
                "source_repo": "Admindatosgobes/Laboratorio-de-Datos",
            }
        ]
    ).to_csv(projection_output_path, index=False)

    return {
        "monthly_history": monthly_df,
        "forecast_monthly": forecast[
            ["date", "year", "month", "forecast_mean", "forecast_ci_lower", "forecast_ci_upper"]
        ].copy(),
        "projected_total": projected_total,
    }


def _coerce_number_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9\.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _read_delimited_or_excel(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    sample = path.read_text(encoding="latin-1", errors="ignore").splitlines()[0]
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","
    last_error = None
    for encoding in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
        try:
            return pd.read_csv(path, sep=delimiter, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
    assert last_error is not None
    raise last_error


def standardize_grid_capacity_file(path: Path, distributor_network: str) -> pd.DataFrame:
    raw = _read_delimited_or_excel(path)
    columns_normalized = {col: re.sub(r"\s+", " ", str(col)).strip().lower() for col in raw.columns}
    raw = raw.rename(columns=columns_normalized)

    node_column = None
    for candidate in ["nombre subestación", "subestación", "subestacion", "nudo", "node", "nombre nudo"]:
        if candidate in raw.columns:
            node_column = candidate
            break

    x_column = None
    for candidate in ["coordenada utm x", "utm x", "x", "longitude", "longitud"]:
        if candidate in raw.columns:
            x_column = candidate
            break

    y_column = None
    for candidate in ["coordenada utm y", "utm y", "y", "latitude", "latitud"]:
        if candidate in raw.columns:
            y_column = candidate
            break

    capacity_column = None
    for candidate in [
        "capacidad firme disponible (mw)",
        "capacidad disponible (mw)",
        "capacidad disponible",
        "capacidad firme disponible",
    ]:
        if candidate in raw.columns:
            capacity_column = candidate
            break

    voltage_column = None
    for candidate in ["nivel de tensión (kv)", "nivel de tension (kv)", "tensión", "tension", "kv"]:
        if candidate in raw.columns:
            voltage_column = candidate
            break

    if node_column is None or x_column is None or y_column is None or capacity_column is None:
        return pd.DataFrame(
            columns=[
                "node_name",
                "latitude",
                "longitude",
                "capacity_available_mw",
                "voltage_kv",
                "distributor_network",
            ]
        )

    parsed = raw[[node_column, x_column, y_column, capacity_column] + ([voltage_column] if voltage_column else [])].copy()
    parsed = parsed.rename(
        columns={
            node_column: "node_name",
            x_column: "x_coord",
            y_column: "y_coord",
            capacity_column: "capacity_available_mw",
            voltage_column: "voltage_kv" if voltage_column else None,
        }
    )
    parsed["x_coord"] = _coerce_number_series(parsed["x_coord"])
    parsed["y_coord"] = _coerce_number_series(parsed["y_coord"])
    parsed["capacity_available_mw"] = _coerce_number_series(parsed["capacity_available_mw"]).fillna(0.0)
    if "voltage_kv" in parsed.columns:
        parsed["voltage_kv"] = _coerce_number_series(parsed["voltage_kv"])
    else:
        parsed["voltage_kv"] = np.nan

    parsed = parsed.dropna(subset=["x_coord", "y_coord", "node_name"])
    if parsed.empty:
        return pd.DataFrame(
            columns=[
                "node_name",
                "latitude",
                "longitude",
                "capacity_available_mw",
                "voltage_kv",
                "distributor_network",
            ]
        )

    looks_like_utm = parsed["x_coord"].between(100000, 1000000).all() and parsed["y_coord"].between(3000000, 5000000).all()
    grid_gdf = gpd.GeoDataFrame(
        parsed,
        geometry=gpd.points_from_xy(parsed["x_coord"], parsed["y_coord"]),
        crs="EPSG:25830" if looks_like_utm else "EPSG:4326",
    ).to_crs("EPSG:4326")

    grid_gdf["latitude"] = grid_gdf.geometry.y
    grid_gdf["longitude"] = grid_gdf.geometry.x
    grid_gdf["distributor_network"] = distributor_network
    return pd.DataFrame(
        grid_gdf[
            [
                "node_name",
                "latitude",
                "longitude",
                "capacity_available_mw",
                "voltage_kv",
                "distributor_network",
            ]
        ]
    ).drop_duplicates()


def load_grid_capacity_bundle(external_dir: Path) -> pd.DataFrame:
    bundle: List[pd.DataFrame] = []
    patterns = [
        ("i-DE", ["ide*", "*R1-001*Demanda*", "*r1-001*demanda*"]),
        ("Endesa", ["edistribucion*", "*R1-299*Demanda*", "*r1-299*demanda*"]),
        ("Viesgo", ["viesgo*", "*R1005*demanda*", "*R1-005*Demanda*", "*r1-005*demanda*"]),
    ]
    seen_paths = set()
    for distributor, distributor_patterns in patterns:
        for pattern in distributor_patterns:
            for path in sorted(external_dir.glob(pattern)):
                if path.is_dir() or path in seen_paths:
                    continue
                seen_paths.add(path)
                standardized = standardize_grid_capacity_file(path, distributor)
                if not standardized.empty:
                    bundle.append(standardized)
    if not bundle:
        return pd.DataFrame(
            columns=[
                "node_name",
                "latitude",
                "longitude",
                "capacity_available_mw",
                "voltage_kv",
                "distributor_network",
            ]
        )
    return pd.concat(bundle, ignore_index=True).drop_duplicates()


def assign_proxy_distributor(latitude: float, longitude: float) -> str:
    if latitude >= 43.0 and -5.3 <= longitude <= -2.4:
        return "Viesgo"
    if longitude >= 0.5 or latitude <= 38.4:
        return "Endesa"
    return "i-DE"


def _haversine_km(lat1: float, lon1: float, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    r = 6371.0
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = np.radians(lat2)
    lon2_rad = np.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def enrich_stations_with_grid(
    file_2: pd.DataFrame,
    grid_nodes: pd.DataFrame,
    charger_power_kw: int,
    max_match_distance_km: float = 35.0,
) -> pd.DataFrame:
    stations = file_2.copy()
    if stations.empty:
        stations["distributor_network"] = []
        stations["grid_status"] = []
        return stations

    estimated_demand_kw = stations["n_chargers_proposed"].astype(float) * float(charger_power_kw)
    stations["estimated_demand_kw"] = estimated_demand_kw

    if grid_nodes is None or grid_nodes.empty:
        stations["distributor_network"] = stations.apply(
            lambda row: assign_proxy_distributor(float(row["latitude"]), float(row["longitude"])),
            axis=1,
        )
        return stations

    node_lats = grid_nodes["latitude"].to_numpy(dtype=float)
    node_lons = grid_nodes["longitude"].to_numpy(dtype=float)
    node_caps_kw = grid_nodes["capacity_available_mw"].to_numpy(dtype=float) * 1000.0

    distributors: List[str] = []
    statuses: List[str] = []
    node_names: List[Optional[str]] = []
    capacity_available_kw: List[Optional[float]] = []
    distance_matches: List[Optional[float]] = []

    for station in stations.itertuples(index=False):
        distances = _haversine_km(float(station.latitude), float(station.longitude), node_lats, node_lons)
        nearest_idx = int(np.argmin(distances))
        nearest_distance = float(distances[nearest_idx])
        nearest_capacity = float(node_caps_kw[nearest_idx])
        nearest_distributor = str(grid_nodes.iloc[nearest_idx]["distributor_network"])
        nearest_node_name = str(grid_nodes.iloc[nearest_idx]["node_name"])

        if nearest_distance > max_match_distance_km:
            distributors.append(assign_proxy_distributor(float(station.latitude), float(station.longitude)))
            statuses.append(station.grid_status)
            node_names.append(None)
            capacity_available_kw.append(None)
            distance_matches.append(None)
            continue

        demand_kw = float(station.n_chargers_proposed) * float(charger_power_kw)
        if nearest_capacity < demand_kw:
            grid_status = "Congested"
        elif nearest_capacity < demand_kw * 2:
            grid_status = "Moderate"
        else:
            grid_status = "Sufficient"

        distributors.append(nearest_distributor)
        statuses.append(grid_status)
        node_names.append(nearest_node_name)
        capacity_available_kw.append(round(nearest_capacity, 2))
        distance_matches.append(round(nearest_distance, 3))

    stations["distributor_network"] = distributors
    stations["grid_status"] = statuses
    stations["nearest_grid_node"] = node_names
    stations["available_capacity_kw"] = capacity_available_kw
    stations["distance_to_grid_node_km"] = distance_matches
    return stations


def try_build_official_external_inputs(
    roads_gdf: gpd.GeoDataFrame,
    external_dir: Path,
    target_year: int = 2027,
) -> Dict[str, object]:
    external_dir.mkdir(parents=True, exist_ok=True)

    baseline_summary_path = external_dir / "existing_interurban_stations_by_route.csv"
    baseline_matched_path = external_dir / "existing_interurban_stations_matched.csv"
    baseline_scalar_path = external_dir / "existing_interurban_stations.csv"
    nap_xml_path = external_dir / "nap_charging_points.xml"
    traffic_geojson_path = external_dir / "mitma_traffic_segments.geojson"
    traffic_summary_path = external_dir / "mitma_traffic_by_route.csv"
    gasolineras_json_path = external_dir / "miterd_estaciones_terrestres.json"
    gasolineras_xls_path = external_dir / "geoportal_gasolineras_eess.xls"
    gasolineras_matched_path = external_dir / "geoportal_gasolineras_matched.csv"
    ine_population_json_path = external_dir / "ine_municipal_population_2025.json"
    ine_population_csv_path = external_dir / "ine_municipal_population_2025.csv"
    ine_hotel_overnight_json_path = external_dir / "ine_hotel_overnight_stays.json"
    ine_provincial_overnight_json_path = external_dir / "ine_provincial_overnight_stays.json"
    ine_provincial_overnight_csv_path = external_dir / "ine_provincial_overnight_stays.csv"

    if not baseline_summary_path.exists() or not baseline_scalar_path.exists():
        if not nap_xml_path.exists():
            download_file(DEFAULT_NAP_XML_URL, nap_xml_path)
        baseline_result = build_charging_baseline_from_nap(
            roads_gdf,
            nap_xml_path,
            matched_output_path=baseline_matched_path,
            summary_output_path=baseline_summary_path,
        )
        pd.DataFrame(
            [
                {
                    "total_existing_stations_baseline": baseline_result["total_existing_stations_baseline"],
                    "source": "NAP-DGT/MITERD DATEX II spatially matched to RTIG",
                }
            ]
        ).to_csv(baseline_scalar_path, index=False)

    projection_path = external_dir / "ev_projection_2027.csv"
    monthly_ev_path = external_dir / "ev_monthly_counts_official.csv"
    projection_is_stock_style = False
    if projection_path.exists():
        existing_projection = pd.read_csv(projection_path)
        if not existing_projection.empty and "method" in existing_projection.columns:
            projection_is_stock_style = "stock proxy" in str(existing_projection["method"].iloc[0]).lower()

    if monthly_ev_path.exists() and (not projection_path.exists() or not projection_is_stock_style):
        build_ev_projection_from_monthly_history(monthly_ev_path, projection_path, target_year=target_year)
    elif not projection_path.exists():
        build_ev_projection_from_official_repo(monthly_ev_path, projection_path, target_year=target_year)

    edistrib_path = external_dir / "edistribucion_capacity_2026_03.csv"
    if not edistrib_path.exists():
        download_file(DEFAULT_EDISTRIBUCION_URL, edistrib_path)

    ide_path = external_dir / "ide_capacity_2026_02.csv"
    if not ide_path.exists():
        try:
            download_file(DEFAULT_IDE_URL, ide_path)
        except (requests.RequestException, ValueError):
            pass

    if not traffic_summary_path.exists():
        if not traffic_geojson_path.exists():
            try:
                downloaded_traffic_path = download_arcgis_geojson_paginated(
                    DEFAULT_MITMA_TRAFFIC_URL,
                    traffic_geojson_path,
                    out_fields=["ID", "Nombre", "Longitud", "IMD_total", "IMD_pesado"],
                )
                traffic_geojson_path = downloaded_traffic_path
            except (requests.RequestException, ValueError, KeyError, json.JSONDecodeError):
                pass
        if traffic_geojson_path.exists():
            build_mitma_traffic_inputs(
                roads_gdf,
                traffic_geojson_path,
                traffic_summary_path,
            )

    if not gasolineras_json_path.exists() and not gasolineras_matched_path.exists():
        try:
            download_file(DEFAULT_GASOLINERAS_JSON_URL, gasolineras_json_path)
        except (requests.RequestException, ValueError):
            pass

    if not gasolineras_matched_path.exists():
        try:
            if gasolineras_json_path.exists():
                build_geoportal_gasolineras_baseline(
                    roads_gdf,
                    gasolineras_json_path,
                    matched_output_path=gasolineras_matched_path,
                    max_distance_km=8.0,
                )
            elif gasolineras_xls_path.exists():
                build_geoportal_gasolineras_baseline(
                    roads_gdf,
                    gasolineras_xls_path,
                    matched_output_path=gasolineras_matched_path,
                    max_distance_km=8.0,
                )
        except (ValueError, ImportError):
            pass

    if not ine_population_json_path.exists() and not ine_population_csv_path.exists():
        try:
            download_file(DEFAULT_INE_MUNICIPAL_POP_URL, ine_population_json_path)
        except (requests.RequestException, ValueError):
            pass
    if ine_population_json_path.exists() and not ine_population_csv_path.exists():
        try:
            ine_population = parse_ine_municipal_population_json(ine_population_json_path, target_year=2025)
            ine_population_csv_path.parent.mkdir(parents=True, exist_ok=True)
            ine_population.to_csv(ine_population_csv_path, index=False)
        except json.JSONDecodeError:
            pass

    if not ine_hotel_overnight_json_path.exists():
        try:
            download_file(DEFAULT_INE_HOTEL_OVERNIGHT_URL, ine_hotel_overnight_json_path)
        except (requests.RequestException, ValueError):
            pass
    if not ine_provincial_overnight_json_path.exists() and not ine_provincial_overnight_csv_path.exists():
        try:
            download_file(DEFAULT_INE_PROVINCIAL_OVERNIGHT_URL, ine_provincial_overnight_json_path)
        except (requests.RequestException, ValueError):
            pass
    if ine_provincial_overnight_json_path.exists() and not ine_provincial_overnight_csv_path.exists():
        try:
            tourism = parse_ine_provincial_overnight_stays_json(ine_provincial_overnight_json_path)
            ine_provincial_overnight_csv_path.parent.mkdir(parents=True, exist_ok=True)
            tourism.to_csv(ine_provincial_overnight_csv_path, index=False)
        except json.JSONDecodeError:
            pass

    return {
        "baseline_summary_path": baseline_summary_path,
        "baseline_scalar_path": baseline_scalar_path,
        "ev_projection_path": projection_path,
        "traffic_summary_path": traffic_summary_path,
        "gasolineras_json_path": gasolineras_json_path,
        "gasolineras_matched_path": gasolineras_matched_path,
        "ine_population_json_path": ine_population_json_path,
        "ine_population_csv_path": ine_population_csv_path,
        "ine_hotel_overnight_json_path": ine_hotel_overnight_json_path,
        "ine_provincial_overnight_json_path": ine_provincial_overnight_json_path,
        "ine_provincial_overnight_csv_path": ine_provincial_overnight_csv_path,
        "grid_capacity_paths": [path for path in [edistrib_path, ide_path] if path.exists()],
    }
