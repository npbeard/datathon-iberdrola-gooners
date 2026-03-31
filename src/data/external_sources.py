from __future__ import annotations

import math
import re
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

    projected_total = int(round(forecast.loc[forecast["year"] == target_year, "forecast_mean"].sum()))
    monthly_output_path.parent.mkdir(parents=True, exist_ok=True)
    projection_output_path.parent.mkdir(parents=True, exist_ok=True)
    monthly_df.to_csv(monthly_output_path, index=False)
    pd.DataFrame(
        [
            {
                "target_year": target_year,
                "total_ev_projected_2027": projected_total,
                "method": "SARIMA(1,0,2)(1,0,1,12) on official datos.gob.es EV turismo matriculations",
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
    if not projection_path.exists():
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

    return {
        "baseline_summary_path": baseline_summary_path,
        "baseline_scalar_path": baseline_scalar_path,
        "ev_projection_path": projection_path,
        "grid_capacity_paths": [path for path in [edistrib_path, ide_path] if path.exists()],
    }
