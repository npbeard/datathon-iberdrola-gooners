from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point

import src.data.external_sources as es


def sample_roads():
    return gpd.GeoDataFrame(
        {
            "carretera": ["A-1", "AP-7"],
            "geometry": [
                LineString([(-3.8, 40.4), (-3.2, 40.8)]),
                LineString([(0.6, 41.3), (1.8, 42.0)]),
            ],
        },
        crs="EPSG:4326",
    )


def test_download_file_and_parse_nap_xml(tmp_path, requests_mock):
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
      xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
      xmlns:locx="http://datex2.eu/schema/3/locationExtension"
      xmlns:com="http://datex2.eu/schema/3/common">
      <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
        <egi:energyInfrastructureSite id="SITE_1" version="">
          <fac:name><com:values><com:value lang="es">Station One</com:value></com:values></fac:name>
          <fac:locationReference>
            <loc:_locationReferenceExtension>
              <loc:facilityLocation>
                <locx:address>
                  <locx:addressLine order="1"><locx:text><com:values><com:value lang="es">Municipio: Madrid</com:value></com:values></locx:text></locx:addressLine>
                </locx:address>
              </loc:facilityLocation>
            </loc:_locationReferenceExtension>
            <loc:coordinatesForDisplay>
              <loc:latitude>40.5</loc:latitude>
              <loc:longitude>-3.6</loc:longitude>
            </loc:coordinatesForDisplay>
          </fac:locationReference>
          <fac:operator id="OP_1" version="">
            <fac:name><com:values><com:value lang="es">Operator One</com:value></com:values></fac:name>
          </fac:operator>
          <egi:energyInfrastructureStation id="SITE_1_1" version="">
            <egi:refillPoint id="POINT_1" version="">
              <egi:connector>
                <egi:connectorType>iec62196T2COMBO</egi:connectorType>
                <egi:maxPowerAtSocket>150000.0</egi:maxPowerAtSocket>
              </egi:connector>
            </egi:refillPoint>
          </egi:energyInfrastructureStation>
        </egi:energyInfrastructureSite>
      </egi:energyInfrastructureTable>
    </d2:payload>
    """
    requests_mock.get("https://example.com/nap.xml", content=xml_content.encode("utf-8"))
    output = es.download_file("https://example.com/nap.xml", tmp_path / "nap.xml")
    parsed = es.parse_nap_charging_xml(output)

    assert output.exists()
    assert parsed.loc[0, "site_id"] == "SITE_1"
    assert parsed.loc[0, "operator_name"] == "Operator One"
    assert parsed.loc[0, "connector_count"] == 1
    assert round(parsed.loc[0, "max_power_kw"], 2) == 150.0

    requests_mock.get("https://example.com/unavailable", content=b"<!DOCTYPE html><title>Service unavailable</title>")
    try:
        es.download_file("https://example.com/unavailable", tmp_path / "bad.html")
    except ValueError:
        pass
    else:
        raise AssertionError("Expected ValueError for HTML service unavailable page")


def test_spatial_matching_baseline_and_route_enrichment(tmp_path):
    chargers = pd.DataFrame(
        [
            {"site_id": "S1", "site_name": "One", "latitude": 40.5, "longitude": -3.5},
            {"site_id": "S2", "site_name": "Two", "latitude": 41.45, "longitude": 0.9},
            {"site_id": "S3", "site_name": "Far", "latitude": 36.0, "longitude": -8.0},
        ]
    )
    roads = sample_roads()
    matched = es.spatially_match_chargers_to_roads(chargers, roads, max_distance_km=10)
    by_route, total_existing = es.summarize_interurban_baseline(matched)
    route_summary = pd.DataFrame(
        [
            {"carretera": "A-1", "route_score": 100.0},
            {"carretera": "AP-7", "route_score": 90.0},
        ]
    )
    enriched = es.enrich_route_summary_with_baseline(route_summary, by_route)

    assert total_existing == 1
    assert set(by_route["carretera"]) == {"AP-7"}
    assert "coverage_gap_score" in enriched.columns

    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
    <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
      xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
      xmlns:fac="http://datex2.eu/schema/3/facilities"
      xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
      xmlns:com="http://datex2.eu/schema/3/common">
      <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
        <egi:energyInfrastructureSite id="SITE_1" version="">
          <fac:name><com:values><com:value lang="es">Station One</com:value></com:values></fac:name>
          <fac:locationReference>
            <loc:coordinatesForDisplay>
              <loc:latitude>41.45</loc:latitude>
              <loc:longitude>0.9</loc:longitude>
            </loc:coordinatesForDisplay>
          </fac:locationReference>
        </egi:energyInfrastructureSite>
      </egi:energyInfrastructureTable>
    </d2:payload>
    """
    xml_path = tmp_path / "nap.xml"
    xml_path.write_text(xml_content, encoding="utf-8")
    built = es.build_charging_baseline_from_nap(
        roads,
        xml_path,
        matched_output_path=tmp_path / "matched.csv",
        summary_output_path=tmp_path / "summary.csv",
        max_distance_km=10,
    )
    assert built["total_existing_stations_baseline"] == 1
    assert (tmp_path / "matched.csv").exists()
    assert (tmp_path / "summary.csv").exists()

    traffic = gpd.GeoDataFrame(
        {
            "Nombre": ["A-1 tramo", "AP-7 tramo"],
            "Longitud": [10.0, 12.0],
            "IMD_total": [18000.0, 25000.0],
            "IMD_pesado": [1800.0, 5000.0],
            "geometry": [
                LineString([(-3.81, 40.39), (-3.19, 40.81)]),
                LineString([(0.61, 41.31), (1.81, 42.01)]),
            ],
        },
        crs="EPSG:4326",
    )
    traffic_path = tmp_path / "traffic.geojson"
    traffic.to_file(traffic_path, driver="GeoJSON")
    loaded_traffic = es.load_mitma_traffic_segments(traffic_path)
    traffic_by_route = es.summarize_traffic_by_route(roads, loaded_traffic, max_distance_km=10)
    enriched_with_traffic = es.enrich_route_summary_with_traffic(route_summary, traffic_by_route)
    assert "traffic_imd_total" in enriched_with_traffic.columns
    assert enriched_with_traffic["traffic_imd_total"].max() > 0
    built_traffic = es.build_mitma_traffic_inputs(roads, traffic_path, tmp_path / "traffic_by_route.csv", max_distance_km=10)
    assert (tmp_path / "traffic_by_route.csv").exists()
    assert not built_traffic["by_route"].empty

    gas_xls = tmp_path / "gas.xls"
    gas_xls.write_bytes(b"xls")
    original_read_excel = pd.read_excel

    def fake_read_excel(path, header=0, *args, **kwargs):
        if Path(path) == gas_xls:
            return pd.DataFrame(
                [
                    {
                        "Provincia": "Madrid",
                        "Municipio": "Madrid",
                        "Localidad": "Madrid",
                        "Código postal": "28001",
                        "Dirección": "A-1 km 12",
                        "Margen": "D",
                        "Longitud": -3.5,
                        "Latitud": 40.5,
                        "Rótulo": "REPSOL",
                        "Tipo venta": "P",
                        "Rem.": "dm",
                        "Horario": "L-D: 24H",
                        "Tipo servicio": "L-D: 24H (A)",
                    }
                ]
            )
        return original_read_excel(path, header=header, *args, **kwargs)

    pd.read_excel = fake_read_excel
    try:
        parsed_gas = es.parse_geoportal_gasolineras_xls(gas_xls)
        assert parsed_gas.loc[0, "source_dataset"] == "Geoportal Gasolineras"
        matched_gas = es.build_geoportal_gasolineras_baseline(
            roads,
            gas_xls,
            matched_output_path=tmp_path / "geoportal_matched.csv",
            max_distance_km=10,
        )
        assert (tmp_path / "geoportal_matched.csv").exists()
        assert not matched_gas.empty
    finally:
        pd.read_excel = original_read_excel

    gas_json = tmp_path / "gas.json"
    gas_json.write_text(
        """
        {
          "Fecha": "15/04/2026 09:40:57",
          "ListaEESSPrecio": [
            {
              "Provincia": "Madrid",
              "Municipio": "Madrid",
              "Localidad": "Madrid",
              "C.P.": "28001",
              "Dirección": "A-1 km 12",
              "Margen": "D",
              "Longitud (WGS84)": "-3,500000",
              "Latitud": "40,500000",
              "Rótulo": "REPSOL",
              "Tipo Venta": "P",
              "Remisión": "dm",
              "Horario": "L-D: 24H"
            }
          ],
          "ResultadoConsulta": "OK"
        }
        """,
        encoding="utf-8",
    )
    parsed_json = es.parse_geoportal_gasolineras_json(gas_json)
    assert parsed_json.loc[0, "source_dataset"] == "MITERD REST Carburantes"
    matched_json = es.build_geoportal_gasolineras_baseline(
        roads,
        gas_json,
        matched_output_path=tmp_path / "geoportal_json_matched.csv",
        max_distance_km=10,
    )
    assert (tmp_path / "geoportal_json_matched.csv").exists()
    assert not matched_json.empty
    empty_gas_json = tmp_path / "gas_empty.json"
    empty_gas_json.write_text('{"ListaEESSPrecio": []}', encoding="utf-8")
    assert es.parse_geoportal_gasolineras_json(empty_gas_json).empty


def test_build_ev_projection_from_official_repo(monkeypatch, tmp_path):
    def fake_read_parquet(url, columns):
        name = url.rsplit("/", 1)[-1]
        year = int(name.split("_")[0])
        month = int(name.split("_")[1].split(".")[0])
        size = year - 2014 + month
        return pd.DataFrame(
            {
                "FEC_MATRICULA": [f"01{month:02d}{year}"] * size,
                "COD_TIPO": ["40"] * size,
                "COD_PROPULSION_ITV": ["2"] * size,
                "CLAVE_TRAMITE": ["1"] * size,
            }
        )

    class FakeForecast:
        def summary_frame(self, alpha=0.5):
            return pd.DataFrame(
                {
                    "mean": np.log(np.linspace(1000, 4700, 48)),
                    "mean_ci_lower": np.log(np.linspace(900, 4600, 48)),
                    "mean_ci_upper": np.log(np.linspace(1100, 4800, 48)),
                }
            )

    class FakeFitted:
        def get_forecast(self, horizon):
            assert horizon == 48
            return FakeForecast()

    class FakeSARIMAX:
        def __init__(self, series, order, seasonal_order):
            assert order == (1, 0, 2)
            assert seasonal_order == (1, 0, 1, 12)

        def fit(self, disp=False):
            return FakeFitted()

    monkeypatch.setattr(pd, "read_parquet", fake_read_parquet)
    monkeypatch.setattr(es.sm.tsa.statespace, "SARIMAX", FakeSARIMAX)

    result = es.build_ev_projection_from_official_repo(
        tmp_path / "ev_monthly_counts_official.csv",
        tmp_path / "ev_projection_2027.csv",
        target_year=2027,
    )

    assert result["projected_total"] > 0
    assert (tmp_path / "ev_monthly_counts_official.csv").exists()
    saved = pd.read_csv(tmp_path / "ev_projection_2027.csv")
    assert int(saved["total_ev_projected_2027"].iloc[0]) == result["projected_total"]


def test_grid_capacity_standardization_and_bundle(tmp_path):
    csv_path = tmp_path / "edistribucion_capacity.csv"
    csv_path.write_text(
        "\ufeffGestor de red;Coordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
        "R1-299;535771,67;4074262,07;AGUADULC;66;12,5\n",
        encoding="utf-8",
    )
    parsed = es.standardize_grid_capacity_file(csv_path, "Endesa")
    assert parsed.loc[0, "node_name"] == "AGUADULC"
    assert parsed.loc[0, "distributor_network"] == "Endesa"
    assert parsed.loc[0, "capacity_available_mw"] == 12.5
    assert 36 <= parsed.loc[0, "latitude"] <= 44

    invalid_path = tmp_path / "ide_invalid.csv"
    invalid_path.write_text("only,one,column\n1\n", encoding="utf-8")
    invalid = es.standardize_grid_capacity_file(invalid_path, "i-DE")
    assert invalid.empty

    viesgo_dir = tmp_path / "viesgo-folder"
    viesgo_dir.mkdir()
    viesgo_file = tmp_path / "2026_03_05_R1005_demanda.csv"
    viesgo_file.write_text(
        "Gestor de red;Provincia;Municipio;Coordenada UTM X;Coordenada UTM Y;Subestación ;Nivel de tensión (kV);Capacidad firme disponible (MW);Nombre subestación\n"
        "R1-005;Asturias;Valdes;215.356,7;4.825.840,4;101;20;8,97;ALMUNA\n",
        encoding="latin-1",
    )
    bundle = es.load_grid_capacity_bundle(tmp_path)
    assert not bundle.empty
    assert "Viesgo" in set(bundle["distributor_network"])

    utf_path = tmp_path / "grid_utf.csv"
    utf_path.write_text("node,longitude,latitude,capacity available (mw)\n", encoding="utf-8")
    try:
        es._read_delimited_or_excel(tmp_path / "missing.csv")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Expected FileNotFoundError for missing file")


def test_grid_matching_and_try_build_inputs(tmp_path, monkeypatch):
    stations = pd.DataFrame(
        [
            {
                "location_id": "IBE_001",
                "latitude": 40.5,
                "longitude": -3.5,
                "route_segment": "A-1",
                "n_chargers_proposed": 4,
                "grid_status": "Moderate",
            },
            {
                "location_id": "IBE_002",
                "latitude": 41.5,
                "longitude": 1.2,
                "route_segment": "AP-7",
                "n_chargers_proposed": 8,
                "grid_status": "Moderate",
            },
            {
                "location_id": "IBE_003",
                "latitude": 35.0,
                "longitude": -9.0,
                "route_segment": "A-9",
                "n_chargers_proposed": 4,
                "grid_status": "Moderate",
            },
        ]
    )
    nodes = pd.DataFrame(
        [
            {"node_name": "Node 1", "latitude": 40.51, "longitude": -3.49, "capacity_available_mw": 2.0, "voltage_kv": 66, "distributor_network": "i-DE"},
            {"node_name": "Node 2", "latitude": 41.51, "longitude": 1.21, "capacity_available_mw": 0.5, "voltage_kv": 66, "distributor_network": "Endesa"},
        ]
    )
    enriched = es.enrich_stations_with_grid(stations, nodes, charger_power_kw=150, max_match_distance_km=10)
    assert enriched.loc[0, "grid_status"] == "Sufficient"
    assert enriched.loc[1, "grid_status"] == "Congested"
    assert enriched.loc[2, "distributor_network"] == "Endesa"


def test_additional_external_source_branches(tmp_path, monkeypatch):
    route_summary = pd.DataFrame([{"carretera": "A-1", "route_score": 10.0}])
    enriched = es.enrich_route_summary_with_baseline(route_summary, pd.DataFrame())
    assert enriched.loc[0, "existing_station_count"] == 0

    monthly_history_path = tmp_path / "ev_monthly_counts_official.csv"
    pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=24, freq="MS"),
            "electric_turismo_registrations": list(range(100, 124)),
        }
    ).to_csv(monthly_history_path, index=False)

    class FakeForecast:
        def summary_frame(self, alpha=0.5):
            return pd.DataFrame(
                {
                    "mean": np.log(np.linspace(1000, 1500, 72)),
                    "mean_ci_lower": np.log(np.linspace(900, 1400, 72)),
                    "mean_ci_upper": np.log(np.linspace(1100, 1600, 72)),
                }
            )

    class FakeFitted:
        def get_forecast(self, horizon):
            assert horizon == 72
            return FakeForecast()

    class FakeSARIMAX:
        def __init__(self, series, order, seasonal_order):
            assert order == (1, 0, 2)
            assert seasonal_order == (1, 0, 1, 12)

        def fit(self, disp=False):
            return FakeFitted()

    monkeypatch.setattr(es.sm.tsa.statespace, "SARIMAX", FakeSARIMAX)
    projection = es.build_ev_projection_from_monthly_history(
        monthly_history_path,
        tmp_path / "ev_projection_2027.csv",
        target_year=2027,
    )
    saved_projection = pd.read_csv(tmp_path / "ev_projection_2027.csv")
    assert projection["projected_total"] == int(saved_projection["total_ev_projected_2027"].iloc[0])
    assert "stock proxy" in saved_projection["method"].iloc[0].lower()

    external_dir = tmp_path / "external"
    external_dir.mkdir()
    (external_dir / "existing_interurban_stations_by_route.csv").write_text("carretera,existing_station_count\nA-1,1\n", encoding="utf-8")
    (external_dir / "existing_interurban_stations.csv").write_text("total_existing_stations_baseline,source\n3,test\n", encoding="utf-8")
    monthly_history_path.replace(external_dir / "ev_monthly_counts_official.csv")
    pd.DataFrame(
        [
            {
                "target_year": 2027,
                "total_ev_projected_2027": 999,
                "method": "legacy annual registrations",
                "source_repo": "repo",
            }
        ]
    ).to_csv(external_dir / "ev_projection_2027.csv", index=False)

    roads = gpd.GeoDataFrame({"carretera": ["A-1"], "geometry": [Point(-3.5, 40.5)]}, crs="EPSG:4326")
    monkeypatch.setattr(es, "download_file", lambda url, output_path, timeout=180: output_path)
    result = es.try_build_official_external_inputs(roads, external_dir, target_year=2027)
    refreshed = pd.read_csv(external_dir / "ev_projection_2027.csv")
    assert "stock proxy" in refreshed["method"].iloc[0].lower()
    assert result["ev_projection_path"] == external_dir / "ev_projection_2027.csv"

    fallback_stations = pd.DataFrame(
        [
            {
                "location_id": "IBE_001",
                "latitude": 40.5,
                "longitude": -3.5,
                "route_segment": "A-1",
                "n_chargers_proposed": 4,
                "grid_status": "Moderate",
            }
        ]
    )
    fallback = es.enrich_stations_with_grid(fallback_stations, pd.DataFrame(), charger_power_kw=150)
    assert fallback.loc[0, "distributor_network"] == "i-DE"

    roads = sample_roads()

    def fake_download(url, output_path, timeout=180):
        if output_path.suffix == ".xml":
            output_path.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
                <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
                  xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
                  xmlns:fac="http://datex2.eu/schema/3/facilities"
                  xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
                  xmlns:com="http://datex2.eu/schema/3/common">
                  <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
                    <egi:energyInfrastructureSite id="SITE_1" version="">
                      <fac:name><com:values><com:value lang="es">Station One</com:value></com:values></fac:name>
                      <fac:locationReference>
                        <loc:coordinatesForDisplay>
                          <loc:latitude>40.5</loc:latitude>
                          <loc:longitude>-3.5</loc:longitude>
                        </loc:coordinatesForDisplay>
                      </fac:locationReference>
                    </egi:energyInfrastructureSite>
                  </egi:energyInfrastructureTable>
                </d2:payload>
                """,
                encoding="utf-8",
            )
        else:
            output_path.write_text(
                "\ufeffGestor de red;Coordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
                "R1-299;535771,67;4074262,07;AGUADULC;66;12,5\n",
                encoding="utf-8",
            )
        return output_path

    monkeypatch.setattr(es, "download_file", fake_download)
    monkeypatch.setattr(
        es,
        "build_ev_projection_from_official_repo",
        lambda monthly_output_path, projection_output_path, target_year: projection_output_path.write_text(
            "target_year,total_ev_projected_2027\n2027,123456\n",
            encoding="utf-8",
        ),
    )
    result = es.try_build_official_external_inputs(roads, tmp_path, target_year=2027)
    assert result["baseline_summary_path"].exists()
    assert result["ev_projection_path"].exists()
    assert result["grid_capacity_paths"][0].exists()


def test_external_source_branch_helpers(tmp_path, monkeypatch):
    plain = es._strip_namespace("plain")
    assert plain == "plain"

    root = es.ET.fromstring("<root><value> </value><value>Good</value><child>Text</child></root>")
    assert es._find_text(root, "missing") is None
    assert es._find_texts(root, "missing") == []
    assert es._find_text(root, "value") == "Good"
    assert es._find_texts(root, "value") == ["Good"]

    roads = sample_roads().to_crs("EPSG:3857")
    metric = es._roads_for_matching(roads)
    assert str(metric.crs).endswith("3857")

    roads_no_crs = sample_roads().set_crs(None, allow_override=True)
    metric_from_none = es._roads_for_matching(roads_no_crs)
    assert str(metric_from_none.crs).endswith("3857")

    empty_match = es.spatially_match_chargers_to_roads(pd.DataFrame(), sample_roads())
    assert empty_match.empty

    missing_coords = es.spatially_match_chargers_to_roads(
        pd.DataFrame([{"site_id": "S", "latitude": np.nan, "longitude": np.nan}]),
        sample_roads(),
    )
    assert missing_coords.empty

    baseline_empty = es.summarize_interurban_baseline(pd.DataFrame())
    assert baseline_empty[0].empty
    assert baseline_empty[1] == 0
    assert es.load_mitma_traffic_segments(tmp_path / "missing.geojson").empty
    assert es.parse_geoportal_gasolineras_xls(tmp_path / "missing.xls").empty
    assert es.parse_geoportal_gasolineras_json(tmp_path / "missing.json").empty
    assert es.summarize_traffic_by_route(sample_roads(), gpd.GeoDataFrame()).empty
    enriched_no_traffic = es.enrich_route_summary_with_traffic(pd.DataFrame([{"carretera": "A-1"}]), pd.DataFrame())
    assert enriched_no_traffic.loc[0, "traffic_imd_total"] == 0.0
    empty_traffic_path = tmp_path / "empty_traffic.geojson"
    gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs="EPSG:4326").to_file(empty_traffic_path, driver="GeoJSON")
    assert es.load_mitma_traffic_segments(empty_traffic_path).empty

    traffic_no_crs = gpd.GeoDataFrame(
        {"geometry": [LineString([(-3.8, 40.4), (-3.2, 40.8)])], "Nombre": ["A-1"], "Longitud": [10], "IMD_total": [12000], "IMD_pesado": [1200]},
        crs="EPSG:4326",
    ).set_crs(None, allow_override=True)
    traffic_no_crs_path = tmp_path / "traffic_no_crs.geojson"
    traffic_no_crs_path.write_text("{}", encoding="utf-8")
    original_read_file = gpd.read_file
    monkeypatch.setattr(gpd, "read_file", lambda path: traffic_no_crs if path == traffic_no_crs_path else original_read_file(path))
    assert str(es.load_mitma_traffic_segments(traffic_no_crs_path).crs).endswith("4326")

    traffic_missing_cols = gpd.GeoDataFrame({"geometry": [LineString([(-3.8, 40.4), (-3.2, 40.8)])]}, crs="EPSG:4326")
    traffic_missing_cols_path = tmp_path / "traffic_missing_cols.geojson"
    traffic_missing_cols.to_file(traffic_missing_cols_path, driver="GeoJSON")
    loaded_missing_cols = es.load_mitma_traffic_segments(traffic_missing_cols_path)
    assert loaded_missing_cols.loc[0, "traffic_imd_total"] == 0.0

    traffic_3857 = gpd.GeoDataFrame(
        {"geometry": [LineString([(-3.8, 40.4), (-3.2, 40.8)])], "Nombre": ["A-1"], "Longitud": [10], "IMD_total": [12000], "IMD_pesado": [1200]},
        crs="EPSG:4326",
    ).to_crs("EPSG:3857")
    traffic_3857_path = tmp_path / "traffic_3857.geojson"
    traffic_3857_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(gpd, "read_file", lambda path: traffic_3857 if path == traffic_3857_path else traffic_no_crs if path == traffic_no_crs_path else original_read_file(path))
    assert str(es.load_mitma_traffic_segments(traffic_3857_path).crs).endswith("4326")

    far_traffic = gpd.GeoDataFrame(
        {
            "traffic_route_name": ["Far"],
            "traffic_length_km": [10.0],
            "traffic_imd_total": [1000.0],
            "traffic_imd_pesado": [100.0],
            "geometry": [LineString([(-9.0, 36.0), (-8.8, 36.2)])],
        },
        crs="EPSG:4326",
    )
    assert es.summarize_traffic_by_route(sample_roads(), far_traffic, max_distance_km=1.0).empty

    gas_xls = tmp_path / "gas.xls"
    gas_xls.write_bytes(b"xls")
    monkeypatch.setattr(
        pd,
        "read_excel",
        lambda path, header=0, *args, **kwargs: pd.DataFrame([{"Provincia": "Madrid", "Longitud": -3.5, "Latitud": 40.5}])
        if Path(path) == gas_xls
        else pd.DataFrame(),
    )
    parsed_gas = es.parse_geoportal_gasolineras_xls(gas_xls)
    assert parsed_gas.loc[0, "longitude"] == -3.5

    xml_path = tmp_path / "minimal.xml"
    xml_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
          xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
          xmlns:fac="http://datex2.eu/schema/3/facilities"
          xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
          xmlns:com="http://datex2.eu/schema/3/common">
          <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
            <egi:energyInfrastructureSite id="SITE_2" version="">
              <fac:name><com:values><com:value lang="es">Station Two</com:value></com:values></fac:name>
              <fac:locationReference>
                <loc:coordinatesForDisplay>
                  <loc:latitude>40.6</loc:latitude>
                  <loc:longitude>-3.4</loc:longitude>
                </loc:coordinatesForDisplay>
              </fac:locationReference>
              <egi:energyInfrastructureStation id="SITE_2_1" version="">
                <egi:refillPoint id="POINT_2" version="">
                  <egi:connector>
                    <egi:connectorType>iec62196T2COMBO</egi:connectorType>
                    <egi:maxPowerAtSocket>invalid</egi:maxPowerAtSocket>
                  </egi:connector>
                </egi:refillPoint>
              </egi:energyInfrastructureStation>
            </egi:energyInfrastructureSite>
          </egi:energyInfrastructureTable>
        </d2:payload>
        """,
        encoding="utf-8",
    )
    built = es.build_charging_baseline_from_nap(sample_roads(), xml_path)
    assert built["matched"].loc[0, "site_id"] == "SITE_2"

    xlsx_path = tmp_path / "ide.xlsx"
    monkeypatch.setattr(pd, "read_excel", lambda path: pd.DataFrame({"Nombre Nudo": ["A"]}))
    xlsx_loaded = es._read_delimited_or_excel(xlsx_path)
    assert "Nombre Nudo" in xlsx_loaded.columns

    latin_path = tmp_path / "latin.csv"
    latin_path.write_bytes("Subestación ;Capacidad firme disponible (MW)\nÑodo;8,5\n".encode("latin-1"))
    latin_loaded = es._read_delimited_or_excel(latin_path)
    assert "Subestación " in latin_loaded.columns

    decode_path = tmp_path / "decode.csv"
    decode_path.write_text("a;b\n1;2\n", encoding="utf-8")

    def read_csv_decode(*args, **kwargs):
        raise UnicodeDecodeError("utf-8", b"x", 0, 1, "bad")

    original_read_csv = pd.read_csv
    monkeypatch.setattr(pd, "read_csv", read_csv_decode)
    try:
        es._read_delimited_or_excel(decode_path)
    except UnicodeDecodeError:
        pass
    else:
        raise AssertionError("Expected UnicodeDecodeError from fallback reader")
    monkeypatch.setattr(pd, "read_csv", original_read_csv)

    latlon_path = tmp_path / "ide_latlon.csv"
    latlon_path.write_text(
        "node,latitude,longitude,capacidad disponible (mw)\nNode A,40.5,-3.5,2.5\n",
        encoding="utf-8",
    )
    latlon_parsed = es.standardize_grid_capacity_file(latlon_path, "i-DE")
    assert latlon_parsed.loc[0, "distributor_network"] == "i-DE"
    assert np.isnan(latlon_parsed.loc[0, "voltage_kv"])

    empty_grid_path = tmp_path / "viesgo_empty.csv"
    empty_grid_path.write_text(
        "Nombre Subestación,Coordenada UTM X,Coordenada UTM Y,Capacidad firme disponible (MW)\nNode A,,,1\n",
        encoding="utf-8",
    )
    assert es.standardize_grid_capacity_file(empty_grid_path, "Viesgo").empty

    moderate_station = pd.DataFrame(
        [
            {
                "location_id": "IBE_010",
                "latitude": 40.5,
                "longitude": -3.5,
                "route_segment": "A-1",
                "n_chargers_proposed": 4,
                "grid_status": "Moderate",
            }
        ]
    )
    moderate_nodes = pd.DataFrame(
        [
            {"node_name": "Node M", "latitude": 40.5, "longitude": -3.5, "capacity_available_mw": 0.9, "voltage_kv": 66, "distributor_network": "i-DE"}
        ]
    )
    moderate = es.enrich_stations_with_grid(moderate_station, moderate_nodes, charger_power_kw=150)
    assert moderate.loc[0, "grid_status"] == "Moderate"

    empty_station_result = es.enrich_stations_with_grid(
        pd.DataFrame(columns=["location_id", "latitude", "longitude", "route_segment", "n_chargers_proposed", "grid_status"]),
        moderate_nodes,
        charger_power_kw=150,
    )
    assert empty_station_result.empty

    cached_dir = tmp_path / "cached"
    cached_dir.mkdir()
    (cached_dir / "existing_interurban_stations_by_route.csv").write_text("carretera,existing_station_count\nA-1,1\n", encoding="utf-8")
    (cached_dir / "existing_interurban_stations.csv").write_text("total_existing_stations_baseline,source\n1,cached\n", encoding="utf-8")
    (cached_dir / "ev_projection_2027.csv").write_text("target_year,total_ev_projected_2027\n2027,123\n", encoding="utf-8")
    (cached_dir / "edistribucion_capacity_2026_03.csv").write_text("dummy\n", encoding="utf-8")
    (cached_dir / "ide_capacity_2026_02.csv").write_text("dummy\n", encoding="utf-8")
    (cached_dir / "geoportal_gasolineras_matched.csv").write_text("latitude,longitude,carretera\n40.5,-3.5,A-1\n", encoding="utf-8")

    called = {"download": 0}

    def fail_download(*args, **kwargs):
        called["download"] += 1
        raise AssertionError("download_file should not be called when cache exists")

    monkeypatch.setattr(es, "download_file", fail_download)
    result = es.try_build_official_external_inputs(sample_roads(), cached_dir, target_year=2027)
    assert called["download"] == 0
    assert result["baseline_scalar_path"].exists()

    ide_fail_dir = tmp_path / "ide-fail"
    ide_fail_dir.mkdir()
    (ide_fail_dir / "existing_interurban_stations_by_route.csv").write_text("carretera,existing_station_count\nA-1,1\n", encoding="utf-8")
    (ide_fail_dir / "existing_interurban_stations.csv").write_text("total_existing_stations_baseline,source\n1,cached\n", encoding="utf-8")
    (ide_fail_dir / "ev_projection_2027.csv").write_text("target_year,total_ev_projected_2027\n2027,123\n", encoding="utf-8")
    (ide_fail_dir / "edistribucion_capacity_2026_03.csv").write_text("dummy\n", encoding="utf-8")
    (ide_fail_dir / "geoportal_gasolineras_matched.csv").write_text("latitude,longitude,carretera\n40.5,-3.5,A-1\n", encoding="utf-8")
    monkeypatch.setattr(
        es,
        "download_file",
        lambda url, output_path, timeout=180: (_ for _ in ()).throw(ValueError("ide unavailable"))
        if url == es.DEFAULT_IDE_URL
        else output_path.write_text("ok", encoding="utf-8") or output_path,
    )
    ide_fail_result = es.try_build_official_external_inputs(sample_roads(), ide_fail_dir, target_year=2027)
    assert all(path.name != "ide_capacity_2026_02.csv" for path in ide_fail_result["grid_capacity_paths"])

    nap_cached_dir = tmp_path / "nap-cached"
    nap_cached_dir.mkdir()
    (nap_cached_dir / "nap_charging_points.xml").write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
          xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
          xmlns:fac="http://datex2.eu/schema/3/facilities"
          xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
          xmlns:com="http://datex2.eu/schema/3/common">
          <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
            <egi:energyInfrastructureSite id="SITE_3" version="">
              <fac:name><com:values><com:value lang="es">Station Three</com:value></com:values></fac:name>
              <fac:locationReference>
                <loc:coordinatesForDisplay>
                  <loc:latitude>41.45</loc:latitude>
                  <loc:longitude>0.9</loc:longitude>
                </loc:coordinatesForDisplay>
              </fac:locationReference>
            </egi:energyInfrastructureSite>
          </egi:energyInfrastructureTable>
        </d2:payload>
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr(
        es,
        "build_ev_projection_from_official_repo",
        lambda monthly_output_path, projection_output_path, target_year: projection_output_path.write_text(
            "target_year,total_ev_projected_2027\n2027,999\n",
            encoding="utf-8",
        ),
    )
    monkeypatch.setattr(
        es,
        "download_file",
        lambda url, output_path, timeout=180: output_path.write_text(
            "\ufeffGestor de red;Coordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
            "R1-299;535771,67;4074262,07;AGUADULC;66;12,5\n",
            encoding="utf-8",
        ) or output_path,
    )
    nap_result = es.try_build_official_external_inputs(sample_roads(), nap_cached_dir, target_year=2027)
    assert nap_result["baseline_summary_path"].exists()

    ide_success_dir = tmp_path / "ide-success"
    ide_success_dir.mkdir()

    def selective_download(url, output_path, timeout=180):
        if "i-de.es" in url:
            output_path.write_text(
                "\ufeffCoordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
                "535771,67;4074262,07;IDE_NODE;66;9,5\n",
                encoding="utf-8",
            )
        elif "edistribucion" in url:
            output_path.write_text(
                "\ufeffGestor de red;Coordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
                "R1-299;535771,67;4074262,07;AGUADULC;66;12,5\n",
                encoding="utf-8",
            )
        else:
            output_path.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
                <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
                  xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
                  xmlns:fac="http://datex2.eu/schema/3/facilities"
                  xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
                  xmlns:com="http://datex2.eu/schema/3/common">
                  <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
                    <egi:energyInfrastructureSite id="SITE_3" version="">
                      <fac:name><com:values><com:value lang="es">Station Three</com:value></com:values></fac:name>
                      <fac:locationReference>
                        <loc:coordinatesForDisplay>
                          <loc:latitude>40.5</loc:latitude>
                          <loc:longitude>-3.5</loc:longitude>
                        </loc:coordinatesForDisplay>
                      </fac:locationReference>
                    </egi:energyInfrastructureSite>
                  </egi:energyInfrastructureTable>
                </d2:payload>
                """,
                encoding="utf-8",
            )
        return output_path

    monkeypatch.setattr(es, "download_file", selective_download)
    monkeypatch.setattr(
        es,
        "build_ev_projection_from_official_repo",
        lambda monthly_output_path, projection_output_path, target_year: projection_output_path.write_text(
            "target_year,total_ev_projected_2027\n2027,999\n",
            encoding="utf-8",
        ),
    )
    ide_success = es.try_build_official_external_inputs(sample_roads(), ide_success_dir, target_year=2027)
    assert any(path.name == "ide_capacity_2026_02.csv" for path in ide_success["grid_capacity_paths"])

    ide_failure_dir = tmp_path / "ide-failure"
    ide_failure_dir.mkdir()

    def ide_fails(url, output_path, timeout=180):
        if "i-de.es" in url:
            raise ValueError("temporarily unavailable")
        if "edistribucion" in url:
            output_path.write_text(
                "\ufeffGestor de red;Coordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
                "R1-299;535771,67;4074262,07;AGUADULC;66;12,5\n",
                encoding="utf-8",
            )
        else:
            output_path.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
                <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
                  xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
                  xmlns:fac="http://datex2.eu/schema/3/facilities"
                  xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
                  xmlns:com="http://datex2.eu/schema/3/common">
                  <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
                    <egi:energyInfrastructureSite id="SITE_4" version="">
                      <fac:name><com:values><com:value lang="es">Station Four</com:value></com:values></fac:name>
                      <fac:locationReference>
                        <loc:coordinatesForDisplay>
                          <loc:latitude>40.5</loc:latitude>
                          <loc:longitude>-3.5</loc:longitude>
                        </loc:coordinatesForDisplay>
                      </fac:locationReference>
                    </egi:energyInfrastructureSite>
                  </egi:energyInfrastructureTable>
                </d2:payload>
                """,
                encoding="utf-8",
            )
        return output_path

    monkeypatch.setattr(es, "download_file", ide_fails)
    monkeypatch.setattr(
        es,
        "build_ev_projection_from_official_repo",
        lambda monthly_output_path, projection_output_path, target_year: projection_output_path.write_text(
            "target_year,total_ev_projected_2027\n2027,999\n",
            encoding="utf-8",
        ),
    )
    ide_failure = es.try_build_official_external_inputs(sample_roads(), ide_failure_dir, target_year=2027)
    assert all(path.name != "ide_capacity_2026_02.csv" for path in ide_failure["grid_capacity_paths"])


def test_download_arcgis_geojson_and_traffic_try_build(tmp_path, monkeypatch, requests_mock):
    requests_mock.get(
        es.DEFAULT_MITMA_TRAFFIC_URL,
        [
            {
                "json": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {
                                "ID": 1,
                                "Nombre": "A-1",
                                "Longitud": 10,
                                "IMD_total": 15000,
                                "IMD_pesado": 1800,
                            },
                            "geometry": {
                                "type": "LineString",
                                "coordinates": [[-3.8, 40.4], [-3.2, 40.8]],
                            },
                        }
                    ],
                }
            },
            {"json": {"type": "FeatureCollection", "features": []}},
        ],
    )
    traffic_output = tmp_path / "traffic.geojson"
    saved = es.download_arcgis_geojson_paginated(
        es.DEFAULT_MITMA_TRAFFIC_URL,
        traffic_output,
        out_fields=["ID", "Nombre", "Longitud", "IMD_total", "IMD_pesado"],
        batch_size=1,
    )
    assert saved.exists()
    assert "FeatureCollection" in saved.read_text(encoding="utf-8")

    ext_dir = tmp_path / "official"
    ext_dir.mkdir()

    def fake_download(url, output_path, timeout=180):
        if output_path.suffix == ".xml":
            output_path.write_text(
                """<?xml version="1.0" encoding="UTF-8"?>
                <d2:payload xmlns:d2="http://datex2.eu/schema/3/d2Payload"
                  xmlns:egi="http://datex2.eu/schema/3/energyInfrastructure"
                  xmlns:fac="http://datex2.eu/schema/3/facilities"
                  xmlns:loc="http://datex2.eu/schema/3/locationReferencing"
                  xmlns:com="http://datex2.eu/schema/3/common">
                  <egi:energyInfrastructureTable id="ELECTROLINERAS" version="20260327">
                    <egi:energyInfrastructureSite id="SITE_1" version="">
                      <fac:name><com:values><com:value lang="es">Station One</com:value></com:values></fac:name>
                      <fac:locationReference><loc:coordinatesForDisplay><loc:latitude>40.5</loc:latitude><loc:longitude>-3.5</loc:longitude></loc:coordinatesForDisplay></fac:locationReference>
                    </egi:energyInfrastructureSite>
                  </egi:energyInfrastructureTable>
                </d2:payload>
                """,
                encoding="utf-8",
            )
        else:
            output_path.write_text(
                "\ufeffGestor de red;Coordenada UTM X;Coordenada UTM Y;Nombre Subestación;Nivel de Tensión (kV);Capacidad firme disponible (MW)\n"
                "R1-299;535771,67;4074262,07;AGUADULC;66;12,5\n",
                encoding="utf-8",
            )
        return output_path

    monkeypatch.setattr(es, "download_file", fake_download)
    monkeypatch.setattr(
        es,
        "build_ev_projection_from_official_repo",
        lambda monthly_output_path, projection_output_path, target_year: projection_output_path.write_text(
            "target_year,total_ev_projected_2027\n2027,123456\n",
            encoding="utf-8",
        ),
    )
    monkeypatch.setattr(
        es,
        "download_arcgis_geojson_paginated",
        lambda query_url, output_path, out_fields, where="1=1", batch_size=1000, timeout=180: traffic_output,
    )
    monkeypatch.setattr(
        es,
        "download_file",
        fake_download,
    )
    monkeypatch.setattr(
        es,
        "parse_geoportal_gasolineras_json",
        lambda path: pd.DataFrame(
            [
                {
                    "longitude": -3.5,
                    "latitude": 40.5,
                    "direccion": "A-1 km 10",
                    "rotulo": "REPSOL",
                    "tipo_venta": "P",
                    "rem": "dm",
                    "horario": "L-D: 24H",
                    "tipo_servicio": "L-D: 24H (A)",
                    "source_dataset": "Geoportal Gasolineras",
                }
            ]
        ),
    )
    result = es.try_build_official_external_inputs(sample_roads(), ext_dir, target_year=2027)
    assert result["traffic_summary_path"].exists()
    assert result["gasolineras_json_path"].exists()
    assert result["gasolineras_matched_path"].exists()

    failed_ext_dir = tmp_path / "official-failed-traffic"
    failed_ext_dir.mkdir()
    monkeypatch.setattr(
        es,
        "download_arcgis_geojson_paginated",
        lambda query_url, output_path, out_fields, where="1=1", batch_size=1000, timeout=180: (_ for _ in ()).throw(ValueError("traffic unavailable")),
    )
    failed_result = es.try_build_official_external_inputs(sample_roads(), failed_ext_dir, target_year=2027)
    assert not failed_result["traffic_summary_path"].exists()

    failed_gas_dir = tmp_path / "official-failed-gas"
    failed_gas_dir.mkdir()
    (failed_gas_dir / "miterd_estaciones_terrestres.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        es,
        "build_geoportal_gasolineras_baseline",
        lambda roads_gdf, xls_path, matched_output_path=None, max_distance_km=8.0: (_ for _ in ()).throw(
            ImportError("xlrd missing")
        ),
    )
    failed_gas_result = es.try_build_official_external_inputs(sample_roads(), failed_gas_dir, target_year=2027)
    assert not failed_gas_result["gasolineras_matched_path"].exists()

    failed_gas_download_dir = tmp_path / "official-failed-gas-download"
    failed_gas_download_dir.mkdir()
    monkeypatch.setattr(
        es,
        "download_file",
        lambda url, output_path, timeout=180: (_ for _ in ()).throw(ValueError("gasolineras unavailable"))
        if "EstacionesTerrestres" in url
        else fake_download(url, output_path, timeout=timeout),
    )
    failed_gas_download_result = es.try_build_official_external_inputs(sample_roads(), failed_gas_download_dir, target_year=2027)
    assert not failed_gas_download_result["gasolineras_json_path"].exists()

    failed_gas_fallback_dir = tmp_path / "official-failed-gas-fallback-xls"
    failed_gas_fallback_dir.mkdir()
    (failed_gas_fallback_dir / "geoportal_gasolineras_eess.xls").write_bytes(b"xls")
    monkeypatch.setattr(
        es,
        "download_file",
        lambda url, output_path, timeout=180: (_ for _ in ()).throw(ValueError("gasolineras unavailable"))
        if "EstacionesTerrestres" in url
        else fake_download(url, output_path, timeout=timeout),
    )
    monkeypatch.setattr(
        es,
        "build_geoportal_gasolineras_baseline",
        lambda roads_gdf, source_path, matched_output_path=None, max_distance_km=8.0: pd.DataFrame(
            [{"longitude": -3.5, "latitude": 40.5, "carretera": "A-1"}]
        ).to_csv(matched_output_path, index=False)
        or pd.read_csv(matched_output_path),
    )
    fallback_gas_result = es.try_build_official_external_inputs(sample_roads(), failed_gas_fallback_dir, target_year=2027)
    assert fallback_gas_result["gasolineras_matched_path"].exists()
