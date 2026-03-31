"""Validate the datathon submission files against the March 2026 brief."""

from __future__ import annotations

import math
from pathlib import Path
from typing import List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SUBMISSION_DIR = ROOT / "data" / "submission"

FILE_1_COLUMNS = [
    "total_proposed_stations",
    "total_existing_stations_baseline",
    "total_friction_points",
    "total_ev_projected_2027",
]
FILE_2_COLUMNS = [
    "location_id",
    "latitude",
    "longitude",
    "route_segment",
    "n_chargers_proposed",
    "grid_status",
]
FILE_3_COLUMNS = [
    "bottleneck_id",
    "latitude",
    "longitude",
    "route_segment",
    "distributor_network",
    "estimated_demand_kw",
    "grid_status",
]
VALID_GRID_STATUS = {"Sufficient", "Moderate", "Congested"}
VALID_DISTRIBUTORS = {"i-DE", "Endesa", "Viesgo"}


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required submission file: {path}")
    return pd.read_csv(path)


def validate_columns(df: pd.DataFrame, expected_columns: List[str], file_name: str) -> List[str]:
    issues = []
    if df.columns.tolist() != expected_columns:
        issues.append(f"{file_name}: expected columns {expected_columns}, got {df.columns.tolist()}")
    return issues


def validate_submission() -> List[str]:
    issues: List[str] = []

    file_1 = load_csv(SUBMISSION_DIR / "File 1.csv")
    file_2 = load_csv(SUBMISSION_DIR / "File 2.csv")
    file_3 = load_csv(SUBMISSION_DIR / "File 3.csv")

    issues.extend(validate_columns(file_1, FILE_1_COLUMNS, "File 1.csv"))
    issues.extend(validate_columns(file_2, FILE_2_COLUMNS, "File 2.csv"))
    issues.extend(validate_columns(file_3, FILE_3_COLUMNS, "File 3.csv"))

    if len(file_1) != 1:
        issues.append(f"File 1.csv: expected exactly 1 row, found {len(file_1)}")

    invalid_status_2 = set(file_2["grid_status"].dropna()) - VALID_GRID_STATUS
    if invalid_status_2:
        issues.append(f"File 2.csv: invalid grid_status values {sorted(invalid_status_2)}")

    invalid_status_3 = set(file_3["grid_status"].dropna()) - {"Moderate", "Congested"}
    if invalid_status_3:
        issues.append(f"File 3.csv: invalid grid_status values {sorted(invalid_status_3)}")

    invalid_distributors = set(file_3["distributor_network"].dropna()) - VALID_DISTRIBUTORS
    if invalid_distributors:
        issues.append(f"File 3.csv: invalid distributor_network values {sorted(invalid_distributors)}")

    if int(file_1["total_proposed_stations"].iloc[0]) != len(file_2):
        issues.append("File 1.csv: total_proposed_stations does not match the row count of File 2.csv")

    if int(file_1["total_friction_points"].iloc[0]) != len(file_3):
        issues.append("File 1.csv: total_friction_points does not match the row count of File 3.csv")

    if not file_3.empty:
        if not file_3["grid_status"].isin(["Moderate", "Congested"]).all():
            issues.append("File 3.csv: contains rows with grid_status outside Moderate/Congested")

        for row in file_3.itertuples(index=False):
            matching = file_2[
                (file_2["latitude"].round(6) == round(row.latitude, 6))
                & (file_2["longitude"].round(6) == round(row.longitude, 6))
                & (file_2["route_segment"] == row.route_segment)
            ]
            if matching.empty:
                issues.append(
                    f"File 3.csv: bottleneck {row.bottleneck_id} does not match any File 2 station by route and coordinates"
                )
                continue

            chargers = int(matching["n_chargers_proposed"].iloc[0])
            expected_demand = chargers * 150
            if not math.isclose(float(row.estimated_demand_kw), expected_demand, rel_tol=0, abs_tol=1e-9):
                issues.append(
                    f"File 3.csv: bottleneck {row.bottleneck_id} has estimated_demand_kw={row.estimated_demand_kw}, expected {expected_demand}"
                )

    return issues


if __name__ == "__main__":  # pragma: no cover
    problems = validate_submission()
    if problems:
        print("Submission validation failed:")
        for issue in problems:
            print(f"- {issue}")
        raise SystemExit(1)

    print("Submission validation passed.")
