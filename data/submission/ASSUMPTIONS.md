# Submission Assumptions

- `File 1.csv`, `File 2.csv`, and `File 3.csv` were generated from the local RTIG dataset.
- Station placement uses a spacing heuristic of `120.0` km along A-/AP-/N- corridors using PK ranges.
- Existing-station baseline source status: `loaded:existing_interurban_stations.csv`.
- EV projection source status: `loaded:ev_projection_2027.csv`.
- Grid capacity source status: `loaded:grid_capacity_files`.
- Existing charging baseline uses the official NAP-DGT/MITERD XML spatially matched to RTIG corridors within a 5 km threshold.
- EV demand uses the official datos.gob.es electrification exercise data and a SARIMA extension of the published notebook approach to reach 2027.
- Grid matching currently uses the nearest available published distributor demand-capacity nodes in `data/external/`, with demand-side files from i-DE, Endesa, and Viesgo represented in the current build.
- Friction-point counts and distributor assignments therefore reflect published node snapshots rather than a single-distributor proxy.
- Remaining assumptions are mainly methodological: station spacing (`120 km` default), charger-count policy, and the thresholds used to convert nearest-node capacity into `Moderate` vs `Congested` grid status.
