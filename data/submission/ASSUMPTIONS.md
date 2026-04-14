# Submission Assumptions

- `File 1.csv`, `File 2.csv`, and `File 3.csv` were generated from the local RTIG dataset.
- Station placement uses a corridor-planning model that combines route span, strategic relevance, TEN-T status, baseline charging scarcity, and dynamic spacing around a `120.0` km reference.
- Existing-station baseline source status: `loaded:existing_interurban_stations.csv`.
- EV projection source status: `loaded:ev_projection_2027.csv`.
- Grid capacity source status: `loaded:grid_capacity_files`.
- Business-attractiveness proxy status: `loaded:existing_interurban_stations_matched.csv`.
- Existing charging baseline uses the official NAP-DGT/MITERD XML spatially matched to RTIG corridors within a 5 km threshold.
- EV demand uses the official datos.gob.es electrification exercise data and a SARIMA extension of the published notebook approach to estimate the 2027 EV stock proxy.
- Grid matching uses the nearest available published distributor demand-capacity nodes in `data/external/`, classifying locations as `Sufficient`, `Moderate`, or `Congested` based on whether available capacity is above 2x demand, between 1x and 2x demand, or below demand.
- Business attractiveness uses a conservative proxy based on nearby interurban charging-site metadata mentioning restaurants, cafes, hotels, fuel, parking, and retail uses, so competing candidate sites on the same corridor are nudged toward stronger service ecosystems.
- Exact duplicate station coordinates on the same route are merged into a single site so the package better reflects the "fewest stations possible" objective.
