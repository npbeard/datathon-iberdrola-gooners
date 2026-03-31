# Executive Summary
## IE Iberdrola Datathon March 2026

## 1. Problem Framing

The goal of this project is to help Iberdrola decide where new public fast-charging stations should be prioritized on Spain's interurban road network for a 2027 scenario. The challenge is not only geographic coverage. A location can look good from a mobility point of view and still be a bad choice if the nearby grid cannot support high-power chargers.

Our approach focuses on three questions:

1. Which interurban corridors matter most for a first charging rollout?
2. How can we propose a network with as few stations as possible while still covering long-distance travel?
3. Where do mobility needs and grid limitations collide, creating friction points that need reinforcement or phased deployment?

## 2. Data Used

The current repository is built around the Ministry of Transport RTIG road dataset retrieved through the ArcGIS REST service. After cleaning and reprojection, the working dataset contains 1,602 road segments and roughly 38,529 km of network length.

Main fields used in the current version:

- road identifier (`carretera`)
- PK start and end (`pk_inicio`, `pk_fin`)
- road type (`tipo_de_via`)
- TEN-T membership (`tent`)
- geometry and derived length

The final package now combines the RTIG road network with three additional external layers:

- official NAP-DGT/MITERD charging-station baseline
- 2027 EV projection derived from the mandatory datos.gob.es exercise
- published demand-capacity files from i-DE, Endesa, and Viesgo

The remaining assumptions are no longer about whether these datasets are present. They are mainly about the siting rules applied on top of them, and they are documented in `data/submission/ASSUMPTIONS.md`.

## 3. Methodology

We used the following workflow:

1. Download RTIG road geometries from the Ministry REST API and keep a local cached copy so the pipeline can still run if the service is unavailable.
2. Clean the Esri geometry payload, validate features, and reproject the network to WGS84 for mapping.
3. Filter the network to interurban corridors relevant to the brief, especially A-, AP-, and N- roads.
4. Use PK ranges and route lengths as a first heuristic for proposed station spacing.
5. Generate the required output structure:
   - `File 1.csv`: global KPI summary
   - `File 2.csv`: proposed charging locations
   - `File 3.csv`: friction points
6. Validate the final files against the datathon rules before submission.

This gives a reproducible package with the main external inputs already integrated.

## 4. Current Results

The current generated submission package contains:

- 342 proposed charging locations in `File 2.csv`
- 9,699 existing baseline stations in `File 1.csv`
- 183 friction points in `File 3.csv`
- 120,008 projected EVs in the 2027 scenario in `File 1.csv`
- a self-contained HTML map in `maps/proposed_charging_network.html`
- a self-contained local scenario explorer in `maps/offline_scenario_explorer.html`

These values should be interpreted as a realistic planning package rather than exact investment commitments. Their value is that the required files exist, they pass validation, and the methodology is traceable back to official transport, charging, EV, and grid sources.

## 5. Key Findings

Several patterns are already visible from the current road-network analysis:

- TEN-T corridors account for a meaningful share of strategic interurban connectivity and are a natural starting point for a phased rollout.
- Long interurban axes such as AP-7 and other national corridors are the most likely candidates for early deployment because they combine long-distance travel relevance with clearer spacing logic.
- A station-placement heuristic based on route span produces a practical first network while still surfacing where grid reinforcement is likely to be needed.
- The friction-point layer is now more credible because it no longer depends on one distributor only. The current package includes node-level demand-capacity snapshots from i-DE, Endesa, and Viesgo.

## 6. Main Assumptions And Limits

This is the most important section for the jury.

The current repository is much stronger than the earlier road-only version, but there are still methodological assumptions that should be stated clearly:

1. station placement still uses a spacing heuristic rather than a full optimization model
2. charger-count decisions still depend on a policy choice (`conservative`, `balanced`, or `aggressive`) rather than a single official rule
3. `grid_status` is based on nearest-node matching plus documented thresholds, so it should be presented as a planning signal, not as a formal access study

These limitations should be stated clearly in the final presentation. They do not weaken the submission as much as hidden placeholders would. In fact, being explicit about them makes the package more credible.

## 7. Recommended 2027 Rollout Logic

Based on the current work, the most defensible rollout logic is:

### Phase 1

Focus on the major interurban corridors with the clearest long-distance need and strongest strategic relevance, especially TEN-T-aligned axes.

### Phase 2

Refine charger counts by corridor using the real baseline charging map and the 2027 EV projection so route-by-route density decisions are easier to justify.

### Phase 3

Overlay real distributor capacity nodes and separate stations into:

- immediately viable
- viable with moderate upgrades
- delayed pending reinforcement

This gives Iberdrola a practical way to distinguish between "best sites to build now" and "best sites to protect for later expansion."

## 8. Why This Matters For Iberdrola

From a business point of view, the project is useful because it translates a broad electrification challenge into a shortlist of deployable locations and a second shortlist of grid bottlenecks. That is a more actionable output than a simple map of roads or a generic demand forecast.

Even in its current state, the repo already helps answer:

- where a first rollout could start
- how to structure a minimum viable national corridor network
- which locations need a grid conversation before a charging conversation

## 9. What Would Most Improve The Final Score

If we had to prioritize only a few remaining tasks before submission, they would be:

1. tighten the explanation of why the 342 proposed locations are enough for a first national corridor rollout
2. present friction points as a phased execution issue, not just a technical warning
3. keep the final numbers visible in one notebook and one short pitch
4. make sure the jury opens the offline scenario explorer during the demo

## 10. Final Assessment

The strongest part of this project today is that it combines a reproducible technical pipeline with official charging, EV, and grid inputs, and turns them into a package the jury can open locally without setup problems.

That makes it a credible datathon submission. The remaining opportunity is not to add more software complexity, but to present the current results in a sharper and more Iberdrola-specific way.
