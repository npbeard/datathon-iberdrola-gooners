# Executive Summary
## IE Iberdrola Datathon March 2026

## 1. Decision Context

Iberdrola does not need a generic map of roads. It needs an interurban deployment plan for 2027 that answers three practical questions:

1. Where should new public fast-charging sites be prioritised to support long-distance EV travel?
2. How can that network be kept as lean as possible by taking credit for existing infrastructure?
3. Which promising locations are constrained by the electrical grid and therefore require phased deployment?

This project addresses those questions using a reproducible pipeline that joins the RTIG road network, the official national charging-point baseline, the mandatory datos.gob.es electrification workflow, and published distributor grid-capacity files.

## 2. What The Package Delivers

The repository produces the three required competition CSV files plus an offline-ready map for the jury:

- `File 1.csv`: global network KPI summary
- `File 2.csv`: proposed charging locations
- `File 3.csv`: friction points where mobility need collides with grid limits
- `maps/proposed_charging_network.html`: self-contained BI visualization that opens locally without internet access
- `maps/offline_scenario_explorer.html`: scenario-testing companion for the pitch

The project is validated by automated tests and a submission checker to reduce the risk of technical disqualification.

## 3. Data Foundation

The solution is built from official or competition-relevant sources:

- Ministry of Transport RTIG road geometries for Spain's interurban corridors
- NAP-DGT/MITERD charging-point publication to measure the existing public baseline
- datos.gob.es electrification exercise as the mandatory EV-adoption anchor
- Published demand-capacity files from i-DE, Endesa, and Viesgo for node-level grid constraints

The value of this combination is that the final proposal is not purely geographic. It treats charging deployment as the intersection of corridor need and electrical feasibility.

## 4. Methodology

The final methodology is intentionally simple enough to explain in five minutes and rigorous enough to defend:

### Step 1: Restrict the network to the brief

Only interurban A-, AP-, and N- corridors from the RTIG network are considered. This keeps the solution aligned with the assignment and avoids drifting into urban-centre charging logic.

### Step 2: Rank corridor service need

Each route receives a planning score based on:

- route length and PK span
- TEN-T strategic relevance
- geometric complexity
- scarcity of existing interurban charging stations already matched to that corridor

This improves on a naive fixed-spacing approach because a route with strong existing coverage should not receive the same number of new sites as a corridor with similar length but a much weaker baseline.

### Step 3: Convert corridor need into a lean deployment network

Instead of placing stations at one rigid national interval, the model uses dynamic spacing around a 120 km planning reference:

- higher-need corridors receive tighter spacing
- already-served corridors receive spacing credit
- exact duplicate coordinates on the same route are merged into one site

This makes the package more consistent with the Datathon's primary objective: cover interurban mobility demand with the fewest stations possible.

### Step 4: Test each site against grid reality

Each proposed station is matched to the nearest available published distributor node. Grid status is assigned using the brief's fixed charger power of 150 kW:

- `Sufficient`: available capacity is at least 2x site demand
- `Moderate`: available capacity is between 1x and 2x site demand
- `Congested`: available capacity is below site demand

This produces a second layer of strategic value: not just where Iberdrola would like to build, but where it can build immediately versus where reinforcement should come first.

## 5. Strategic Interpretation

The proposal should be read as a phased rollout plan rather than a one-shot national build:

- `Sufficient` sites are immediate deployment candidates
- `Moderate` sites are suitable for staged rollout with limited reinforcement or careful sequencing
- `Congested` sites should be protected strategically but treated as grid-led projects first

For Iberdrola, that distinction is critical. It turns a technical map into an investment roadmap.

## 6. Business Value For Iberdrola

The project creates value in four ways:

1. It prioritises corridors rather than isolated points, which is closer to how a national charging operator would actually plan.
2. It avoids overbuilding by accounting for the existing interurban charging baseline.
3. It makes grid bottlenecks explicit, reducing the risk of proposing commercially attractive but infeasible sites.
4. It gives Iberdrola a clear "build now / phase later / reinforce first" framing for 2027.

## 7. Remaining Assumptions

The remaining assumptions should be stated openly in the final submission because transparency will score better than false precision:

- corridor demand is estimated through public proxies, not proprietary traffic flows
- the 2027 EV number is a stock proxy built from official registrations plus forecasted additions
- nearest-node grid matching is a planning approximation, not a formal access study

These assumptions are reasonable in the context of a datathon because they are explicit, evidence-based, and consistent with the available public data.

## 8. Recommended Jury Message

The strongest closing message is:

This is not just a map of where chargers could go. It is a short-term Iberdrola deployment strategy that minimises redundant sites, respects real grid constraints, and distinguishes immediately viable corridors from those that should be phased with reinforcement.
