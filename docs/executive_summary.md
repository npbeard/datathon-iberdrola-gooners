# Executive Summary
## IE Iberdrola Datathon March 2026

## 1. Decision Context

The main goal of this project was to move beyond a generic road map and build something that could actually help Iberdrola think about interurban charging in 2027. In practice, we focused on three questions:

1. Where should new public fast-charging sites be prioritised to support long-distance EV travel?
2. How can that network be kept as lean as possible by taking credit for existing infrastructure?
3. Which promising locations are constrained by the electrical grid and therefore require phased deployment?

To answer them, we built a reproducible pipeline that combines the RTIG road network, the official public charging baseline, the mandatory datos.gob.es electrification workflow, published grid-capacity files, and public business-demand signals from MITERD and INE.

## 2. What The Package Delivers

The repository produces the three required competition CSV files and an offline-ready map for the jury:

- `File 1.csv`: global network KPI summary
- `File 2.csv`: proposed charging locations
- `File 3.csv`: friction points where mobility need collides with grid limits
- `maps/proposed_charging_network.html`: self-contained BI visualization that opens locally without internet access
- `maps/offline_scenario_explorer.html`: scenario-testing companion for the pitch

We also included automated tests and a submission checker so the package is easier to trust and less likely to fail for technical reasons.

## 3. Data Foundation

The project is built mainly on official or competition-relevant sources:

- Ministry of Transport RTIG road geometries for Spain's interurban corridors
- NAP-DGT/MITERD charging-point publication to measure the existing public baseline
- datos.gob.es electrification exercise as the mandatory EV-adoption anchor
- Published demand-capacity files from i-DE, Endesa, and Viesgo for node-level grid constraints
- MITERD/Geoportal Gasolineras roadside-service inventory for commercial co-location potential
- INE municipal population and provincial overnight stays to capture market access and tourism pressure

What we found useful about this combination is that the final proposal is not just geographic. It treats charging deployment as the intersection of corridor need, existing coverage, business attractiveness, and electrical feasibility.

## 4. Methodology

We tried to keep the methodology simple enough to explain clearly, while still being strong enough to defend.

### Step 1: Restrict the network to the brief

We only considered interurban A-, AP-, and N- corridors from the RTIG network. This keeps the solution aligned with the brief and avoids drifting into urban charging logic.

### Step 2: Rank corridor service need

Each route receives a planning score based on:

- route length and PK span
- TEN-T strategic relevance
- geometric complexity
- scarcity of existing interurban charging stations already matched to that corridor
- traffic intensity and heavy-vehicle share from MITMA
- market-access and tourism attractiveness from INE and roadside-service proxies

The reason for doing this is that a route with strong existing coverage should not receive the same treatment as a corridor with similar length but much weaker charging availability.

### Step 3: Convert corridor need into a lean deployment network

Instead of placing stations at one rigid national interval, we used dynamic spacing around a 120 km planning reference:

- higher-need corridors receive tighter spacing
- already-served corridors receive spacing credit
- exact duplicate coordinates on the same route are merged into one site

This keeps the package closer to the main Datathon objective: covering interurban mobility demand with as few new stations as possible.

### Step 4: Test each site against grid reality

Each proposed station is matched to the nearest available published distributor node. Grid status is assigned using the brief’s fixed charger power of 150 kW:

- `Sufficient`: available capacity is at least 2x site demand
- `Moderate`: available capacity is between 1x and 2x site demand
- `Congested`: available capacity is below site demand

This adds an important second layer to the analysis: not just where Iberdrola might want to build, but where it can build immediately and where reinforcement should come first.

### Step 5: Prioritise commercially stronger stop environments

The final station candidates are not chosen only through corridor geometry. We also give controlled credit to locations with stronger stop quality:

- nearby official roadside-service infrastructure
- larger municipal market access
- provinces with stronger official overnight-stay demand

We think this matters because interurban fast charging is more attractive when drivers have a realistic reason to stop, wait, and use nearby services.

## 5. Strategic Interpretation

We think the proposal makes the most sense as a phased rollout plan rather than a one-shot national build:

- `Sufficient` sites are immediate deployment candidates
- `Moderate` sites are suitable for staged rollout with limited reinforcement or careful sequencing
- `Congested` sites should be protected strategically but treated as grid-led projects first

That distinction is important because it turns the output from a technical map into something closer to an investment roadmap.

## 6. Business Value For Iberdrola

From our point of view, the project creates value in five ways:

1. It prioritises corridors rather than isolated points, which is closer to how a national charging operator would actually plan.
2. It avoids overbuilding by accounting for the existing interurban charging baseline.
3. It makes grid bottlenecks explicit, reducing the risk of proposing commercially attractive but infeasible sites.
4. It favours sites with stronger business context, not just mathematically convenient coordinates.
5. It gives Iberdrola a clear "build now / phase later / reinforce first" framing for 2027.

## 7. Remaining Assumptions

The main remaining assumptions should be stated openly in the final submission:

- corridor demand is estimated through public proxies, not proprietary traffic flows
- the 2027 EV number is a stock proxy built from official registrations plus forecasted additions
- nearest-node grid matching is a planning approximation, not a formal access study
- tourism demand is captured through public overnight-stay indicators at provincial scale, not site-level private footfall data

We think these assumptions are reasonable for a datathon because they are explicit, evidence-based, and consistent with the public data that was actually available to us.

## 8. Recommended Jury Message

If we had to summarise the project in one sentence for the jury, it would be:

This is not just a map of where chargers could go. It is a short-term Iberdrola deployment strategy that tries to minimise redundant sites, respect real grid constraints, and prioritise corridors where mobility need, business attractiveness, and phased buildability come together.
