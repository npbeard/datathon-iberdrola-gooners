# Executive Summary
## IE Iberdrola Datathon March 2026

## 1. Project Context

For this Datathon, we wanted to answer a practical question: if Iberdrola had to plan its interurban EV charging network for 2027, where should it prioritize new stations, and where would the grid make that difficult?

What we found very early is that this is not just a mapping problem. A location can look perfect from a transport point of view and still be hard to develop if the nearest grid node does not have enough available capacity. Because of that, we approached the challenge from three angles at the same time:

- mobility need along interurban corridors
- existing charging coverage
- grid feasibility

Our aim was to propose a network that is useful for Iberdrola in the short term, but also realistic enough to defend.

## 2. What We Are Submitting

Our repository produces the required outputs from the brief:

- `File 1.csv`: overall network KPI summary
- `File 2.csv`: proposed charging locations
- `File 3.csv`: friction points, meaning proposed sites where charging demand exists but grid capacity is tight or insufficient
- `maps/proposed_charging_network.html`: self-contained map for the jury
- `maps/offline_scenario_explorer.html`: extra visual support for presentation and discussion

The current validated package contains:

- `252` proposed charging locations
- `9,699` existing interurban baseline stations from the official charging dataset
- `153` friction points
- `549,226` projected EVs in 2027
- `1,742` proposed chargers in total

We also included a validation script so the output files can be checked against the format required in the Datathon brief.

## 3. Data Used

We tried to stay as close as possible to official or competition-relevant sources. The main inputs in our project are:

- RTIG road geometries from the Ministry of Transport
- the National Access Point / MITERD charging-point dataset for the current baseline
- the mandatory `datos.gob.es` electrification workflow for the 2027 EV projection
- grid-capacity files from i-DE, Endesa, and Viesgo
- MITMA traffic data
- roadside-service data from MITERD / Geoportal Gasolineras
- INE population and overnight-stay data

Using these sources together was important for us because a charger should not be placed only where a road exists. We wanted to consider whether the corridor matters, whether it is already covered, whether there is likely demand nearby, whether the stop environment makes business sense, and whether the grid could support the site.

## 4. Methodology

We tried to keep the methodology simple enough to explain clearly, while still making it useful for the actual challenge.

### Step 1: Keep the analysis inside the Datathon scope

We restricted the analysis to interurban RTIG roads, especially A-, AP-, and N- corridors. This follows the brief directly and keeps the focus on long-distance travel rather than urban charging.

### Step 2: Rank corridor need

We scored corridors using a combination of:

- route span and route length
- TEN-T relevance
- geometric complexity
- scarcity of existing charging stations already matched to the corridor
- traffic intensity and heavy-vehicle share
- business and demand proxies such as roadside services, population, and overnight stays

The logic here is simple: a route should not get the same priority if it is already relatively well served. We wanted to give more weight to corridors where mobility need and lack of coverage appear together.

At the route level, we also added a business layer. In the code, this comes from three inputs:

- a `business_support_score` built from nearby roadside and service anchors
- nearby municipal population as a market-access proxy
- provincial overnight stays as a tourism-demand proxy

These factors do not replace corridor need, but they do help us distinguish between corridors that look similar on pure transport logic. In practice, they push the model toward routes that are more likely to support a commercially credible stop.

### Step 3: Turn corridor need into a lean station network

Instead of using one rigid national spacing rule, we worked with dynamic spacing around a `120 km` reference:

- corridors with stronger need receive tighter spacing
- corridors with stronger existing coverage receive more spacing credit
- duplicate coordinates on the same route are merged into one site

This was our way of staying close to the main goal of the challenge, which is to cover interurban demand with the lowest possible number of new stations.

### Step 4: Size the sites

Once the candidate sites were selected, we assigned the number of chargers based on corridor need and local demand signals. The current package ends up with:

- `252` sites
- `1,742` chargers
- `6.91` chargers per site on average

### Step 5: Add business fit at the station level

After deciding how many sites each route should get, we still had to choose where along the corridor each station should sit. This is where the business-fit logic matters most.

For each target point on a route, the model checks a few nearby alternatives around the ideal spacing position and scores them using a local business-fit signal. That score is based on nearby business anchors, especially:

- official roadside service and fuel locations from the Geoportal Gasolineras / MITERD source
- existing interurban charging-site metadata when it includes useful commercial context
- larger nearby population centers
- provinces with stronger overnight-stay demand

Each anchor gets a base score depending on what it is. For example, food, lodging, fuel, retail, parking, and service-area keywords add different amounts. Gas-station anchors also receive a little more weight when they appear to offer stronger operating conditions, such as 24-hour schedules or more complete service coverage. Population and tourism signals then add a smaller extra boost.

At the route level, these anchor values are aggregated into the `business_support_score`. At the station level, the model calculates a local business-fit score by looking at nearby anchors and giving more credit to the closest ones. In other words, a proposed site gets a higher business-fit score when it sits near a stronger and denser stop ecosystem.

We did not use business fit as the only criterion, but we did use it to choose better stop environments when several candidate points on the same corridor were otherwise similar.

### Step 6: Check grid viability

For each proposed location, we matched the site to the nearest available distributor node. Following the Datathon rule of `150 kW` per charger, we classified each site as:

- `Sufficient`: available capacity is at least 2x the estimated site demand
- `Moderate`: available capacity is between 1x and 2x the estimated site demand
- `Congested`: available capacity is below the estimated site demand

We know these thresholds are still an assumption, so we state them clearly. The brief makes it very clear that assumptions like this need to be visible and justified.

This is also where friction points are defined. In our package, a friction point is not just any location with weak grid conditions in general. It is a proposed charging site from `File 2.csv` that ends up classified as `Moderate` or `Congested` once we compare its estimated demand against the nearest distributor node. So every row in `File 3.csv` is a subset of the proposed stations in `File 2.csv`, and each one marks a location where the mobility case is there, but the grid case is tighter.

## 5. Main Results

We think the final output is intentionally selective. The proposal is not trying to place chargers everywhere. It is trying to place them where they make the most sense within the limits of the data and the grid.

### Submission snapshot

- Proposed sites: `252`
- Proposed chargers: `1,742`
- Existing baseline stations: `9,699`
- Friction points: `153`
- Projected EV stock proxy in 2027: `549,226`

### Grid-status split

- `99` sites are `Sufficient`
- `15` sites are `Moderate`
- `138` sites are `Congested`

For us, this is one of the most important findings in the project. A large share of the locations that look interesting from a mobility perspective are still constrained by the grid.

Another way to read this is that friction points are not failures in the model. They are part of the result. They show where Iberdrola would likely need to coordinate grid reinforcement, phase deployment more carefully, or treat the site as a medium-term opportunity instead of an immediate build.

### Distributor split of friction points

- `132` friction points are in `i-DE`
- `13` friction points are in `Endesa`
- `8` friction points are in `Viesgo`

This suggests that the rollout strategy should not only be route-based. It should also take distributor coordination into account, especially where reinforcement needs are concentrated.

### Corridors with the most proposed sites

Some of the most represented corridors in the package are:

- `A-66`: 6 sites / 48 chargers
- `A-7`: 5 sites / 60 chargers
- `A-2`: 5 sites / 46 chargers
- `N-340`: 5 sites / 40 chargers
- `A-23`, `A-4`, `A-6`, `A-8`, and `AP-7N`: 4 sites each

These routes stand out because they combine strategic relevance, distance, and spacing needs with the rest of the demand and grid logic in the model.

## 6. Business Fit and Site Quality

One part that matters from a business point of view is whether a station sits in a place where drivers are actually likely to stop. We did not want the model to recommend coordinates that technically fit the spacing logic but make weak commercial sense in practice.

That is why we added a business-fit layer. In simple terms, the score rewards sites that are closer to:

- fuel and roadside-service ecosystems
- food and retail options
- lodging or other longer-stop services
- larger surrounding populations
- provinces with stronger overnight-stay activity

In the code, this works in two layers:

1. Route-level business support. Each corridor gets a higher score when it has more relevant roadside and commercial anchors associated with it.
2. Station-level business fit. When the model chooses the final point on the route, it favors the nearby candidate with the strongest local business signal.

We think this matters because interurban charging works better when the stop feels natural. A driver is more likely to accept charging time if the station is near food, services, or a known roadside stop area. So even though the Datathon is centered on infrastructure planning, we felt it was important to include a business-quality lens as well.

## 7. Strategic Interpretation

We think the best way to read our output is as a phased rollout plan rather than a one-time national deployment.

### Phase 1: Build first where the grid already allows it

The `Sufficient` locations are the most straightforward sites to prioritize. They are the places where corridor need and grid feasibility already line up.

### Phase 2: Roll out carefully where the grid is tighter

The `Moderate` locations still make sense, but they probably need better sequencing, more caution, or some limited reinforcement before they become easy wins.

### Phase 3: Treat congested sites as strategic medium-term opportunities

The `Congested` locations should not be ignored. In many cases, they are still strong corridor candidates. The issue is that they are not immediately buildable under current grid conditions, so they should be treated as reinforcement-led opportunities.

This gives Iberdrola a more realistic roadmap:

- build now where need and feasibility already match
- phase the more constrained locations carefully
- use friction points to decide where grid reinforcement would unlock the most value

## 8. Assumptions and Limitations

We want to be clear about the main limits of the project:

- corridor demand is estimated with public proxies, not private commercial demand data
- the 2027 EV figure is a projected stock proxy based on the mandatory `datos.gob.es` workflow
- nearest-node grid matching is a planning approximation, not a formal grid access study
- business attractiveness is approximated through public roadside, population, and tourism indicators rather than private footfall or transaction data
- the `120 km` spacing reference is a planning rule, not a hard technical law
- the grid thresholds are a practical way to classify feasibility using the public capacity data we had available

We think these assumptions are reasonable for a Datathon, but they are still assumptions, so they should be read with that in mind.

## 9. Value for Iberdrola

From our point of view, the project is useful for Iberdrola for a few reasons:

1. It plans by corridor instead of by isolated points.
2. It avoids overbuilding by accounting for the existing charging baseline.
3. It makes grid constraints visible instead of hiding them behind a demand-only model.
4. It gives extra weight to places that are more plausible as real stop environments.
5. It turns the technical output into a phased 2027 deployment roadmap.

## 10. Final Takeaway

We are not only proposing where chargers could go. We are proposing a practical way for Iberdrola to prioritize interurban charging investment in Spain for 2027, while being honest about where the grid still gets in the way.
