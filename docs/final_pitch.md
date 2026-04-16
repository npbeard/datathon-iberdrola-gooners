# Final Pitch Outline

## Slide 1: The Problem

We started from the idea that Spain’s interurban EV network still faces two main bottlenecks:

- long-distance "range anxiety" on national corridors
- local grid capacity that makes some attractive road locations unbuildable today

Our objective was to design the leanest possible interurban charging network for 2027 that Iberdrola could realistically act on.

## Slide 2: Our Approach

We combined five layers:

1. RTIG interurban corridors
2. existing official public charging baseline
3. the mandatory datos.gob.es EV-electrification workflow for 2027
4. distributor grid-capacity data from i-DE, Endesa, and Viesgo
5. business-demand signals from MITERD roadside-service data and INE population/tourism data

That lets us answer not just “where demand exists” but “where demand, commercial attractiveness, and electrical feasibility align.”

## Slide 3: Methodology

We ranked corridor need using:

- route length and PK span
- TEN-T strategic relevance
- baseline charging scarcity
- route complexity
- MITMA traffic intensity
- business-fit proxies from roadside services, municipal population, and overnight-stay demand

We then translated that score into dynamic station spacing, merged duplicate candidate coordinates, and tested each site against the nearest available grid node using the fixed 150 kW per charger standard from the brief.

## Slide 4: The Output

In terms of outputs, we deliver exactly what the brief requires:

- `File 1.csv`: network KPI scorecard
- `File 2.csv`: proposed charging sites
- `File 3.csv`: friction points
- a self-contained offline BI visualization

The key difference is that we do not treat friction points as failures. We treat them as part of a phased deployment queue.

## Slide 5: Why The Sites Are Business-Sensible

We did not want to optimise only for road geometry.

We also favoured places where an interurban fast-charging stop is more commercially credible:

- roadside service ecosystems already exist
- nearby market access is larger
- tourism and overnight-stay demand is stronger

We think this makes the plan easier to defend as an Iberdrola investment roadmap, not just as an academic network design.

## Slide 6: Strategic Message

We classify sites into three practical business actions:

- `Sufficient`: build now
- `Moderate`: build with limited reinforcement or staged rollout
- `Congested`: protect the location strategically, but reinforce first

This gives a more realistic 2027 roadmap instead of just a map of good intentions.

## Slide 7: Why This Matters

Our project helps Iberdrola:

- minimise redundant interurban sites
- focus capital on corridors with the clearest mobility value
- avoid proposing locations that fail the grid-feasibility test
- prioritise stop environments that are more attractive for drivers and co-located services
- separate immediate wins from longer-term infrastructure plays

## Closing Line

We are not only proposing where chargers could go. We are proposing how Iberdrola could sequence the next wave of interurban charging investment in Spain, focusing on sites that are needed, buildable, and commercially credible.
