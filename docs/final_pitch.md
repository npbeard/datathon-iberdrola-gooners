# Final Pitch Outline

## Slide 1: The Problem

Spain's interurban EV network is still constrained by two bottlenecks:

- long-distance "range anxiety" on national corridors
- local grid capacity that makes some attractive road locations unbuildable today

Our objective was to design the leanest possible 2027 interurban charging network that Iberdrola could actually act on.

## Slide 2: Our Approach

We combined four layers:

1. RTIG interurban corridors
2. existing official public charging baseline
3. the mandatory datos.gob.es EV-electrification workflow for 2027
4. distributor grid-capacity data from i-DE, Endesa, and Viesgo

That allows us to answer not just "where demand exists" but "where demand and electrical feasibility align."

## Slide 3: Methodology

We ranked corridor need using:

- route length and PK span
- TEN-T strategic relevance
- baseline charging scarcity
- route complexity

We then translated that score into dynamic station spacing, merged duplicate candidate coordinates, and tested each site against the nearest available grid node using the fixed 150 kW per charger standard from the brief.

## Slide 4: The Output

We deliver exactly what the brief requires:

- `File 1.csv`: network KPI scorecard
- `File 2.csv`: proposed charging sites
- `File 3.csv`: friction points
- a self-contained offline BI visualization

The key difference is that our friction points are not treated as failures. They become a phased deployment queue for Iberdrola.

## Slide 5: Strategic Message

We classify sites into three business actions:

- `Sufficient`: build now
- `Moderate`: build with limited reinforcement or staged rollout
- `Congested`: protect the location strategically, but reinforce first

This creates a realistic 2027 roadmap instead of an abstract map of good intentions.

## Slide 6: Why This Matters

Our project helps Iberdrola:

- minimise redundant interurban sites
- focus capital on corridors with the clearest mobility value
- avoid proposing locations that fail the grid-feasibility test
- separate immediate wins from longer-term infrastructure plays

## Closing Line

We are not only proposing where chargers should go. We are proposing how Iberdrola should sequence the next wave of interurban charging investment in Spain.
