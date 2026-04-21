# Final Pitch
## IE Iberdrola Datathon March 2026

## Slide 1: The Problem

When we started this project, the main idea was simple: Spain needs better interurban EV coverage, but not every good road location is actually feasible from the grid side.

So our objective was to design a 2027 interurban charging network that covers long-distance mobility demand with as few new stations as possible, while still being realistic about electrical constraints.

## Slide 2: Our Approach

We combined five main layers:

1. RTIG interurban corridors
2. the existing official charging baseline
3. the mandatory `datos.gob.es` EV projection for 2027
4. grid-capacity data from i-DE, Endesa, and Viesgo
5. demand and stop-quality signals from traffic, roadside services, population, and tourism data

This helped us move away from a purely geographic approach and towards something that is closer to a real deployment decision.

## Slide 3: Methodology

We ranked corridors using:

- route length and PK span
- TEN-T relevance
- scarcity of existing charging supply
- route complexity
- traffic intensity and heavy-vehicle share
- business and demand proxies

After that, we translated corridor need into dynamic spacing around a `120 km` reference, merged duplicated site coordinates on the same route, and checked each proposed location against the nearest available grid node using the Datathon rule of `150 kW` per charger.

## Slide 4: The Output

Our current validated package contains:

- `252` proposed charging sites
- `1,742` chargers
- `153` friction points
- `549,226` projected EVs in 2027

Grid status split:

- `99` Sufficient
- `15` Moderate
- `138` Congested

These outputs feed directly into the required deliverables:

- `File 1.csv`
- `File 2.csv`
- `File 3.csv`
- `maps/proposed_charging_network.html`

## Slide 5: Why It Matters

For us, the most important result is that many strong corridor opportunities are limited less by mobility demand and more by grid conditions.

That naturally turns the proposal into a phased plan:

- `Sufficient`: build first
- `Moderate`: phase carefully
- `Congested`: reinforce first

This helps Iberdrola avoid two common problems: overbuilding where coverage already exists and prioritising locations that look good on a map but are not ready from the grid side.

## Slide 6: Final Message

Our recommendation is to start where corridor need and grid feasibility already align, and then use the friction points to guide where reinforcement would unlock the next wave of interurban charging.

## Closing Line

We are not just showing where chargers could be placed. We are showing how Iberdrola could prioritise interurban charging rollout in Spain for 2027 in a more realistic way.
