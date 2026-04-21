# Final Presentation Outline
## IE Iberdrola Datathon March 2026

This version is meant to sound closer to a student team presenting its work: clear, confident, and practical, without sounding too scripted.

## Recommended Structure

We recommend `6` slides plus a short closing sentence. That keeps the pacing safe for a `5-minute` presentation.

## Slide 1. The Problem

### Title
`Planning Spain's 2027 Interurban EV Charging Network`

### What to show

- one simple Spain / RTIG context visual
- one sentence on the challenge
- one line showing the tension between mobility need and grid feasibility

### Key message

The challenge is not only to add more chargers. It is to place the minimum number of new interurban stations in locations that also make sense from the grid side.

### Speaker notes

Start in a straightforward way. Explain that the project is about long-distance EV travel, not urban charging, and that the main difficulty is that good transport locations are not always good electrical locations.

## Slide 2. Our Approach

### Title
`What We Combined`

### What to show

- a simple visual with five blocks:
  `RTIG roads`
  `existing chargers`
  `2027 EV projection`
  `grid nodes`
  `demand / business signals`

### Key message

We combined the mandatory and official datasets with extra public data so the final proposal would be more realistic.

### Speaker notes

Keep this simple and human. You are not trying to impress with jargon here. Just explain that you used existing coverage, projected demand, and grid capacity together instead of looking at only one of them.

## Slide 3. Methodology

### Title
`How We Chose the Sites`

### What to show

- a 3-step flow:
  `rank corridors`
  `place the fewest stations possible`
  `check the grid`
- one short note on `120 km` dynamic spacing
- one short note on `150 kW` per charger

### Key message

We first ranked corridors, then placed stations with flexible spacing instead of a rigid national rule, and finally checked whether those sites were realistic from the grid side.

### Speaker notes

Do not overexplain. The judges mainly need to understand that your method is reasonable, aligned with the brief, and easy to follow.

## Slide 4. The Output

### Title
`What Our Model Produces`

### What to show

- the four key numbers:
  `252 proposed sites`
  `1,742 chargers`
  `153 friction points`
  `549,226 projected EVs in 2027`
- a small grid-status chart:
  `99 Sufficient`
  `15 Moderate`
  `138 Congested`

### Key message

The result is a selective network proposal that also makes the grid bottlenecks visible.

### Speaker notes

Mention that these numbers come directly from the final validated output files. That helps make the presentation feel grounded and consistent with the submission package.

## Slide 5. Why It Matters for Iberdrola

### Title
`From Model Output to Rollout Plan`

### What to show

- three simple labels:
  `Sufficient = build first`
  `Moderate = phase carefully`
  `Congested = reinforce first`
- optional note:
  `132 i-DE | 13 Endesa | 8 Viesgo`

### Key message

The main value of the project is not only choosing sites. It is helping separate immediate opportunities from locations that need grid work first.

### Speaker notes

This is the slide where you shift from technical explanation to business relevance. The tone here should be practical: what should Iberdrola actually do with these results?

## Slide 6. Final Recommendation

### Title
`Our Main Recommendation`

### What to show

- one final map screenshot
- three short takeaways:
  `avoid redundant sites`
  `prioritise buildable corridors`
  `use friction points to guide reinforcement`

### Key message

Our proposal is best understood as a phased 2027 roadmap: build first where the network is needed and feasible, and use the friction points to decide where grid reinforcement matters most.

### Speaker notes

End with a short sentence you can say naturally, without sounding memorised.

Suggested close:

`Our project is not just about where chargers could go. It is about how Iberdrola could prioritise interurban charging rollout in a way that is both useful and realistic.`

## What to Avoid

- long paragraphs
- too much technical detail on the slides
- code screenshots
- too many charts
- overly formal business language

If something sounds like a consultancy report, simplify it.

## Likely Q&A Questions

1. Why did you choose dynamic spacing around `120 km`?
2. How did you define `Sufficient`, `Moderate`, and `Congested`?
3. Why are so many friction points in `i-DE` territory?
4. How did you avoid overbuilding where chargers already exist?
5. If Iberdrola had to start tomorrow, what would be the first step?

## Useful Backup Slides

- `Appendix A`: assumptions and data sources
- `Appendix B`: methodology and file structure
