# Walton production dashboard — design brief (beta)

The beta lives at `docs/beta/` and is built by `src/build_beta_dashboard.py` from the same
data as the current site. The current site is untouched until the beta replaces it.

## Who reads it, where

- Zubair and the supervisors, several times a day, mostly on a phone; on a laptop in the
  office; occasionally on a wall screen. Ease of consuming the information is the primary
  goal. Nothing decorative.
- The first screen must answer "how are we doing today" without scrolling: pounds so far,
  pounds yesterday against normal, the week so far, who has filed End of Shift.

## Sources and what we took from each

- **Grafana best practices**: a dashboard tells one story and reduces cognitive load; the
  most important thing sits top-left; stat tiles for headline numbers; one time-range
  control that scopes everything; no misleading stacking; name and date every view.
- **Linear's personalized sidebar**: navigation is a short, stable list the reader can
  learn; rarely used items go behind "More". On the desktop it is a left sidebar; on the
  phone the same list becomes a bottom tab bar, so one mental model works in both places.
- **Financial Times charts** (John Burn-Murdoch, via danielroelfs.com): the title states
  the message and the subtitle states the measure; no vertical gridlines, hairline
  horizontal ones only; no axis titles; direct labels at the line end instead of a legend;
  one accent hue on a warm neutral surface; the latest point emphasised with a hollow dot;
  small multiples instead of ten lines in one plot.
- **The dataviz method** (bundled skill): pick the form by the data's job; categorical hues
  in a fixed validated order that follows the entity, never its rank; 2px lines, <= 24px bars
  with 4px rounded data-ends, hairline solid grid; a legend for >= 2 series; never a number
  on every point; tooltips enhance but never gate; a table view for every chart.

## Layout

- **Desktop (>= 900px)**: left sidebar 240px (collapsible to 64px icons, remembered);
  content column max 1080px. **Phone**: top bar with the page name and the data date; a
  fixed bottom tab bar with the same five items; content is one view at a time.
- **Views** (hash-routed, no page reloads): Today · Week · Shifts · Machines · More.
  - *Today*: hero number (pounds so far today from the live feed, or yesterday when the
    plant is closed), a stat row (yesterday vs the 4-week same-weekday average, week to
    date vs last week at the same point, open jobs, End of Shift filed), machine tiles for
    today, the latest changes from the live feed.
  - *Week*: pounds by machine and day for the current week with a shift selector, one
    column chart of the plant's day totals with the 4-week average as a rule.
  - *Shifts*: the End of Shift reports as filed, one card per shift, with a day picker.
  - *Machines*: one small chart per machine, 20 weeks, 4-week average as the line, raw
    weeks as a faint step behind, latest value labelled.
  - *More*: links to the current dashboard, daily details, the End of Shift form, and
    the data notes.
- One control row per view, above everything it scopes. Never a control inside a card.

## Type and colour

- System sans everywhere, including the hero figure. Hero 44px, stat values 26px,
  body 15px, captions 12px. Proportional figures on big numbers; `tabular-nums` only in
  table columns and axis ticks.
- Surface: warm off-white `#faf9f6` page, `#ffffff` cards, hairline borders
  `rgba(17,24,39,.10)`. Dark mode is selected, not inverted: `#111210` page, `#1a1b18` cards.
- Brand accent (single hue): `#0b6e4f` (evergreen, already the site's brand). Used for the
  one emphasised line, the active nav item, links. Never as a background block.
- Status: good `#0ca30c`, warning `#fab219`, critical `#d03b3b`, always with a word or icon.
- Machines (categorical, fixed slot per machine, validated with the skill's validator):
  slot 1 Extruder `#2a78d6`, 2 Auto Tie Baler `#eb6834`, 3 Grinder `#1baf7a`,
  4 Guillotine `#eda100`, 5 Shredder `#e87ba4`, 6 Green Max Densifier `#008300`,
  7 others `#4a3aa7`. Used only as swatches beside names and for tiles; charts with one
  series per facet use the brand hue.

## What we avoid (the anti-slop list)

Cards inside cards, gradients and glows, pills and badges that carry no state, icons
for decoration, ten-line legends, dual axes, dashed gridlines, per-card control rows,
a number on every point, and any element that does not help someone read the plant
faster.
