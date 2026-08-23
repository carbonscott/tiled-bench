"""Build the before/after HTML report (runbook §5) from the two sweep CSVs.

Usage: build_report.py BEFORE.CSV AFTER.CSV OUT.HTML [INTERPRETATION.md]
All numbers come from the CSVs via pandas; the optional interpretation file is
inserted verbatim (as <pre-formatted> prose) after the tables.
"""
import html
import sys
from pathlib import Path

import pandas as pd

before = pd.read_csv(sys.argv[1])
after = pd.read_csv(sys.argv[2])
out = Path(sys.argv[3])
interp = Path(sys.argv[4]).read_text() if len(sys.argv) > 4 else ""

METRICS = ["wall_s", "entities_per_s", "nodes_per_s", "call_p50_ms", "app_dur_p50_ms"]
HIGHER_BETTER = {"entities_per_s", "nodes_per_s"}


def med(df):
    return (df.groupby(["layout", "n_entities"])[METRICS].median().reset_index())


mb, ma = med(before), med(after)
m = mb.merge(ma, on=["layout", "n_entities"], suffixes=("_b", "_a"))

INK, INK2, INK3, RULE = "#12181F", "#4A5563", "#7C8797", "#D6DBE3"
BLUE, ORANGE, GREEN, RED = "#4C72B0", "#DD8452", "#1F7A33", "#B4552D"


def svg_chart(layout, metric, title, w=520, h=300):
    sub = m[m.layout == layout].sort_values("n_entities")
    xs = list(sub.n_entities)
    yb, ya = list(sub[f"{metric}_b"]), list(sub[f"{metric}_a"])
    import math
    ml, mr, mt, mbot = 56, 16, 30, 40
    pw, ph = w - ml - mr, h - mt - mbot
    lx = [math.log10(x) for x in xs]
    x0, x1 = min(lx), max(lx)
    ymax = max(yb + ya) * 1.08 or 1

    def X(v):
        return ml + (math.log10(v) - x0) / (x1 - x0 or 1) * pw

    def Y(v):
        return mt + ph - (v / ymax) * ph

    def poly(ys, color):
        pts = " ".join(f"{X(x):.1f},{Y(y):.1f}" for x, y in zip(xs, ys))
        dots = "".join(f'<circle cx="{X(x):.1f}" cy="{Y(y):.1f}" r="3.5" fill="{color}"/>'
                       for x, y in zip(xs, ys))
        return f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2"/>{dots}'

    gridlines = ""
    for i in range(5):
        gy = mt + ph * i / 4
        val = ymax * (1 - i / 4)
        gridlines += (f'<line x1="{ml}" y1="{gy:.1f}" x2="{w-mr}" y2="{gy:.1f}" stroke="{RULE}"/>'
                      f'<text x="{ml-6}" y="{gy+4:.1f}" text-anchor="end" font-size="10" fill="{INK3}">{val:.3g}</text>')
    xlabels = "".join(f'<text x="{X(x):.1f}" y="{h-mbot+16}" text-anchor="middle" font-size="10" fill="{INK3}">{x}</text>'
                      for x in xs)
    return f'''<svg viewBox="0 0 {w} {h}" style="width:100%;max-width:{w}px;font-family:ui-monospace,Consolas,monospace">
<text x="{ml}" y="16" font-size="12" fill="{INK}" font-weight="600">{html.escape(title)}</text>
{gridlines}{xlabels}
<text x="{w/2:.0f}" y="{h-6}" text-anchor="middle" font-size="10" fill="{INK3}">n_entities (log)</text>
{poly(yb, ORANGE)}{poly(ya, BLUE)}
<rect x="{w-170}" y="{mt}" width="10" height="10" fill="{ORANGE}"/><text x="{w-155}" y="{mt+9}" font-size="10" fill="{INK2}">before</text>
<rect x="{w-100}" y="{mt}" width="10" height="10" fill="{BLUE}"/><text x="{w-85}" y="{mt+9}" font-size="10" fill="{INK2}">after</text>
</svg>'''


def pct_cell(b, a, metric):
    if pd.isna(b) or pd.isna(a) or b == 0:
        return "<td>–</td>"
    pct = (a - b) / b * 100
    better = (pct > 0) if metric in HIGHER_BETTER else (pct < 0)
    color = GREEN if better else RED
    return f'<td style="color:{color};font-weight:600">{pct:+.1f}%</td>'


def table(layout):
    sub = m[m.layout == layout].sort_values("n_entities")
    rows = ""
    for _, r in sub.iterrows():
        for metric in METRICS:
            b, a = r[f"{metric}_b"], r[f"{metric}_a"]
            rows += (f"<tr><td>{r.n_entities}</td><td>{metric}</td>"
                     f"<td>{b:.3g}</td><td>{a:.3g}</td>"
                     f"<td>{(a-b):+.3g}</td>{pct_cell(b, a, metric)}</tr>")
    return f'''<table><thead><tr><th>n</th><th>metric</th><th>before</th><th>after</th><th>Δ</th><th>%</th></tr></thead>
<tbody>{rows}</tbody></table>'''


def guard(df, name):
    bad = df[(df.entities != df.n_entities) | (df.artifacts != df.n_entities) | (df.art_failed > 0)]
    return f"{name}: {len(df)} rows, {len(bad)} guardrail violations"


shas_b = ", ".join(sorted(before.tcb_sha.astype(str).unique()))
shas_a = ", ".join(sorted(after.tcb_sha.astype(str).unique()))
tiled_v = ", ".join(sorted(before.tiled_version.astype(str).unique()))
layouts = sorted(m.layout.unique())

sections = ""
for layout in layouts:
    sections += f'''<h2>{layout}</h2>
<div style="display:flex;gap:24px;flex-wrap:wrap">
{svg_chart(layout, "wall_s", f"{layout}: wall_s (median) vs n")}
{svg_chart(layout, "entities_per_s", f"{layout}: entities/s (median) vs n")}
</div>
{table(layout)}'''

interp_html = f'<h2>Interpretation</h2><div style="max-width:75ch;white-space:pre-wrap">{html.escape(interp)}</div>' if interp else ""

out.write_text(f'''<!doctype html><meta charset="utf-8">
<title>tcb register: before/after ({shas_b} → {shas_a})</title>
<style>
body{{font-family:system-ui,sans-serif;color:{INK};max-width:1150px;margin:2rem auto;padding:0 1rem;background:#fff}}
h1{{font-size:1.5rem}} h2{{font-size:1.1rem;margin-top:2rem;border-bottom:1px solid {RULE};padding-bottom:.3rem}}
table{{border-collapse:collapse;font-family:ui-monospace,Consolas,monospace;font-size:.8rem;margin-top:1rem}}
th,td{{padding:.35em .8em;border-bottom:1px solid {RULE};text-align:right}}
th{{color:{INK3};text-transform:uppercase;font-size:.7rem;letter-spacing:.05em}}
td:nth-child(2){{text-align:left;color:{INK2}}}
.meta{{color:{INK2};font-size:.9rem;line-height:1.5}}
</style>
<h1>HTTP registration: before vs after</h1>
<p class="meta">Local SQLite, fresh DB per (layout, size), stock write-path, workers=8, milano exclusive node.
tiled {tiled_v}. <b>before</b> = tcb {shas_b} · <b>after</b> = tcb {shas_a}.<br>
Fix A (layer 2): <code>assume_new</code> skips the per-entity existence-GET (a guaranteed 404 on fresh keys).<br>
Fix B (layer 1): <code>tcb generate</code> records per-entity shape/dtype in the manifest; registration reads them
instead of opening each HDF5 (<code>get_artifact_info</code> scan) — per-file cost on per_entity layouts.<br>
Reps: median over the schedule (5 @ n≤100, 3 @ n=1000).</p>
{sections}
{interp_html}
<h2>Correctness</h2>
<p class="meta">{guard(before, "before")}<br>{guard(after, "after")}<br>
Guardrail: every row must have entities == artifacts == n_entities and art_failed == 0.
Read-back check (after code): one artifact re-read post-registration; array shape/dtype and
artifact metadata asserted — see the job log for the [readback] line.</p>
''')
print(f"wrote {out}")
