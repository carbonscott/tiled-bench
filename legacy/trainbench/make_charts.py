"""Headline charts for the training-shaped retrieval campaign.

Fig 1 (edrixs): two panels sharing the y-axis — default loader vs tuned loader
(hoisted dataset handles + 256 MB chunk cache). Fig 2 (nips3): one panel.
samples/sec vs worker processes, log-log; warm solid, cold dashed; medians of
guardrail-clean reps only.
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

C = {  # fixed identity -> hue (validated categorical palette, light mode)
    "h5py_plain": "#2a78d6",
    "mode_a_cached": "#eb6834",
    "mode_a": "#1baf7a",
    "http": "#4a3aa7",
}
LABEL = {
    "h5py_plain": "plain h5py (hand-rolled index)",
    "mode_a_cached": "TileWright Mode A (cached handles)",
    "mode_a": "TileWright Mode A (shipped: reopen/sample)",
    "http": "HTTP per-sample (shared server)",
}
INK, MUTED, GRID = "#1a1a19", "#6b6a63", "#e7e6df"
WORKERS = [1, 2, 4, 8, 16, 32]

plt.rcParams.update({
    "font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
    "text.color": INK, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.titlesize": 9.5, "figure.facecolor": "white", "axes.facecolor": "white",
})


def med(df, path, cache, workers):
    r = df[(df.path == path) & (df.cache_state == cache) & (df.workers == workers)]
    return r.samples_per_s.median() if len(r) else None


def draw(ax, df, series, title):
    for path in series:
        caches = ["ambient"] if path == "http" else ["warm", "cold"]
        for cache in caches:
            ys = [med(df, path, cache, w) for w in WORKERS]
            xs = [w for w, y in zip(WORKERS, ys) if y]
            ys = [y for y in ys if y]
            if not ys:
                continue
            ls = "--" if cache == "cold" else "-"
            ax.plot(xs, ys, ls, color=C[path], lw=2, marker="o", ms=4.5,
                    markerfacecolor="white", markeredgecolor=C[path], markeredgewidth=1.6)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(WORKERS, [str(w) for w in WORKERS])
    ax.grid(True, which="major", color=GRID, lw=0.7)
    ax.grid(False, which="minor")
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left")
    ax.set_xlabel("worker processes")
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)


def right_label(ax, y, text, color):
    ax.annotate(text, xy=(1.0, y), xycoords=("axes fraction", "data"),
                xytext=(4, 0), textcoords="offset points", fontsize=7.5,
                color=color, va="center")


df_e = pd.read_csv("results/trainbench/edrixs.csv")
df_e = df_e[df_e.guardrail_ok == 1]
df_n = pd.read_csv("results/trainbench/nips3.csv")
df_n = df_n[df_n.guardrail_ok == 1]

# ── Fig 1: EDRIXS ────────────────────────────────────────────────────────────────
e_default = df_e[(df_e.notes != "dsid_cache_fix") & (df_e.rdcc_mb == 1.0)]
e_tuned = pd.concat([df_e[df_e.notes == "dsid_cache_fix"],
                     df_e[df_e.path == "http"]])

fig, axes = plt.subplots(1, 2, figsize=(9.6, 4.2), sharey=True, dpi=160)
draw(axes[0], e_default, ["h5py_plain", "mode_a_cached", "mode_a", "http"],
     "default loader  (h5py defaults: 1 MB chunk cache)")
draw(axes[1], e_tuned, ["h5py_plain", "mode_a_cached", "http"],
     "tuned loader  (hoisted dataset handles + 256 MB chunk cache)")
axes[0].set_ylabel("samples / second  (shuffled epoch)")
right_label(axes[0], 40, "gzip band-decompress\ntax: ~25 ms/sample", MUTED)
right_label(axes[1], 4400, "epoch fixed costs +\ndecompress once", MUTED)
right_label(axes[1], 72, "server request wall", C["http"])
fig.suptitle("EDRIXS (10,000 samples, 48 KB each, 5 gzip-chunked batched HDF5 files) — "
             "fully shuffled epoch", x=0.01, ha="left", fontsize=11, weight="bold")
fig.text(0.01, 0.905, "warm = solid, cold (client cache evicted) = dashed · "
         "medians over 3–5 reps, milano exclusive node, tiled-test server · "
         "gzip makes warm≈cold: CPU decompression dominates both",
         fontsize=8, color=MUTED)
handles = [plt.Line2D([], [], color=C[p], lw=2, label=LABEL[p])
           for p in ["h5py_plain", "mode_a_cached", "mode_a", "http"]]
handles += [plt.Line2D([], [], color=MUTED, lw=1.6, ls=s, label=l)
            for s, l in [("-", "warm"), ("--", "cold")]]
fig.legend(handles=handles, loc="lower center", ncol=3, frameon=False, fontsize=8,
           bbox_to_anchor=(0.5, -0.02))
fig.tight_layout(rect=(0, 0.06, 0.97, 0.88))
fig.savefig("results/trainbench/edrixs_saturation.png", bbox_inches="tight")

# ── Fig 2: NiPS3 ─────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(7.4, 4.6), dpi=160)
draw(ax, df_n, ["h5py_plain", "mode_a_cached", "mode_a", "http"],
     "")
ax.set_ylabel("samples / second  (shuffled epoch)")
right_label(ax, 8000, "RAM bandwidth\n(24–29 GB/s)", MUTED)
right_label(ax, 1636, "Weka ≥5.4 GB/s/node\n(not yet saturated)", MUTED)
right_label(ax, 60, "server request wall", C["http"])
fig.suptitle("NiPS3 (7,616 samples, ~2.6 MB × 6 artifacts each, one HDF5 file per "
             "sample) — fully shuffled epoch", x=0.01, ha="left", fontsize=11,
             weight="bold")
fig.text(0.01, 0.9, "warm = solid, cold (client cache evicted) = dashed · medians over "
         "3–5 reps · one-time download to local disk\n(egress campaign wire rates): "
         "19.4 GB ≈ 15 s–4 min, then use the h5py lines",
         fontsize=8, color=MUTED)
handles = [plt.Line2D([], [], color=C[p], lw=2, label=LABEL[p])
           for p in ["h5py_plain", "mode_a_cached", "mode_a", "http"]]
handles += [plt.Line2D([], [], color=MUTED, lw=1.6, ls=s, label=l)
            for s, l in [("-", "warm"), ("--", "cold")]]
fig.legend(handles=handles, loc="lower center", ncol=3, frameon=False, fontsize=8,
           bbox_to_anchor=(0.5, -0.04))
fig.tight_layout(rect=(0, 0.05, 0.88, 0.84))
fig.savefig("results/trainbench/nips3_saturation.png", bbox_inches="tight")
print("wrote results/trainbench/{edrixs,nips3}_saturation.png")
