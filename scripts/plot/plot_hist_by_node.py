#!/usr/bin/env python3

import sys
import glob
import pandas as pd
import plotly.graph_objects as go


def plot_file(csv_path: str):
    df = pd.read_csv(csv_path)

    # Only successful requests
    if "ok" in df.columns:
        df = df[df["ok"] == True]

    if df.empty:
        print(f"[warn] no successful rows in {csv_path}")
        return

    fig = go.Figure()

    for node, g in df.groupby("node"):
        lat = g["latency_ms"].astype(float)

        p50 = lat.quantile(0.50)
        p90 = lat.quantile(0.90)
        p99 = lat.quantile(0.99)

        fig.add_trace(
            go.Histogram(
                x=lat,
                name=(
                    f"{node}<br>"
                    f"p50={p50:.1f}ms "
                    f"p90={p90:.1f}ms "
                    f"p99={p99:.1f}ms"
                ),
                opacity=0.6,
                nbinsx=40,
                hovertemplate="Latency: %{x} ms<br>Count: %{y}<extra></extra>",
            )
        )

    fig.update_layout(
        title=f"Client latency histogram by node<br>{csv_path}",
        xaxis_title="Latency (ms)",
        yaxis_title="Count",
        barmode="overlay",
        bargap=0.05,
        legend_title="Node",
    )

    fig.show()


def main():
    if len(sys.argv) < 2:
        print("usage:")
        print("  plot_hist_by_node.py experiment_results/runs/*.csv")
        sys.exit(1)

    paths = []
    for arg in sys.argv[1:]:
        paths.extend(glob.glob(arg))

    if not paths:
        print("no CSV files matched")
        sys.exit(1)

    for path in sorted(paths):
        print(f"[plot] {path}")
        plot_file(path)


if __name__ == "__main__":
    main()
