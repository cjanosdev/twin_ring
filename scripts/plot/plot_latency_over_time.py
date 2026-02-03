#!/usr/bin/env python3
import sys, glob
import pandas as pd
import plotly.graph_objects as go

def parse_ok(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin(["true", "1", "t", "yes", "y"])

def plot_latency_by_order(csv_path: str, smooth_window: int = 1):
    df = pd.read_csv(csv_path)

    # filter "ok" robustly (handles "true"/"false" strings)
    if "ok" in df.columns:
        df["ok"] = parse_ok(df["ok"])
        df = df[df["ok"]]

    # ensure numeric
    df["latency_ms"] = pd.to_numeric(df["latency_ms"], errors="coerce")
    df = df.dropna(subset=["latency_ms"])

    if df.empty:
        print(f"[warn] no rows to plot in {csv_path} after filtering")
        return

    # Use finished_at for stable ordering if present; otherwise just keep file order
    if "finished_at" in df.columns:
        df["finished_at"] = pd.to_datetime(df["finished_at"], utc=True, errors="coerce")

    fig = go.Figure()

    for node, g in df.groupby("node"):
        # stable order: by finished_at if available, else by original order
        if "finished_at" in g.columns and g["finished_at"].notna().any():
            g = g.sort_values("finished_at")
        else:
            g = g.copy()

        # request order index for this node
        g = g.reset_index(drop=True)
        g["req_idx"] = g.index

        y = g["latency_ms"]
        if smooth_window > 1:
            # rolling median is good for latency
            y = y.rolling(window=smooth_window, min_periods=1).median()

        fig.add_trace(go.Scatter(
            x=g["req_idx"],
            y=y,
            mode="lines+markers",
            name=node,
            marker=dict(size=5),
        ))

        print(f"[debug] {node}: {len(g)} points")

    fig.update_layout(
        title=f"Latency by request order (one line per node, smooth={smooth_window})<br>{csv_path}",
        xaxis_title="Request index (per node)",
        yaxis_title="Latency (ms)",
        legend_title="Node",
    )

    fig.show()

def main():
    if len(sys.argv) < 2:
        print("usage: python plot_latency_by_order.py <csv_or_glob> [smooth_window]")
        print('example: python plot_latency_by_order.py "../../experiment_results/runs/*/*.csv" 5')
        sys.exit(1)

    pattern = sys.argv[1]
    smooth = int(sys.argv[2]) if len(sys.argv) >= 3 else 1

    paths = sorted(glob.glob(pattern))
    if not paths:
        print("no CSV files matched")
        sys.exit(1)

    for p in paths:
        print(f"[plot] {p}")
        plot_latency_by_order(p, smooth_window=smooth)

if __name__ == "__main__":
    main()
