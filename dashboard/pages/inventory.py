"""
pages/inventory.py  –  Analisa Inventori & Stok per Cabang (via Datamart)
Focus: item dengan stok paling sedikit per cabang → restock alert.
"""
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, callback
import plotly.graph_objects as go
import pandas as pd

from config import query_df, REFRESH_INTERVAL_MS
from components.filters import (
    top_bar, date_filter_bar, auto_refresh, section_heading,
    empty_fig, chart_layout, PALETTE, BRAND_MID, MUTED, BRAND_TEXT, BRAND_ACCENT,
)

LOW_STOCK_QTY = 10

def layout():
    cabang_opts = _load_cabang()
    item_opts   = _load_item()
    return html.Div([
        top_bar("Analisa Inventori & Stok per Cabang"),
        dbc.Container(fluid=True, className="page-container", children=[
            date_filter_bar(
                "inventory", cabang_options=cabang_opts,
                extra_filters=[
                    dbc.Col([
                        html.Label("Item", className="filter-label"),
                        dcc.Dropdown(id="inventory-item", options=item_opts, multi=True, placeholder="Semua item", className="dash-dropdown-dark"),
                    ], md=3),
                    dbc.Col([
                        html.Label("Batas Stok Kritis", className="filter-label"),
                        dbc.Input(id="inventory-low-threshold", type="number", value=LOW_STOCK_QTY, min=0, style={"background": "#334155", "color": BRAND_TEXT, "border": "1px solid rgba(255,255,255,0.1)"}),
                    ], md=2),
                ],
            ),
            auto_refresh("inventory", REFRESH_INTERVAL_MS),
            dbc.Row(id="inventory-kpi-row", className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("⚠️  Item Stok Kritis per Cabang (perlu restock segera)", ""), dcc.Graph(id="inventory-low-stock", config={"displayModeBar": False}, style={"height": "420px"})]),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Nilai Stok Akhir per Cabang", "🏦"), dcc.Graph(id="inventory-cabang-value", config={"displayModeBar": False})], md=6),
                dbc.Col([section_heading("Qty Stok Akhir per Cabang — Top 15 Item", "📦"), dcc.Graph(id="inventory-heatmap", config={"displayModeBar": False})], md=6),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Mutasi Stok Harian (Masuk vs Keluar) per Cabang", "🔄"), dcc.Graph(id="inventory-flow-chart", config={"displayModeBar": False})], md=8),
                dbc.Col([section_heading("Proporsi Stok per Kategori Item", "🥧"), dcc.Graph(id="inventory-cat-pie", config={"displayModeBar": False})], md=4),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Inventory Turnover Rate per Item (Top 15)", "♻️"), dcc.Graph(id="inventory-turnover", config={"displayModeBar": False})]),
            ], className="g-3 mb-3"),
        ]),
    ])

def _load_cabang():
    df = query_df("SELECT id_cabang, nama_cabang FROM default.dim_cabang ORDER BY nama_cabang")
    if df.empty: return []
    return [{"label": r.nama_cabang, "value": r.id_cabang} for _, r in df.iterrows()]

def _load_item():
    df = query_df("SELECT id_item, nama_item FROM default.dim_item ORDER BY nama_item LIMIT 300")
    if df.empty: return []
    return [{"label": r.nama_item, "value": r.id_item} for _, r in df.iterrows()]

@callback(
    Output("inventory-kpi-row",      "children"),
    Output("inventory-low-stock",    "figure"),
    Output("inventory-cabang-value", "figure"),
    Output("inventory-heatmap",      "figure"),
    Output("inventory-flow-chart",   "figure"),
    Output("inventory-cat-pie",      "figure"),
    Output("inventory-turnover",     "figure"),
    Input("inventory-refresh",           "n_clicks"),
    Input("inventory-auto-interval",     "n_intervals"),
    State("inventory-daterange",         "start_date"),
    State("inventory-daterange",         "end_date"),
    State("inventory-cabang",            "value"),
    State("inventory-item",              "value"),
    State("inventory-low-threshold",     "value"),
    prevent_initial_call=False,
)
def update_inventory(n_clicks, n_intervals, start, end, cabangs, items, threshold):
    ef = empty_fig()
    if not start or not end: return [], ef, ef, ef, ef, ef, ef
    threshold = threshold if threshold is not None else LOW_STOCK_QTY

    cl = [f"tanggal BETWEEN '{start}' AND '{end}'"]
    if cabangs: cl.append(f"id_cabang IN ({','.join(str(c) for c in cabangs)})")
    if items: cl.append(f"id_item IN ({','.join(str(i) for i in items)})")
    where = "WHERE " + " AND ".join(cl)

    # ── KPIs (Dari Datamart)
    kpi_sql = f"""
        SELECT sum(nilai_stok) AS total_nilai, sum(qty_masuk) AS total_masuk, sum(qty_keluar) AS total_keluar
        FROM default.dm_inventory_daily {where}
    """
    kdf = query_df(kpi_sql)
    r = kdf.iloc[0] if not kdf.empty else None

    low_sql = f"""
        SELECT count(DISTINCT tuple(id_cabang, id_item)) AS low_items
        FROM (
            SELECT id_cabang, id_item, argMaxMerge(qty_akhir) AS q_akhir
            FROM default.dm_inventory_daily {where} GROUP BY id_cabang, id_item
        ) WHERE q_akhir <= {threshold} AND q_akhir >= 0
    """
    lkdf = query_df(low_sql)
    low_items = int(lkdf.iloc[0]["low_items"]) if not lkdf.empty else 0

    kpi_row = dbc.Row([
        _kpi("Nilai Stok Akhir",   f"Rp {r.total_nilai/1e6:.1f}M" if pd.notna(r.total_nilai) else "—", "🏦", "#4ade80"),
        _kpi("Total Qty Masuk",    f"{r.total_masuk:,.0f}"        if pd.notna(r.total_masuk) else "—", "⬆️", "#38bdf8"),
        _kpi("Total Qty Keluar",   f"{r.total_keluar:,.0f}"       if pd.notna(r.total_keluar) else "—", "⬇️", "#fb7185"),
        _kpi(f"Item Stok ≤ {threshold}", str(low_items), "⚠️", "#f97316"),
    ], className="g-3")

    alert_sql = f"""
        SELECT c.nama_cabang AS cabang, i.nama_item AS item, i.satuan AS satuan,
               argMaxMerge(np.qty_akhir) AS final_qty
        FROM default.dm_inventory_daily np
        LEFT JOIN default.dim_cabang c ON np.id_cabang = c.id_cabang
        LEFT JOIN default.dim_item   i ON np.id_item   = i.id_item
        {where} GROUP BY cabang, item, satuan
        HAVING final_qty <= {threshold} AND final_qty >= 0 ORDER BY final_qty ASC LIMIT 60
    """
    adf = query_df(alert_sql)
    if adf.empty: fig_low = empty_fig("✅  Tidak ada item dengan stok kritis pada periode ini.")
    else:
        def alert_color(q): return "#fb7185" if q == 0 else ("#f97316" if q <= threshold / 2 else "#facc15")
        colors = [alert_color(q) for q in adf["final_qty"]]
        fig_low = go.Figure(go.Bar(
            x=adf["final_qty"], y=adf["cabang"] + " · " + adf["item"], orientation="h", marker_color=colors,
            text=adf.apply(lambda r: f"{r.final_qty:.1f} {r.satuan}", axis=1), textposition="outside",
            hovertemplate="<b>%{y}</b><br>Stok: %{x:.1f}<extra></extra>",
        ))
        fig_low.add_vline(x=threshold, line_dash="dot", line_color="#facc15", line_width=2, annotation_text=f"Threshold ({threshold})", annotation_font_color="#facc15")
        fig_low.update_layout(**chart_layout(yaxis=dict(autorange="reversed"), xaxis=dict(title="Qty Stok Akhir"), height=max(360, len(adf) * 26)))

    vcab_sql = f"""
        SELECT c.nama_cabang AS cabang, sum(np.nilai_stok) AS nilai
        FROM default.dm_inventory_daily np
        LEFT JOIN default.dim_cabang c ON np.id_cabang = c.id_cabang
        {where} GROUP BY cabang ORDER BY nilai DESC
    """
    vcdf = query_df(vcab_sql)
    if vcdf.empty: fig_vcab = ef
    else:
        fig_vcab = go.Figure(go.Bar(x=vcdf["cabang"], y=vcdf["nilai"], marker_color=PALETTE * 5, text=vcdf["nilai"].apply(lambda v: f"Rp {v/1e6:.1f}M"), textposition="outside"))
        fig_vcab.update_layout(**chart_layout())

    heat_sql = f"""
        SELECT c.nama_cabang AS cabang, i.nama_item AS item, argMaxMerge(np.qty_akhir) AS final_qty
        FROM default.dm_inventory_daily np
        LEFT JOIN default.dim_cabang c ON np.id_cabang = c.id_cabang
        LEFT JOIN default.dim_item i ON np.id_item = i.id_item
        {where} GROUP BY cabang, item
    """
    hdf = query_df(heat_sql)
    if hdf.empty: fig_heat = ef
    else:
        top15 = hdf.groupby("item")["final_qty"].sum().nlargest(15).index.tolist()
        hdf15 = hdf[hdf["item"].isin(top15)]
        pivot = hdf15.pivot_table(index="item", columns="cabang", values="final_qty", fill_value=0)
        fig_heat = go.Figure(go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale=[[0.0, "#fb7185"], [0.2, "#f97316"], [0.5, "#facc15"], [1.0, "#4ade80"]],
            text=pivot.values.round(1), texttemplate="%{text}", textfont=dict(size=9, color="white"),
            hovertemplate="<b>%{y}</b><br>%{x}<br>Qty: %{z:.1f}<extra></extra>", showscale=True, colorbar=dict(title="Qty", tickfont=dict(color=MUTED)),
        ))
        fig_heat.update_layout(**chart_layout(height=max(300, len(top15) * 30), margin=dict(l=160, r=20, t=20, b=60)))

    flow_sql = f"""
        SELECT c.nama_cabang AS cabang, np.tanggal AS tgl, sum(np.qty_masuk) AS masuk, sum(np.qty_keluar) AS keluar
        FROM default.dm_inventory_daily np
        LEFT JOIN default.dim_cabang c ON np.id_cabang = c.id_cabang
        {where} GROUP BY cabang, tgl ORDER BY tgl
    """
    fdf = query_df(flow_sql)
    if fdf.empty: fig_flow = ef
    else:
        fig_flow = go.Figure()
        for i, cab in enumerate(fdf["cabang"].unique()):
            sub = fdf[fdf["cabang"] == cab]
            fig_flow.add_trace(go.Scatter(x=sub["tgl"], y=sub["masuk"], name=f"{cab} Masuk", line=dict(color=PALETTE[i % len(PALETTE)], width=1.5), legendgroup=cab))
            fig_flow.add_trace(go.Scatter(x=sub["tgl"], y=-sub["keluar"], name=f"{cab} Keluar", line=dict(color=PALETTE[i % len(PALETTE)], width=1.5, dash="dot"), legendgroup=cab, showlegend=False))
        fig_flow.add_hline(y=0, line_color=MUTED, line_dash="dash")
        fig_flow.update_layout(**chart_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.2, font=dict(size=10))))

    cat_sql = f"""
        SELECT i.kategori AS kategori, sum(np.nilai_stok) AS nilai
        FROM default.dm_inventory_daily np
        LEFT JOIN default.dim_item i ON np.id_item = i.id_item
        {where} GROUP BY kategori ORDER BY nilai DESC
    """
    cpdf = query_df(cat_sql)
    if cpdf.empty: fig_pie = ef
    else:
        fig_pie = go.Figure(go.Pie(labels=cpdf["kategori"], values=cpdf["nilai"], hole=0.42, marker=dict(colors=PALETTE * 3), textinfo="label+percent", textfont=dict(color=BRAND_TEXT)))
        fig_pie.update_layout(**chart_layout(showlegend=False))

    # Average dari argMaxMerge tidak dimungkinkan dalam satu query sederhana ClickHouse. 
    # Kita menggunakan data yg diagregasi di Pandas untuk menghitung turnover rate.
    to_sql = f"""
        SELECT i.nama_item AS item, sum(np.qty_keluar) AS out_qty, avg(argMaxMerge(np.qty_akhir)) AS avg_stock
        FROM default.dm_inventory_daily np
        LEFT JOIN default.dim_item i ON np.id_item = i.id_item
        {where} GROUP BY item HAVING avg_stock > 0 ORDER BY out_qty DESC LIMIT 15
    """
    todf = query_df(to_sql)
    if todf.empty: fig_to = ef
    else:
        todf["turnover"] = todf["out_qty"] / todf["avg_stock"]
        todf = todf.sort_values("turnover", ascending=False)
        fig_to = go.Figure(go.Bar(x=todf["item"], y=todf["turnover"], marker=dict(color=todf["turnover"], colorscale="YlOrRd", showscale=True, colorbar=dict(title="Rate", tickfont=dict(color=MUTED)))))
        fig_to.update_layout(**chart_layout(xaxis=dict(tickangle=-30)))

    return kpi_row, fig_low, fig_vcab, fig_heat, fig_flow, fig_pie, fig_to

def _kpi(title, value, icon, color):
    return dbc.Col(dbc.Card(
        style={"borderTop": f"3px solid {color}", "background": BRAND_MID}, className="kpi-card",
        children=dbc.CardBody([
            html.Div([html.Span(icon, className="kpi-icon"), html.P(title, className="kpi-title")], className="d-flex align-items-center gap-2 mb-1"),
            html.H4(value, className="kpi-value"),
        ]),
    ), md=3, sm=6, xs=12)