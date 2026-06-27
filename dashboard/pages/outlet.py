"""
pages/outlet.py  –  Analisa Outlet & Deviasi Cost of Material (via Datamart)
"""
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, callback
import plotly.graph_objects as go
import pandas as pd

from config import query_df, REFRESH_INTERVAL_MS
from components.filters import (
    top_bar, date_filter_bar, auto_refresh, section_heading,
    empty_fig, CHART_LAYOUT, chart_layout, PALETTE, BRAND_MID, MUTED, BRAND_TEXT,
)

def layout():
    cabang_opts = _load_cabang()
    return html.Div([
        top_bar("Analisa Outlet & Deviasi"),
        dbc.Container(fluid=True, className="page-container", children=[
            date_filter_bar("outlet", cabang_options=cabang_opts),
            auto_refresh("outlet", REFRESH_INTERVAL_MS),
            dbc.Row(id="outlet-kpi-row", className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Deviasi Actual vs BOM per Cabang", "⚡"), dcc.Graph(id="outlet-deviation-bar", config={"displayModeBar": False})], md=8),
                dbc.Col([section_heading("CoM Ratio (Actual/BOM)", "🎯"), dcc.Graph(id="outlet-ratio-gauge", config={"displayModeBar": False})], md=4),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Tren Deviasi Harian (Actual − BOM)", "📉"), dcc.Graph(id="outlet-deviation-trend", config={"displayModeBar": False})]),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Top 10 Item dengan Deviasi Tertinggi", "🔴"), dcc.Graph(id="outlet-top-deviation-item", config={"displayModeBar": False})], md=6),
                dbc.Col([section_heading("Perbandingan Cost per Menu", "🍽️"), dcc.Graph(id="outlet-menu-cost", config={"displayModeBar": False})], md=6),
            ], className="g-3 mb-3"),
        ]),
    ])

def _load_cabang():
    df = query_df("SELECT id_cabang, nama_cabang FROM default.dim_cabang ORDER BY nama_cabang")
    if df.empty: return []
    return [{"label": r.nama_cabang, "value": r.id_cabang} for _, r in df.iterrows()]

@callback(
    Output("outlet-kpi-row",             "children"),
    Output("outlet-deviation-bar",       "figure"),
    Output("outlet-ratio-gauge",         "figure"),
    Output("outlet-deviation-trend",     "figure"),
    Output("outlet-top-deviation-item",  "figure"),
    Output("outlet-menu-cost",           "figure"),
    Input("outlet-refresh",              "n_clicks"),
    Input("outlet-auto-interval",        "n_intervals"),
    State("outlet-daterange",            "start_date"),
    State("outlet-daterange",            "end_date"),
    State("outlet-cabang",               "value"),
    prevent_initial_call=False,
)
def update_outlet(n_clicks, n_intervals, start, end, cabangs):
    if not start or not end: return [], empty_fig(), empty_fig(), empty_fig(), empty_fig(), empty_fig()

    clauses = [f"tanggal BETWEEN '{start}' AND '{end}'"]
    if cabangs: clauses.append(f"id_cabang IN ({','.join(str(c) for c in cabangs)})")
    where = "WHERE " + " AND ".join(clauses)

    kpi_sql = f"""
        SELECT sum(actual_cost) AS total_actual, sum(bom_cost) AS total_bom,
               sum(actual_cost) - sum(bom_cost) AS deviasi,
               (sum(actual_cost) / nullIf(sum(bom_cost), 0)) * 100 AS ratio_pct
        FROM default.dm_outlet_daily {where}
    """
    kdf = query_df(kpi_sql)
    if not kdf.empty and pd.notna(kdf.iloc[0].total_actual):
        r = kdf.iloc[0]
        actual = f"Rp {r.total_actual/1_000_000:.1f}M"
        bom    = f"Rp {r.total_bom/1_000_000:.1f}M"
        dev    = f"Rp {r.deviasi/1_000_000:.1f}M"
        ratio  = f"{r.ratio_pct:.1f}%"
        dev_color = "#fb7185" if r.deviasi > 0 else "#4ade80"
    else:
        actual = bom = dev = ratio = "—"
        dev_color = "#4ade80"

    kpi_row = dbc.Row([
        _kpi("Actual Cost",  actual, "💸", "#f97316"), _kpi("BOM Cost",     bom,    "📐", "#38bdf8"),
        _kpi("Total Deviasi",dev,    "⚡", dev_color), _kpi("CoM Ratio",    ratio,  "🎯", "#facc15"),
    ], className="g-3")

    dev_cab_sql = f"""
        SELECT c.nama_cabang AS cabang, sum(com.actual_cost) AS actual, sum(com.bom_cost) AS bom,
               sum(com.actual_cost) - sum(com.bom_cost) AS deviasi
        FROM default.dm_outlet_daily com
        LEFT JOIN default.dim_cabang c ON com.id_cabang = c.id_cabang
        {where} GROUP BY cabang ORDER BY deviasi DESC
    """
    dcdf = query_df(dev_cab_sql)
    if dcdf.empty: fig_dev_bar = empty_fig()
    else:
        fig_dev_bar = go.Figure()
        fig_dev_bar.add_trace(go.Bar(name="Actual", x=dcdf["cabang"], y=dcdf["actual"], marker_color=PALETTE[0]))
        fig_dev_bar.add_trace(go.Bar(name="BOM", x=dcdf["cabang"], y=dcdf["bom"], marker_color=PALETTE[1]))
        fig_dev_bar.update_layout(**chart_layout(barmode="group", legend=dict(orientation="h", y=1.1)))

    try: ratio_val = float(kdf.iloc[0].ratio_pct) if not kdf.empty else 100
    except Exception: ratio_val = 100

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number+delta", value=ratio_val, delta={"reference": 100, "valueformat": ".1f"},
        number={"suffix": "%", "font": {"color": BRAND_TEXT}},
        gauge={
            "axis": {"range": [80, 130], "tickcolor": MUTED}, "bar": {"color": PALETTE[0]},
            "bgcolor": BRAND_MID, "bordercolor": "rgba(255,255,255,0.1)",
            "steps": [{"range": [80, 100], "color": "rgba(74,222,128,0.15)"}, {"range": [100, 110], "color": "rgba(250,204, 21,0.15)"}, {"range": [110, 130], "color": "rgba(251, 113,133,0.15)"}],
            "threshold": {"line": {"color": "#4ade80", "width": 3}, "value": 100},
        },
        title={"text": "Actual/BOM", "font": {"color": MUTED, "size": 12}},
    ))
    fig_gauge.update_layout(**chart_layout(height=280))

    trend_sql = f"SELECT tanggal, sum(actual_cost) - sum(bom_cost) AS deviasi FROM default.dm_outlet_daily {where} GROUP BY tanggal ORDER BY tanggal"
    tdf = query_df(trend_sql)
    if tdf.empty: fig_trend = empty_fig()
    else:
        colors = ["#fb7185" if v > 0 else "#4ade80" for v in tdf["deviasi"]]
        fig_trend = go.Figure(go.Bar(x=tdf["tanggal"], y=tdf["deviasi"], marker_color=colors, name="Deviasi"))
        fig_trend.add_hline(y=0, line_dash="dash", line_color=MUTED)
        fig_trend.update_layout(**chart_layout())

    top_item_sql = f"""
        SELECT i.nama_item AS item, sum(com.actual_cost) - sum(com.bom_cost) AS deviasi
        FROM default.dm_outlet_daily com
        LEFT JOIN default.dim_item i ON com.id_item = i.id_item
        {where} GROUP BY item ORDER BY deviasi DESC LIMIT 10
    """
    tidf = query_df(top_item_sql)
    if tidf.empty: fig_top_item = empty_fig()
    else:
        c = ["#fb7185" if v > 0 else "#4ade80" for v in tidf["deviasi"]]
        fig_top_item = go.Figure(go.Bar(x=tidf["deviasi"], y=tidf["item"], orientation="h", marker_color=c, text=tidf["deviasi"].apply(lambda v: f"Rp {v/1e6:.2f}M"), textposition="inside"))
        fig_top_item.update_layout(**chart_layout(yaxis=dict(autorange="reversed")))

    menu_sql = f"""
        SELECT m.nama_menu AS menu, sum(com.actual_cost) AS actual, sum(com.bom_cost) AS bom
        FROM default.dm_outlet_daily com
        LEFT JOIN default.dim_menu m ON com.id_menu = m.id_menu
        {where} GROUP BY menu ORDER BY actual DESC LIMIT 10
    """
    mcdf = query_df(menu_sql)
    if mcdf.empty: fig_menu = empty_fig()
    else:
        fig_menu = go.Figure()
        fig_menu.add_trace(go.Bar(name="Actual", x=mcdf["menu"], y=mcdf["actual"], marker_color=PALETTE[0]))
        fig_menu.add_trace(go.Bar(name="BOM", x=mcdf["menu"], y=mcdf["bom"], marker_color=PALETTE[1]))
        fig_menu.update_layout(**chart_layout(barmode="group"))

    return kpi_row, fig_dev_bar, fig_gauge, fig_trend, fig_top_item, fig_menu

def _kpi(title, value, icon, color):
    return dbc.Col(dbc.Card(
        style={"borderTop": f"3px solid {color}", "background": BRAND_MID}, className="kpi-card",
        children=dbc.CardBody([
            html.Div([html.Span(icon, className="kpi-icon"), html.P(title, className="kpi-title")], className="d-flex align-items-center gap-2 mb-1"),
            html.H4(value, className="kpi-value"),
        ]),
    ), md=3, sm=6, xs=12)