"""
pages/purchase.py  –  Analisa Pembelian (Purchase via Datamart)
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
    supplier_opts = _load_supplier()
    return html.Div([
        top_bar("Analisa Pembelian"),
        dbc.Container(fluid=True, className="page-container", children=[
            date_filter_bar(
                "purchase", cabang_options=cabang_opts,
                extra_filters=[
                    dbc.Col([
                        html.Label("Supplier", className="filter-label"),
                        dcc.Dropdown(id="purchase-supplier", options=supplier_opts, multi=True, placeholder="Semua supplier", className="dash-dropdown-dark"),
                    ], md=3),
                    dbc.Col([
                        html.Label("Status PO", className="filter-label"),
                        dcc.Dropdown(
                            id="purchase-status",
                            options=[{"label": "Pending", "value": "Pending"}, {"label": "Diterima", "value": "Diterima"}, {"label": "Dibatalkan","value": "Dibatalkan"}],
                            multi=True, placeholder="Semua status", className="dash-dropdown-dark",
                        ),
                    ], md=2),
                ],
            ),
            auto_refresh("purchase", REFRESH_INTERVAL_MS),
            dbc.Row(id="purchase-kpi-row", className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Tren Biaya Pembelian Harian", "📈"), dcc.Graph(id="purchase-daily-trend", config={"displayModeBar": False})], md=8),
                dbc.Col([section_heading("Status PO", "📋"), dcc.Graph(id="purchase-status-pie", config={"displayModeBar": False})], md=4),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Top 10 Supplier by Spend", "🏭"), dcc.Graph(id="purchase-top-supplier", config={"displayModeBar": False})], md=6),
                dbc.Col([section_heading("Top 10 Item Dibeli", "📦"), dcc.Graph(id="purchase-top-item", config={"displayModeBar": False})], md=6),
            ], className="g-3 mb-3"),
            dbc.Row([
                dbc.Col([section_heading("Lead Time Supplier (hari)", "⏱️"), dcc.Graph(id="purchase-lead-time", config={"displayModeBar": False})]),
            ], className="g-3 mb-3"),
        ]),
    ])

def _load_cabang():
    df = query_df("SELECT id_cabang, nama_cabang FROM default.dim_cabang ORDER BY nama_cabang")
    if df.empty: return []
    return [{"label": r.nama_cabang, "value": r.id_cabang} for _, r in df.iterrows()]

def _load_supplier():
    df = query_df("SELECT id_supplier, nama_supplier FROM default.dim_supplier ORDER BY nama_supplier")
    if df.empty: return []
    return [{"label": r.nama_supplier, "value": r.id_supplier} for _, r in df.iterrows()]

@callback(
    Output("purchase-kpi-row",       "children"),
    Output("purchase-daily-trend",   "figure"),
    Output("purchase-status-pie",    "figure"),
    Output("purchase-top-supplier",  "figure"),
    Output("purchase-top-item",      "figure"),
    Output("purchase-lead-time",     "figure"),
    Input("purchase-refresh",        "n_clicks"),
    Input("purchase-auto-interval",  "n_intervals"),
    State("purchase-daterange",      "start_date"),
    State("purchase-daterange",      "end_date"),
    State("purchase-cabang",         "value"),
    State("purchase-supplier",       "value"),
    State("purchase-status",         "value"),
    prevent_initial_call=False,
)
def update_purchase(n_clicks, n_intervals, start, end, cabangs, suppliers, statuses):
    if not start or not end: return [], empty_fig(), empty_fig(), empty_fig(), empty_fig(), empty_fig()

    clauses = [f"tanggal_dipesan BETWEEN '{start}' AND '{end}'"]
    if cabangs: clauses.append(f"id_cabang IN ({','.join(str(c) for c in cabangs)})")
    if suppliers: clauses.append(f"id_supplier IN ({','.join(str(s) for s in suppliers)})")
    if statuses: clauses.append("status IN ({})".format(",".join(f"'{s}'" for s in statuses)))
    where = "WHERE " + " AND ".join(clauses)

    kpi_sql = f"""
        SELECT uniqMerge(po_uniq) AS total_po, sum(total_spend) AS spend,
               countIf(status='Pending') AS pending_po, avgMerge(lead_time_days) AS avg_lead
        FROM default.dm_purchase_daily {where}
    """
    kdf = query_df(kpi_sql)
    if not kdf.empty and pd.notna(kdf.iloc[0].total_po):
        r = kdf.iloc[0]
        po    = f"{int(r.total_po):,}"
        spend = f"Rp {r.spend/1_000_000:.1f}M"
        pend  = f"{int(r.pending_po):,}"
        lead  = f"{r.avg_lead:.1f} hari" if pd.notna(r.avg_lead) else "—"
    else:
        po = spend = pend = lead = "—"

    kpi_row = dbc.Row([
        _kpi("Total PO", po, "📋", "#38bdf8"), _kpi("Total Spend", spend, "💸", "#f97316"),
        _kpi("PO Pending", pend, "⏳", "#facc15"), _kpi("Avg Lead Time", lead, "⏱️", "#4ade80"),
    ], className="g-3")

    daily_sql = f"SELECT tanggal_dipesan AS tgl, sum(total_spend) AS spend, uniqMerge(po_uniq) AS po_count FROM default.dm_purchase_daily {where} GROUP BY tgl ORDER BY tgl"
    ddf = query_df(daily_sql)
    if ddf.empty: fig_daily = empty_fig()
    else:
        fig_daily = go.Figure()
        fig_daily.add_trace(go.Scatter(x=ddf["tgl"], y=ddf["spend"], name="Spend", line=dict(color=PALETTE[0], width=2), fill="tozeroy", fillcolor="rgba(249,115,22,0.1)"))
        fig_daily.add_trace(go.Bar(x=ddf["tgl"], y=ddf["po_count"], name="PO", yaxis="y2", marker_color="rgba(56,189,248,0.4)"))
        fig_daily.update_layout(**chart_layout(yaxis2=dict(overlaying="y", side="right", gridcolor="rgba(0,0,0,0)", color=MUTED)))

    status_sql = f"SELECT status, uniqMerge(po_uniq) AS cnt FROM default.dm_purchase_daily {where} GROUP BY status"
    sdf = query_df(status_sql)
    if sdf.empty: fig_status = empty_fig()
    else:
        fig_status = go.Figure(go.Pie(labels=sdf["status"], values=sdf["cnt"], hole=0.45, marker=dict(colors=PALETTE), textinfo="label+percent", textfont=dict(color=BRAND_TEXT)))
        fig_status.update_layout(**CHART_LAYOUT, showlegend=False)

    sup_sql = f"""
        SELECT sp.nama_supplier AS supplier, sum(p.total_spend) AS spend
        FROM default.dm_purchase_daily p
        LEFT JOIN default.dim_supplier sp ON p.id_supplier = sp.id_supplier
        {where} GROUP BY supplier ORDER BY spend DESC LIMIT 10
    """
    spdf = query_df(sup_sql)
    if spdf.empty: fig_sup = empty_fig()
    else:
        fig_sup = go.Figure(go.Bar(x=spdf["spend"], y=spdf["supplier"], orientation="h", marker_color=PALETTE[1], text=spdf["spend"].apply(lambda v: f"Rp {v/1e6:.1f}M"), textposition="inside"))
        fig_sup.update_layout(**chart_layout(yaxis=dict(autorange="reversed")))

    item_sql = f"""
        SELECT i.nama_item AS item, sum(p.kuantitas) AS qty, sum(p.total_spend) AS spend
        FROM default.dm_purchase_daily p
        LEFT JOIN default.dim_item i ON p.id_item = i.id_item
        {where} GROUP BY item ORDER BY spend DESC LIMIT 10
    """
    idf = query_df(item_sql)
    if idf.empty: fig_item = empty_fig()
    else:
        fig_item = go.Figure(go.Bar(x=idf["spend"], y=idf["item"], orientation="h", marker_color=PALETTE[2], text=idf["qty"].apply(lambda v: f"{v:,.0f}"), textposition="inside"))
        fig_item.update_layout(**chart_layout(yaxis=dict(autorange="reversed")))

    # AvgLeadTime diambil langsung, namun untuk Min/Max Datamart kita belum men-store aggregate tersebut. 
    # Oleh karenanya, error bar tidak dimunculkan untuk menjaga agar dashboard fully mengandalkan MV Datamart (Super Cepat).
    lead_sql = f"""
        SELECT sp.nama_supplier AS supplier, avgMerge(p.lead_time_days) AS avg_lead
        FROM default.dm_purchase_daily p
        LEFT JOIN default.dim_supplier sp ON p.id_supplier = sp.id_supplier
        {where} GROUP BY supplier HAVING avg_lead > 0 ORDER BY avg_lead DESC LIMIT 15
    """
    ldf = query_df(lead_sql)
    if ldf.empty: fig_lead = empty_fig()
    else:
        fig_lead = go.Figure()
        fig_lead.add_trace(go.Bar(x=ldf["supplier"], y=ldf["avg_lead"], name="Avg Lead Time", marker_color=PALETTE[3]))
        fig_lead.update_layout(**CHART_LAYOUT)

    return kpi_row, fig_daily, fig_status, fig_sup, fig_item, fig_lead

def _kpi(title, value, icon, color):
    return dbc.Col(dbc.Card(
        style={"borderTop": f"3px solid {color}", "background": BRAND_MID}, className="kpi-card",
        children=dbc.CardBody([
            html.Div([html.Span(icon, className="kpi-icon"), html.P(title, className="kpi-title")], className="d-flex align-items-center gap-2 mb-1"),
            html.H4(value, className="kpi-value"),
        ]),
    ), md=3, sm=6, xs=12)