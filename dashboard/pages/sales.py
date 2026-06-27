"""
pages/sales.py  –  Analisa Penjualan per Cabang (Hourly Trend - Fixed Format via Datamart)
Focus: perbandingan antar cabang, kenaikan/penurunan, tren per jam (Format: HH:00).
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

def layout():
    cabang_opts = _load_cabang()
    return html.Div([
        top_bar("Analisa Penjualan per Cabang"),
        dbc.Container(fluid=True, className="page-container", children=[
            date_filter_bar(
                "sales",
                cabang_options=cabang_opts,
                extra_filters=[
                    dbc.Col([
                        html.Label("Kategori Bayar", className="filter-label"),
                        dcc.Dropdown(
                            id="sales-payment",
                            options=[
                                {"label": "Cash",     "value": "Cash"},
                                {"label": "Non-Cash", "value": "Non-Cash"},
                                {"label": "Transfer", "value": "Transfer"},
                            ],
                            multi=True,
                            placeholder="Semua",
                            className="dash-dropdown-dark",
                        ),
                    ], md=3),
                ],
            ),
            auto_refresh("sales", REFRESH_INTERVAL_MS),

            dbc.Row(id="sales-kpi-row", className="g-3 mb-3"),

            dbc.Row([
                dbc.Col([
                    section_heading("Revenue Total per Cabang", "🏪"),
                    dcc.Graph(id="sales-cabang-total", config={"displayModeBar": False}),
                ], md=6),
                dbc.Col([
                    section_heading("Kenaikan / Penurunan vs Periode Sebelumnya", "📊"),
                    dcc.Graph(id="sales-cabang-delta", config={"displayModeBar": False}),
                ], md=6),
            ], className="g-3 mb-3"),

            dbc.Row([
                dbc.Col([
                    section_heading("Tren Revenue per Jam — Semua Cabang", "📈"),
                    dcc.Graph(id="sales-multiline-trend", config={"displayModeBar": False}, style={"height": "520px"}),
                ]),
            ], className="g-3 mb-3"),

            dbc.Row([
                dbc.Col([
                    section_heading("Jumlah Transaksi per Cabang (per Jam)", "🧾"),
                    dcc.Graph(id="sales-trx-stacked", config={"displayModeBar": False}),
                ], md=7),
                dbc.Col([
                    section_heading("Market Share Revenue", "🥧"),
                    dcc.Graph(id="sales-share-pie", config={"displayModeBar": False}),
                ], md=5),
            ], className="g-3 mb-3"),

            dbc.Row([
                dbc.Col([
                    section_heading("Top 10 Menu Terlaris (semua cabang)", "🍽️"),
                    dcc.Graph(id="sales-top-menu", config={"displayModeBar": False}),
                ]),
            ], className="g-3 mb-3"),
        ]),
    ])

def _load_cabang():
    df = query_df("SELECT id_cabang, nama_cabang FROM default.dim_cabang ORDER BY nama_cabang")
    if df.empty: return []
    return [{"label": r.nama_cabang, "value": r.id_cabang} for _, r in df.iterrows()]

def _where(start, end, cabangs, payments):
    cl = [f"tanggal_transaksi BETWEEN '{start}' AND '{end}'"]
    if cabangs:
        cl.append(f"id_cabang IN ({','.join(str(c) for c in cabangs)})")
    if payments:
        cl.append("kategori_payment IN ({})".format(",".join(f"'{p}'" for p in payments)))
    return "WHERE " + " AND ".join(cl)

def _prev_period(start, end):
    from datetime import date, timedelta
    s = date.fromisoformat(start[:10])
    e = date.fromisoformat(end[:10])
    delta = (e - s) + timedelta(days=1)
    ps = s - delta
    pe = s - timedelta(days=1)
    return ps.isoformat(), pe.isoformat()

@callback(
    Output("sales-kpi-row",         "children"),
    Output("sales-cabang-total",   "figure"),
    Output("sales-cabang-delta",   "figure"),
    Output("sales-multiline-trend","figure"),
    Output("sales-trx-stacked",    "figure"),
    Output("sales-share-pie",      "figure"),
    Output("sales-top-menu",       "figure"),
    Input("sales-refresh",         "n_clicks"),
    Input("sales-auto-interval",   "n_intervals"),
    State("sales-daterange",       "start_date"),
    State("sales-daterange",       "end_date"),
    State("sales-cabang",          "value"),
    State("sales-payment",         "value"),
    prevent_initial_call=False,
)
def update_sales(n_clicks, n_intervals, start, end, cabangs, payments):
    ef = empty_fig()
    if not start or not end:
        return [], ef, ef, ef, ef, ef, ef

    where = _where(start, end, cabangs, payments)
    prev_start, prev_end = _prev_period(start, end)

    # ── KPIs (Dari Datamart)
    kpi_sql = f"""
        SELECT uniqMerge(trx_uniq) AS total_trx,
               sum(revenue)        AS total_rev,
               count(DISTINCT id_cabang) AS active_cab,
               total_rev / nullIf(total_trx, 0) AS avg_order
        FROM default.dm_sales_hourly {where}
    """
    kdf = query_df(kpi_sql)
    r = kdf.iloc[0] if not kdf.empty else None
    kpi_row = dbc.Row([
        _kpi("Total Transaksi",  f"{int(r.total_trx):,}"      if pd.notna(r.total_trx) else "—", "🧾", "#38bdf8"),
        _kpi("Total Revenue",    f"Rp {r.total_rev/1e6:.1f}M" if pd.notna(r.total_rev) else "—", "💰", "#4ade80"),
        _kpi("Cabang Aktif",     str(int(r.active_cab))       if pd.notna(r.active_cab) else "—", "🏪", BRAND_ACCENT),
        _kpi("Avg Order Value",  f"Rp {r.avg_order:,.0f}"     if pd.notna(r.avg_order) else "—", "📊", "#facc15"),
    ], className="g-3")

    # ── Revenue per cabang
    cab_sql = f"""
        SELECT c.nama_cabang AS cabang, sum(s.revenue) AS revenue,
               uniqMerge(s.trx_uniq) AS trx
        FROM default.dm_sales_hourly s
        LEFT JOIN default.dim_cabang c ON s.id_cabang = c.id_cabang
        {where} GROUP BY cabang ORDER BY revenue DESC
    """
    cdf = query_df(cab_sql)

    if cdf.empty:
        fig_total = fig_share = ef
    else:
        fig_total = go.Figure(go.Bar(
            x=cdf["revenue"], y=cdf["cabang"], orientation="h",
            marker=dict(color=PALETTE * 10, colorscale=None),
            text=cdf["revenue"].apply(lambda v: f"Rp {v/1e6:.1f}M"),
            textposition="inside", insidetextanchor="middle",
            customdata=cdf["trx"],
            hovertemplate="<b>%{y}</b><br>Revenue: Rp %{x:,.0f}<br>Transaksi: %{customdata:,}<extra></extra>",
        ))
        fig_total.update_layout(**chart_layout(yaxis=dict(autorange="reversed", categoryorder="total ascending")))

        fig_share = go.Figure(go.Pie(
            labels=cdf["cabang"], values=cdf["revenue"], hole=0.42,
            marker=dict(colors=PALETTE * 5), textinfo="label+percent", textfont=dict(color=BRAND_TEXT, size=11),
        ))
        fig_share.update_layout(**chart_layout(showlegend=False))

    # ── Delta vs previous period
    extra_cond = ""
    if cabangs: extra_cond += f" AND id_cabang IN ({','.join(str(c) for c in cabangs)})"
    if payments: extra_cond += " AND kategori_payment IN ({})".format(",".join(f"'{p}'" for p in payments))

    curr_hourly_sql = f"SELECT jam, sum(revenue) AS revenue FROM default.dm_sales_hourly WHERE tanggal_transaksi BETWEEN '{start}' AND '{end}' {extra_cond} GROUP BY jam"
    prev_hourly_sql = f"SELECT jam, sum(revenue) AS revenue_prev FROM default.dm_sales_hourly WHERE tanggal_transaksi BETWEEN '{prev_start}' AND '{prev_end}' {extra_cond} GROUP BY jam"
    
    ch_df = query_df(curr_hourly_sql)
    ph_df = query_df(prev_hourly_sql)

    if ch_df.empty and ph_df.empty:
        fig_delta = ef
    else:
        merged_hourly = ch_df.merge(ph_df, on="jam", how="outer").fillna(0)
        merged_hourly["delta"] = merged_hourly["revenue"] - merged_hourly["revenue_prev"]
        merged_hourly["delta_pct"] = merged_hourly.apply(lambda r: (r["delta"] / r["revenue_prev"] * 100) if r["revenue_prev"] > 0 else 0, axis=1)
        merged_hourly = merged_hourly.sort_values("jam")

        colors = ["#4ade80" if v >= 0 else "#fb7185" for v in merged_hourly["delta_pct"]]
        fig_delta = go.Figure(go.Bar(
            x=merged_hourly["jam"], y=merged_hourly["delta_pct"], marker_color=colors,
            text=merged_hourly["delta_pct"].apply(lambda v: f"{v:+.1f}%"), textposition="outside",
            customdata=merged_hourly["delta"],
            hovertemplate="<b>Jam %{x}</b><br>Pertumbuhan: %{y:+.1f}%<br>Selisih Rp: %{customdata:+,.0f}<extra></extra>",
        ))
        fig_delta.add_hline(y=0, line_color=MUTED, line_dash="dash")
        fig_delta.update_layout(**chart_layout(yaxis=dict(title="Perubahan (%)"), xaxis=dict(type="category", title="Jam Operasional")))

    # ── Multi-line trend
    trend_sql = f"""
        SELECT c.nama_cabang AS cabang, s.jam AS jam, sum(s.revenue) AS revenue
        FROM default.dm_sales_hourly s
        LEFT JOIN default.dim_cabang c ON s.id_cabang = c.id_cabang
        {where} GROUP BY cabang, jam ORDER BY cabang ASC, jam ASC
    """
    tdf = query_df(trend_sql)
    if tdf.empty:
        fig_trend = ef
    else:
        all_cabang = tdf["cabang"].unique()
        all_jam = tdf["jam"].unique()
        mux = pd.MultiIndex.from_product([all_cabang, all_jam], names=["cabang", "jam"])
        tdf = tdf.set_index(["cabang", "jam"]).reindex(mux, fill_value=0).reset_index()
        tdf = tdf.sort_values(["cabang", "jam"]).reset_index(drop=True)

        tdf["prev_revenue"] = tdf.groupby("cabang")["revenue"].shift(1)
        tdf["delta"] = tdf["revenue"] - tdf["prev_revenue"]
        
        def make_delta_tag(row):
            if pd.isna(row["prev_revenue"]): return " <span style='color:#94a3b8'>(Awal)</span>"
            d, prev = row["delta"], row["prev_revenue"]
            if prev == 0:
                if row["revenue"] > 0:
                    val_txt = f"+Rp {d/1e3:.0f}k" if d < 1e6 else f"+Rp {d/1e6:.1f}M"
                    return f" <span style='color:#4ade80'>(▲ {val_txt} / +100.0%)</span>"
                else:
                    return " <span style='color:#94a3b8'>(~ 0 / 0.0%)</span>"
            pct = (d / prev) * 100
            if d > 0: return f" <span style='color:#4ade80'>(▲ +Rp {d/1e3:.0f}k / +{pct:.1f}%)</span>" if d < 1e6 else f" <span style='color:#4ade80'>(▲ +Rp {d/1e6:.1f}M / +{pct:.1f}%)</span>"
            elif d < 0: return f" <span style='color:#fb7185'>(▼ -Rp {abs(d)/1e3:.0f}k / {pct:.1f}%)</span>" if abs(d) < 1e6 else f" <span style='color:#fb7185'>(▼ -Rp {abs(d)/1e6:.1f}M / {pct:.1f}%)</span>"
            else: return " <span style='color:#94a3b8'>(~ 0 / 0.0%)</span>"
                
        tdf["delta_txt"] = tdf.apply(make_delta_tag, axis=1)

        fig_trend = go.Figure()
        for i, cab in enumerate(tdf["cabang"].unique()):
            sub = tdf[tdf["cabang"] == cab]
            fig_trend.add_trace(go.Scatter(
                x=sub["jam"], y=sub["revenue"], name=cab, mode="lines+markers",
                line=dict(color=PALETTE[i % len(PALETTE)], width=2), customdata=sub["delta_txt"],
                hovertemplate=f"{cab}: Rp %{{y:,.0f}}%{{customdata}}<extra></extra>",
            ))
        fig_trend.update_layout(**chart_layout(hovermode="x unified", legend=dict(orientation="h", y=-0.15), xaxis=dict(type="category"), height=520))

    # ── Stacked transaksi per JAM
    trx_sql = f"""
        SELECT c.nama_cabang AS cabang, s.jam AS jam, uniqMerge(s.trx_uniq) AS trx
        FROM default.dm_sales_hourly s
        LEFT JOIN default.dim_cabang c ON s.id_cabang = c.id_cabang
        {where} GROUP BY cabang, jam ORDER BY jam ASC
    """
    xdf = query_df(trx_sql)
    if xdf.empty: fig_trx = ef
    else:
        fig_trx = go.Figure()
        for i, cab in enumerate(xdf["cabang"].unique()):
            sub = xdf[xdf["cabang"] == cab]
            fig_trx.add_trace(go.Bar(x=sub["jam"], y=sub["trx"], name=cab, marker_color=PALETTE[i % len(PALETTE)]))
        fig_trx.update_layout(**chart_layout(barmode="stack", legend=dict(orientation="h", y=1.05), xaxis=dict(type="category")))

    # ── Top menu
    menu_sql = f"""
        SELECT m.nama_menu AS menu, c.nama_cabang AS cabang, sum(s.revenue) AS revenue
        FROM default.dm_sales_hourly s
        LEFT JOIN default.dim_menu m ON s.id_menu = m.id_menu
        LEFT JOIN default.dim_cabang c ON s.id_cabang = c.id_cabang
        {where} GROUP BY menu, cabang
    """
    mdf = query_df(menu_sql)
    if mdf.empty: fig_menu = ef
    else:
        top10 = mdf.groupby("menu")["revenue"].sum().nlargest(10).index.tolist()
        mdf10 = mdf[mdf["menu"].isin(top10)]
        fig_menu = go.Figure()
        for i, cab in enumerate(mdf10["cabang"].unique()):
            sub = mdf10[mdf10["cabang"] == cab].set_index("menu").reindex(top10).fillna(0).reset_index()
            fig_menu.add_trace(go.Bar(name=cab, x=sub["menu"], y=sub["revenue"], marker_color=PALETTE[i % len(PALETTE)]))
        fig_menu.update_layout(**chart_layout(barmode="group", legend=dict(orientation="h", y=1.05), xaxis=dict(tickangle=-30)))

    return kpi_row, fig_total, fig_delta, fig_trend, fig_trx, fig_share, fig_menu

def _kpi(title, value, icon, color):
    return dbc.Col(dbc.Card(
        style={"borderTop": f"3px solid {color}", "background": BRAND_MID}, className="kpi-card",
        children=dbc.CardBody([
            html.Div([html.Span(icon, className="kpi-icon"), html.P(title, className="kpi-title")], className="d-flex align-items-center gap-2 mb-1"),
            html.H4(value, className="kpi-value"),
        ]),
    ), md=3, sm=6, xs=12)