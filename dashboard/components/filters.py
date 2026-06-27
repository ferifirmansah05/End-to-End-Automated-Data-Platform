"""
components/filters.py
Reusable filter bar, KPI card, section header, and live clock.
"""
import dash_bootstrap_components as dbc
from dash import html, dcc
import datetime


# ── Colour tokens ────────────────────────────────────────────────
BRAND_DARK   = "#0f172a"   # sidebar / top-bar background
BRAND_MID    = "#1e293b"   # card background
BRAND_ACCENT = "#f97316"   # orange accent (matches existing React UI)
BRAND_TEXT   = "#e2e8f0"
MUTED        = "#94a3b8"


# ── Top header bar ───────────────────────────────────────────────
def top_bar(page_title: str) -> html.Div:
    return html.Div(
        className="top-bar d-flex align-items-center justify-content-between px-4",
        children=[
            html.Div([
                html.Span("📊 ", style={"fontSize": "1.1rem"}),
                html.Span(page_title, className="top-bar-title"),
            ]),
            html.Div([
                html.Span(id="live-clock", className="live-clock"),
                html.Span(" · Realtime", style={"color": MUTED, "fontSize": "0.75rem"}),
            ], className="d-flex align-items-center gap-2"),
        ],
    )


# ── Live clock interval (put once in layout) ─────────────────────
live_clock_interval = dcc.Interval(id="clock-interval", interval=1000, n_intervals=0)


# ── Date-range filter bar ────────────────────────────────────────
def date_filter_bar(
    id_prefix: str,
    cabang_options: list = None,
    extra_filters: list = None,
) -> dbc.Card:
    """
    Standard filter bar: date-range picker + optional branch selector + extras.
    id_prefix ensures each page has unique component IDs.
    """
    today = datetime.date.today()
    default_start = (today - datetime.timedelta(days=29)).isoformat()
    default_end   = today.isoformat()

    branch_select = []
    if cabang_options is not None:
        branch_select = [
            dbc.Col([
                html.Label("Cabang", className="filter-label"),
                dcc.Dropdown(
                    id=f"{id_prefix}-cabang",
                    options=cabang_options,
                    multi=True,
                    placeholder="Semua cabang",
                    className="dash-dropdown-dark",
                ),
            ], md=3),
        ]

    return dbc.Card(
        className="filter-card mb-3",
        children=[
            dbc.CardBody([
                dbc.Row(
                    [
                        dbc.Col([
                            html.Label("Rentang Tanggal", className="filter-label"),
                            dcc.DatePickerRange(
                                id=f"{id_prefix}-daterange",
                                start_date=default_start,
                                end_date=default_end,
                                display_format="DD MMM YYYY",
                                className="date-picker-dark",
                            ),
                        ], md=4),
                        *branch_select,
                        *(extra_filters or []),
                        dbc.Col([
                            html.Label("\u00a0", className="filter-label d-block"),
                            dbc.Button(
                                "⟳  Refresh",
                                id=f"{id_prefix}-refresh",
                                color="warning",
                                size="sm",
                                className="refresh-btn",
                            ),
                        ], md=2, className="d-flex align-items-end"),
                    ],
                    align="end",
                ),
            ]),
        ],
    )


# ── Auto-refresh interval (shared per page) ──────────────────────
def auto_refresh(id_prefix: str, interval_ms: int = 60000) -> dcc.Interval:
    return dcc.Interval(
        id=f"{id_prefix}-auto-interval",
        interval=interval_ms,
        n_intervals=0,
    )


# ── KPI card ─────────────────────────────────────────────────────
def kpi_card(
    title: str,
    value_id: str,
    icon: str = "📌",
    color: str = BRAND_ACCENT,
    subtitle_id: str = None,
) -> dbc.Col:
    subtitle = []
    if subtitle_id:
        subtitle = [html.P(id=subtitle_id, className="kpi-subtitle")]

    return dbc.Col(
        dbc.Card(
            className="kpi-card",
            style={"borderTop": f"3px solid {color}"},
            children=[
                dbc.CardBody([
                    html.Div([
                        html.Span(icon, className="kpi-icon"),
                        html.P(title, className="kpi-title"),
                    ], className="d-flex align-items-center gap-2 mb-1"),
                    html.H4(id=value_id, className="kpi-value"),
                    *subtitle,
                ]),
            ],
        ),
        md=3, sm=6, xs=12,
    )


# ── Section heading ───────────────────────────────────────────────
def section_heading(text: str, icon: str = "") -> html.Div:
    return html.Div(
        className="section-heading",
        children=[
            html.Span(icon + " " if icon else "", style={"marginRight": "4px"}),
            html.Span(text),
        ],
    )


# ── Empty-state placeholder ───────────────────────────────────────
def empty_fig(message: str = "Tidak ada data untuk periode ini.") -> dict:
    """Return a blank plotly figure with a centred annotation."""
    return {
        "data": [],
        "layout": {
            "paper_bgcolor": BRAND_MID,
            "plot_bgcolor":  BRAND_MID,
            "font":          {"color": MUTED},
            "xaxis":         {"visible": False},
            "yaxis":         {"visible": False},
            "annotations":   [
                {
                    "text":      message,
                    "xref":      "paper",
                    "yref":      "paper",
                    "showarrow": False,
                    "font":      {"size": 14, "color": MUTED},
                    "x": 0.5, "y": 0.5,
                }
            ],
        },
    }


# ── Shared plotly layout defaults ─────────────────────────────────
_AXIS_DEFAULTS = dict(gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.1)")

CHART_LAYOUT = dict(
    paper_bgcolor=BRAND_MID,
    plot_bgcolor=BRAND_MID,
    font=dict(color=BRAND_TEXT, family="Inter, sans-serif", size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor="rgba(255,255,255,0.1)",
        borderwidth=1,
    ),
    xaxis=_AXIS_DEFAULTS.copy(),
    yaxis=_AXIS_DEFAULTS.copy(),
)


def chart_layout(**overrides) -> dict:
    """Merge CHART_LAYOUT with caller overrides.
    xaxis/yaxis values are deep-merged so axis grid defaults are preserved.
    Use this instead of update_layout(**CHART_LAYOUT, yaxis=...) to avoid
    'multiple values for keyword argument' errors.
    """
    base = {k: v.copy() if isinstance(v, dict) else v
            for k, v in CHART_LAYOUT.items()}
    for key, val in overrides.items():
        if key in ("xaxis", "yaxis", "xaxis2", "yaxis2") and isinstance(val, dict):
            merged = _AXIS_DEFAULTS.copy()
            merged.update(val)
            base[key] = merged
        else:
            base[key] = val
    return base


PALETTE = ["#f97316", "#38bdf8", "#4ade80", "#facc15", "#c084fc", "#fb7185"]
