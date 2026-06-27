"""
app.py  –  ERP Analytics Dashboard
Multi-page Dash app with dark theme, sidebar navigation, and ClickHouse backend.
"""
import datetime
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, callback

from components.filters import live_clock_interval, BRAND_DARK, BRAND_MID, BRAND_ACCENT, MUTED

# ── Import pages ───────────────────────────────────────────────────
from pages import sales, purchase, outlet, inventory
from config import REFRESH_INTERVAL_MS
import pendulum

lokal_tz = pendulum.timezone("Asia/Jakarta")

# ── App init ───────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
    suppress_callback_exceptions=True,
    title="ERP Analytics",
    update_title=None,
)
server = app.server   # expose Flask server for gunicorn

# ── Sidebar nav items ───────────────────────────────────────────────
NAV_ITEMS = [
    {"href": "/sales",     "label": "Sales",         "icon": "💰"},
    {"href": "/purchase",  "label": "Purchase",      "icon": "🛒"},
    {"href": "/outlet",    "label": "Outlet/Deviasi","icon": "⚡"},
    {"href": "/inventory", "label": "Inventory",     "icon": "📦"},
]


def sidebar() -> html.Div:
    return html.Div(className="sidebar", children=[
        # Logo
        html.Div(className="sidebar-logo", children=[
            html.H5("ERP Analytics"),
            html.Small("Dashboard Realtime"),
        ]),

        # Nav
        html.Div(className="sidebar-nav", children=[
            html.Div("Analisa", className="sidebar-section-label"),
            dbc.Nav(
                [
                    dbc.NavLink(
                        [
                            html.Span(item["icon"], style={"minWidth": "20px"}),
                            html.Span(item["label"], className="nav-label"),
                        ],
                        href=item["href"],
                        active="exact",
                        className="nav-link",
                    )
                    for item in NAV_ITEMS
                ],
                vertical=True,
                pills=True,
            ),
        ]),

        # Footer
        html.Div(className="sidebar-footer", children=[
            html.Div(f"🔴 Live  ·  Auto-refresh: {REFRESH_INTERVAL_MS/1000}s"),
        ]),
    ])


# ── App layout ──────────────────────────────────────────────────────
app.layout = html.Div(
    className="app-shell",
    children=[
        dcc.Location(id="url", refresh=False),
        live_clock_interval,

        # Sidebar
        sidebar(),

        # Main area
        html.Div(
            className="main-content",
            children=[html.Div(id="page-content")],
        ),
    ],
)


# ── Router ───────────────────────────────────────────────────────────
@callback(Output("page-content", "children"), Input("url", "pathname"))
def render_page(pathname: str):
    if pathname in ("/", "/sales"):
        return sales.layout()
    elif pathname == "/purchase":
        return purchase.layout()
    elif pathname == "/outlet":
        return outlet.layout()
    elif pathname == "/inventory":
        return inventory.layout()
    else:
        return html.Div(
            className="d-flex align-items-center justify-content-center",
            style={"height": "80vh"},
            children=[
                html.Div([
                    html.H2("404", style={"color": BRAND_ACCENT, "fontSize": "4rem"}),
                    html.P("Halaman tidak ditemukan.", style={"color": MUTED}),
                    dbc.Button("Kembali ke Sales", href="/sales", color="warning"),
                ], className="text-center"),
            ],
        )


# ── Live clock ────────────────────────────────────────────────────────
@callback(Output("live-clock", "children"), Input("clock-interval", "n_intervals"))
def update_clock(n):
    now = datetime.datetime.now(lokal_tz)
    return now.strftime("%A, %d %b %Y  %H:%M:%S")


# ── Run ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
