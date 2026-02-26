# ╔══════════════════════════════════════════════════════════════════════════╗
# ║         SaaS Executive Dashboard — Streamlit in Snowflake (Native)      ║
# ║  Deploy with: CREATE STREAMLIT in Snowflake Snowsight                   ║
# ║  Database: DBT_SAAS_DEMO  |  Schema: DBT_PROD_SEMANTIC                  ║
# ╚══════════════════════════════════════════════════════════════════════════╝

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session   # ← Snowflake native

# ─────────────────────────────────────────────────────────────────────────────
#  PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SaaS Executive Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
#  SNOWFLAKE NATIVE SESSION  (no credentials needed inside Snowflake)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_session():
    return get_active_session()

session = get_session()

# ─────────────────────────────────────────────────────────────────────────────
#  ENVIRONMENT INFO  (role / warehouse badge)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def get_env_info():
    row = session.sql("""
        SELECT
            CURRENT_ROLE()      AS role,
            CURRENT_WAREHOUSE() AS warehouse,
            CURRENT_DATABASE()  AS database,
            CURRENT_SCHEMA()    AS schema_name,
            CURRENT_USER()      AS user_name
    """).collect()[0]
    return {
        "role":      row["ROLE"],
        "warehouse": row["WAREHOUSE"],
        "database":  row["DATABASE"],
        "schema":    row["SCHEMA_NAME"],
        "user":      row["USER_NAME"],
    }

env = get_env_info()

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
DB     = "DBT_SAAS_DEMO"
SCHEMA = "DBT_DEV_SEMANTIC"
CUTOFF = "(CURRENT_DATE - INTERVAL '3 YEAR')"   # 3-year rolling window

COLORS = {
    "blue":            "#2563EB",
    "sky":             "#0EA5E9",
    "green":           "#10B981",
    "purple":          "#7C3AED",
    "orange":          "#F59E0B",
    "red":             "#EF4444",
    "plan_palette":    ["#2563EB", "#7C3AED", "#10B981", "#F59E0B", "#EF4444", "#0EA5E9"],
    "country_palette": ["#2563EB", "#7C3AED", "#10B981", "#F59E0B", "#EF4444",
                        "#0EA5E9", "#EC4899", "#14B8A6", "#F97316"],
}

PLOTLY_LAYOUT = dict(
    font_family="Sora",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=10, b=0),
    legend=dict(
        orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0,
        font_size=11, bgcolor="rgba(0,0,0,0)",
    ),
    xaxis=dict(
        showgrid=False, tickfont_size=10, tickfont_color="#8A95AE",
        linecolor="#E2E7F0", tickcolor="#E2E7F0",
    ),
    yaxis=dict(
        gridcolor="#E2E7F0", gridwidth=0.8, tickfont_size=10,
        tickfont_color="#8A95AE", showline=False, zeroline=False,
    ),
)

# ─────────────────────────────────────────────────────────────────────────────
#  GLOBAL CSS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  html, body, [class*="css"] { font-family: 'Sora', sans-serif !important; }
  .stApp { background: #F4F6FB; }
  #MainMenu, footer, header { visibility: hidden; }
  .block-container { padding: 1.5rem 2rem 2rem 2rem !important; max-width: 100% !important; }

  /* ── Sidebar ── */
  [data-testid="stSidebar"] { background: #0F1729 !important; border-right: 1px solid #1e2d45; }
  [data-testid="stSidebar"] * { color: rgba(255,255,255,0.75) !important; }
  [data-testid="stSidebar"] .stSelectbox label,
  [data-testid="stSidebar"] .stDateInput label {
    color: rgba(255,255,255,0.4) !important;
    font-size: 9.5px !important;
    letter-spacing: 1.5px;
    text-transform: uppercase;
  }
  [data-testid="stSidebar"] [data-baseweb="select"] {
    background: rgba(255,255,255,0.06) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 8px !important;
  }

  /* ── Environment badge (sidebar) ── */
  .env-badge {
    background: rgba(37,99,235,0.15);
    border: 1px solid rgba(37,99,235,0.25);
    border-radius: 8px;
    padding: 10px 12px;
    margin-bottom: 14px;
  }
  .env-row { display:flex; justify-content:space-between; align-items:center; margin-bottom:5px; }
  .env-row:last-child { margin-bottom:0; }
  .env-key   { font-size:8.5px; font-weight:600; color:rgba(255,255,255,0.35) !important; letter-spacing:1.3px; text-transform:uppercase; }
  .env-val   { font-size:10.5px; font-weight:600; color:rgba(255,255,255,0.85) !important; font-family:'JetBrains Mono',monospace; }
  .env-dot   { width:7px; height:7px; border-radius:50%; background:#10B981; display:inline-block; margin-right:5px; animation:pulse 2s infinite; }

  /* ── KPI Cards ── */
  .kpi-card {
    background:#fff; border:1px solid #E2E7F0; border-radius:14px;
    padding:16px 18px; position:relative; overflow:hidden; min-height:118px;
  }
  .kpi-card::before {
    content:''; position:absolute; top:0; left:0; right:0; height:3px; border-radius:14px 14px 0 0;
  }
  .kpi-card.blue::before   { background:linear-gradient(90deg,#2563EB,#0EA5E9); }
  .kpi-card.green::before  { background:linear-gradient(90deg,#10B981,#34D399); }
  .kpi-card.purple::before { background:linear-gradient(90deg,#7C3AED,#A78BFA); }
  .kpi-card.orange::before { background:linear-gradient(90deg,#F59E0B,#FCD34D); }
  .kpi-card.teal::before   { background:linear-gradient(90deg,#0EA5E9,#38BDF8); }

  .kpi-label  { font-size:9.5px; font-weight:600; color:#8A95AE; letter-spacing:0.7px; text-transform:uppercase; margin-bottom:8px; }
  .kpi-value  { font-family:'JetBrains Mono',monospace; font-size:24px; font-weight:700; letter-spacing:-0.8px; color:#0F1729; line-height:1; }
  .kpi-footer { display:flex; align-items:center; gap:7px; margin-top:10px; flex-wrap:wrap; }
  .kpi-badge-up   { background:rgba(16,185,129,0.1);  color:#10B981; font-size:10px; font-weight:600; padding:3px 7px; border-radius:6px; }
  .kpi-badge-down { background:rgba(239,68,68,0.1);   color:#EF4444; font-size:10px; font-weight:600; padding:3px 7px; border-radius:6px; }
  .kpi-badge-neu  { background:rgba(107,114,128,0.1); color:#6B7280; font-size:10px; font-weight:600; padding:3px 7px; border-radius:6px; }
  .kpi-compare { font-size:9.5px; color:#8A95AE; }

  /* ── Chart cards ── */
  .chart-card { background:#fff; border:1px solid #E2E7F0; border-radius:14px; padding:18px 20px; }
  .section-title { font-size:13px; font-weight:700; color:#0F1729; letter-spacing:-0.3px; margin:0; }
  .section-sub   { font-size:10.5px; color:#8A95AE; margin-top:2px; margin-bottom:12px; }
  .card-head     { display:flex; align-items:flex-start; justify-content:space-between; margin-bottom:10px; }
  .tag           { font-size:9.5px; font-weight:600; padding:3px 9px; border-radius:6px; background:rgba(37,99,235,0.08); color:#2563EB; }
  .tag.green  { background:rgba(16,185,129,0.08); color:#10B981; }
  .tag.purple { background:rgba(124,58,237,0.08); color:#7C3AED; }
  .tag.orange { background:rgba(245,158,11,0.08); color:#F59E0B; }

  /* ── Page header ── */
  .page-header {
    background:#fff; border:1px solid #E2E7F0; border-radius:14px;
    padding:16px 22px; display:flex; align-items:center; justify-content:space-between; margin-bottom:18px;
  }
  .page-title { font-size:19px; font-weight:700; letter-spacing:-0.5px; color:#0F1729; }
  .page-sub   { font-size:11.5px; color:#8A95AE; margin-top:3px; }

  .live-badge {
    background:rgba(16,185,129,0.1); color:#10B981; font-size:10px; font-weight:600;
    padding:5px 12px; border-radius:20px; border:1px solid rgba(16,185,129,0.2);
    display:inline-flex; align-items:center; gap:6px;
  }
  .live-dot { width:6px; height:6px; border-radius:50%; background:#10B981; display:inline-block; animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  /* ── Section label ── */
  .section-label { font-size:9.5px; font-weight:600; color:#8A95AE; letter-spacing:0.8px; text-transform:uppercase; margin-bottom:10px; }

  hr { border:none; border-top:1px solid #E2E7F0; margin:1.2rem 0; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  DATA LOADERS  (fully qualified names + 3-year filter pushed to Snowflake)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_kpis(start_date: str, end_date: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            month,
            total_mrr_usd,
            revenue_usd,
            net_revenue_usd,
            active_paying_users,
            arpu
        FROM {DB}.{SCHEMA}.SEMANTIC_EXECUTIVE_KPIS
        WHERE month >= {CUTOFF}
          AND month BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY month ASC
    """
    return session.sql(sql).to_pandas()

@st.cache_data(ttl=300, show_spinner=False)
def load_revenue(start_date: str, end_date: str, country: str, plan: str) -> pd.DataFrame:
    country_filter = f"AND country = '{country}'" if country != "All" else ""
    plan_filter    = f"AND plan_name = '{plan}'"  if plan    != "All" else ""
    sql = f"""
        SELECT
            month,
            country,
            plan_name,
            net_revenue,
            gross_revenue
        FROM {DB}.{SCHEMA}.SEMANTIC_REVENUE_ANALYSIS
        WHERE month >= {CUTOFF}
          AND month BETWEEN '{start_date}' AND '{end_date}'
          {country_filter}
          {plan_filter}
        ORDER BY month ASC
    """
    return session.sql(sql).to_pandas()

@st.cache_data(ttl=300, show_spinner=False)
def load_cohorts() -> pd.DataFrame:
    sql = f"""
        SELECT
            cohort_month,
            months_since_signup,
            active_users
        FROM {DB}.{SCHEMA}.SEMANTIC_COHORTS
        WHERE cohort_month >= {CUTOFF}
        ORDER BY cohort_month ASC, months_since_signup ASC
    """
    return session.sql(sql).to_pandas()

@st.cache_data(ttl=600, show_spinner=False)
def load_filter_options() -> dict:
    """Fetch distinct filter values directly from Snowflake."""
    countries = session.sql(f"""
        SELECT DISTINCT country
        FROM {DB}.{SCHEMA}.SEMANTIC_REVENUE_ANALYSIS
        WHERE country IS NOT NULL
          AND month >= {CUTOFF}
        ORDER BY country
    """).to_pandas()["COUNTRY"].tolist()

    plans = session.sql(f"""
        SELECT DISTINCT plan_name
        FROM {DB}.{SCHEMA}.SEMANTIC_REVENUE_ANALYSIS
        WHERE plan_name IS NOT NULL
          AND month >= {CUTOFF}
        ORDER BY plan_name
    """).to_pandas()["PLAN_NAME"].tolist()

    date_bounds = session.sql(f"""
        SELECT
            MIN(month) AS min_month,
            MAX(month) AS max_month
        FROM {DB}.{SCHEMA}.SEMANTIC_EXECUTIVE_KPIS
        WHERE month >= {CUTOFF}
    """).collect()[0]

    return {
        "countries":  countries,
        "plans":      plans,
        "min_date":   date_bounds["MIN_MONTH"],
        "max_date":   date_bounds["MAX_MONTH"],
    }


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fmt_usd(val: float) -> str:
    if pd.isna(val): return "—"
    if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
    if val >= 1_000:     return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"

def mom_pct(series: pd.Series) -> float:
    if len(series) < 2: return 0.0
    prev, curr = series.iloc[-2], series.iloc[-1]
    return (curr - prev) / prev * 100 if prev else 0.0

def kpi_card(label: str, value: str, growth: float, compare: str, color: str) -> str:
    if growth > 0.05:
        badge = f'<span class="kpi-badge-up">▲ {growth:+.1f}%</span>'
    elif growth < -0.05:
        badge = f'<span class="kpi-badge-down">▼ {growth:.1f}%</span>'
    else:
        badge = f'<span class="kpi-badge-neu">— 0.0%</span>'
    return f"""
    <div class="kpi-card {color}">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      <div class="kpi-footer">{badge}<span class="kpi-compare">{compare}</span></div>
    </div>"""

def normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Snowflake returns UPPERCASE columns — normalise to lowercase."""
    df.columns = [c.lower() for c in df.columns]
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:

    # Logo
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:20px;padding-bottom:18px;border-bottom:1px solid rgba(255,255,255,0.08);">
      <div style="width:34px;height:34px;background:linear-gradient(135deg,#2563EB,#0EA5E9);border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:17px;">📈</div>
      <div>
        <div style="font-size:15px;font-weight:700;color:#fff!important;letter-spacing:-0.3px;">Analytix</div>
        <div style="font-size:8.5px;color:rgba(255,255,255,0.32)!important;letter-spacing:1.5px;text-transform:uppercase;">SaaS Intelligence</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Environment badge ──
    st.markdown(f"""
    <div class="env-badge">
      <div class="env-row">
        <span class="env-key"><span class="env-dot"></span>Status</span>
        <span class="env-val" style="color:#10B981!important;">Connected</span>
      </div>
      <div class="env-row">
        <span class="env-key">Role</span>
        <span class="env-val">{env['role']}</span>
      </div>
      <div class="env-row">
        <span class="env-key">Warehouse</span>
        <span class="env-val">{env['warehouse']}</span>
      </div>
      <div class="env-row">
        <span class="env-key">User</span>
        <span class="env-val">{env['user']}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Navigation
    st.markdown('<div style="font-size:8.5px;font-weight:600;color:rgba(255,255,255,0.28)!important;letter-spacing:1.8px;text-transform:uppercase;margin-bottom:6px;">NAVIGATION</div>', unsafe_allow_html=True)
    page = st.radio("", ["📊 Executive Summary", "💰 Revenue Analytics", "🔄 Growth & Cohorts"],
                    label_visibility="collapsed")

    st.markdown("<hr style='border-color:rgba(255,255,255,0.07);margin:14px 0;'>", unsafe_allow_html=True)
    st.markdown('<div style="font-size:8.5px;font-weight:600;color:rgba(255,255,255,0.28)!important;letter-spacing:1.8px;text-transform:uppercase;margin-bottom:8px;">FILTERS</div>', unsafe_allow_html=True)

    # Load filter options
    with st.spinner("Loading filters…"):
        opts = load_filter_options()

    min_d = opts["min_date"] if opts["min_date"] else date.today() - timedelta(days=365)
    max_d = opts["max_date"] if opts["max_date"] else date.today()

    date_range = st.date_input(
        "Date Range",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d,
    )
    start_d = str(date_range[0]) if len(date_range) > 0 else str(min_d)
    end_d   = str(date_range[1]) if len(date_range) > 1 else str(max_d)

    sel_country = st.selectbox("Country", ["All"] + opts["countries"])
    sel_plan    = st.selectbox("Plan",    ["All"] + opts["plans"])

    # User card
    st.markdown("<hr style='border-color:rgba(255,255,255,0.07);margin:14px 0;'>", unsafe_allow_html=True)
    initials = "".join(w[0].upper() for w in env["user"].split("_")[:2]) or "EX"
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:9px;padding:10px;border-radius:8px;background:rgba(255,255,255,0.05);">
      <div style="width:32px;height:32px;border-radius:50%;background:linear-gradient(135deg,#2563EB,#7C3AED);display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;color:#fff;flex-shrink:0;">{initials}</div>
      <div>
        <div style="font-size:11.5px;font-weight:600;color:rgba(255,255,255,0.85)!important;">{env['user']}</div>
        <div style="font-size:9.5px;color:rgba(255,255,255,0.32)!important;">{env['role']}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
#  LOAD DATA  (filters pushed to Snowflake — no over-fetching)
# ─────────────────────────────────────────────────────────────────────────────
with st.spinner("Querying Snowflake…"):
    df_kpis    = normalise_cols(load_kpis(start_d, end_d))
    df_revenue = normalise_cols(load_revenue(start_d, end_d, sel_country, sel_plan))
    df_cohorts = normalise_cols(load_cohorts())

# Parse dates
df_kpis["month"]           = pd.to_datetime(df_kpis["month"])
df_revenue["month"]        = pd.to_datetime(df_revenue["month"])
df_cohorts["cohort_month"] = pd.to_datetime(df_cohorts["cohort_month"])


# ─────────────────────────────────────────────────────────────────────────────
#  PAGE META
# ─────────────────────────────────────────────────────────────────────────────
PAGE_META = {
    "📊 Executive Summary": ("Executive Summary",  f"All KPIs · {end_d}"),
    "💰 Revenue Analytics": ("Revenue Analytics",  "MRR trend · plans & countries"),
    "🔄 Growth & Cohorts":  ("Growth & Cohorts",   "MRR growth & cohort retention"),
}
p_title, p_sub = PAGE_META[page]

st.markdown(f"""
<div class="page-header">
  <div>
    <div class="page-title">{p_title}</div>
    <div class="page-sub">{p_sub} &nbsp;·&nbsp; {DB}.{SCHEMA}</div>
  </div>
  <div style="display:flex;gap:10px;align-items:center;">
    <div style="font-size:10px;color:#8A95AE;font-family:'JetBrains Mono',monospace;">
      WH: <strong style="color:#0F1729;">{env['warehouse']}</strong>
      &nbsp;|&nbsp; Role: <strong style="color:#0F1729;">{env['role']}</strong>
    </div>
    <div class="live-badge"><span class="live-dot"></span> Snowflake Native</div>
  </div>
</div>
""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE 1 — EXECUTIVE SUMMARY
# ═════════════════════════════════════════════════════════════════════════════
if page == "📊 Executive Summary":

    if df_kpis.empty:
        st.warning("No data found for the selected date range.")
        st.stop()

    last = df_kpis.iloc[-1]
    prev = df_kpis.iloc[-2] if len(df_kpis) >= 2 else last

    def g(col):
        p = prev[col]
        return float((last[col] - p) / p * 100) if p else 0.0

    # ── KPI Row ──
    st.markdown('<div class="section-label">Key Performance Indicators</div>', unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns(5)
    for col, label, val, growth, color in [
        (c1, "Monthly Recurring Revenue", fmt_usd(last["total_mrr_usd"]),          g("total_mrr_usd"),          "blue"),
        (c2, "Total Revenue",             fmt_usd(last["revenue_usd"]),             g("revenue_usd"),            "green"),
        (c3, "Net Revenue",               fmt_usd(last["net_revenue_usd"]),         g("net_revenue_usd"),        "teal"),
        (c4, "Active Paying Users",       f'{int(last["active_paying_users"]):,}',  g("active_paying_users"),    "purple"),
        (c5, "ARPU / Month",              fmt_usd(last["arpu"]),                    g("arpu"),                   "orange"),
    ]:
        col.markdown(kpi_card(label, val, growth, "vs prev month", color), unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── MRR + Net Revenue Trend ──
    col1, col2 = st.columns([3, 2])

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="card-head"><div><div class="section-title">MRR & Revenue Trend</div><div class="section-sub">Rolling period selected</div></div><span class="tag">Monthly</span></div>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_kpis["month"], y=df_kpis["total_mrr_usd"],
            name="MRR", mode="lines",
            line=dict(color=COLORS["blue"], width=2.5),
            fill="tozeroy", fillcolor="rgba(37,99,235,0.08)",
        ))
        fig.add_trace(go.Scatter(
            x=df_kpis["month"], y=df_kpis["net_revenue_usd"],
            name="Net Revenue", mode="lines",
            line=dict(color=COLORS["green"], width=2.5, dash="dot"),
            fill="tozeroy", fillcolor="rgba(16,185,129,0.05)",
        ))
        fig.update_layout(**PLOTLY_LAYOUT, height=230)
        fig.update_xaxes(tickformat="%b %Y")
        fig.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="card-head"><div><div class="section-title">ARPU Over Time</div><div class="section-sub">Avg revenue per user</div></div><span class="tag orange">ARPU</span></div>', unsafe_allow_html=True)
        fig2 = go.Figure(go.Scatter(
            x=df_kpis["month"], y=df_kpis["arpu"],
            mode="lines+markers",
            line=dict(color=COLORS["orange"], width=2.5),
            marker=dict(size=5, color=COLORS["orange"]),
            fill="tozeroy", fillcolor="rgba(245,158,11,0.08)",
        ))
        fig2.update_layout(**PLOTLY_LAYOUT, height=230, showlegend=False)
        fig2.update_xaxes(tickformat="%b %Y")
        fig2.update_yaxes(tickprefix="$", tickformat=",.2f")
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── Active Users bar ──
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="card-head"><div><div class="section-title">Active Paying Users Growth</div><div class="section-sub">Monthly count</div></div><span class="tag purple">Users</span></div>', unsafe_allow_html=True)
    fig3 = go.Figure(go.Bar(
        x=df_kpis["month"], y=df_kpis["active_paying_users"],
        marker_color=COLORS["purple"], marker_opacity=0.82,
        marker_line_width=0, name="Users",
    ))
    fig3.update_layout(**PLOTLY_LAYOUT, height=170, showlegend=False)
    fig3.update_xaxes(tickformat="%b %Y")
    fig3.update_yaxes(tickformat=",")
    st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE 2 — REVENUE ANALYTICS
# ═════════════════════════════════════════════════════════════════════════════
elif page == "💰 Revenue Analytics":

    if df_revenue.empty:
        st.warning("No revenue data found for the selected filters.")
        st.stop()

    # ── MRR Trend ──
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="card-head"><div><div class="section-title">MRR & Revenue Trend</div><div class="section-sub">Filtered period</div></div><span class="tag">Monthly</span></div>', unsafe_allow_html=True)
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_kpis["month"], y=df_kpis["total_mrr_usd"],
        name="MRR", mode="lines",
        line=dict(color=COLORS["blue"], width=3),
        fill="tozeroy", fillcolor="rgba(37,99,235,0.1)",
    ))
    fig.add_trace(go.Scatter(
        x=df_kpis["month"], y=df_kpis["net_revenue_usd"],
        name="Net Revenue", mode="lines",
        line=dict(color=COLORS["green"], width=2.5, dash="dot"),
        fill="tozeroy", fillcolor="rgba(16,185,129,0.06)",
    ))
    fig.update_layout(**PLOTLY_LAYOUT, height=210)
    fig.update_xaxes(tickformat="%b %Y")
    fig.update_yaxes(tickprefix="$", tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)
    col1, col2 = st.columns(2)

    # ── Revenue by Country ──
    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="card-head"><div><div class="section-title">Revenue by Country</div><div class="section-sub">Top 10 markets</div></div><span class="tag">Geographic</span></div>', unsafe_allow_html=True)
        rev_country = (
            df_revenue.groupby("country")["net_revenue"]
            .sum().reset_index()
            .sort_values("net_revenue", ascending=False)
            .head(10)
        )
        fig_c = go.Figure(go.Bar(
            x=rev_country["net_revenue"],
            y=rev_country["country"],
            orientation="h",
            marker=dict(color=COLORS["country_palette"][:len(rev_country)], line_width=0),
            text=[fmt_usd(v) for v in rev_country["net_revenue"]],
            textposition="outside",
            textfont=dict(size=10, color="#5A6580"),
        ))
        fig_c.update_layout(**PLOTLY_LAYOUT, height=300, showlegend=False)
        fig_c.update_xaxes(tickprefix="$", tickformat=",.0f")
        fig_c.update_yaxes(tickfont_size=11, tickfont_color="#0F1729")
        st.plotly_chart(fig_c, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # ── Revenue by Plan (stacked) ──
    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="card-head"><div><div class="section-title">Revenue by Plan</div><div class="section-sub">Stacked monthly revenue</div></div><span class="tag green">Stacked</span></div>', unsafe_allow_html=True)
        rev_plan_month = (
            df_revenue.groupby(["month", "plan_name"])["net_revenue"]
            .sum().reset_index()
        )
        plans_list = rev_plan_month["plan_name"].unique().tolist()
        fig_p = go.Figure()
        for i, plan in enumerate(plans_list):
            d = rev_plan_month[rev_plan_month["plan_name"] == plan]
            fig_p.add_trace(go.Bar(
                x=d["month"], y=d["net_revenue"], name=plan,
                marker_color=COLORS["plan_palette"][i % len(COLORS["plan_palette"])],
                marker_line_width=0,
            ))
        fig_p.update_layout(**PLOTLY_LAYOUT, barmode="stack", height=300)
        fig_p.update_xaxes(tickformat="%b %Y")
        fig_p.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_p, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── Plan totals summary ──
    rev_plan_total = (
        df_revenue.groupby("plan_name")["net_revenue"]
        .sum().reset_index()
        .sort_values("net_revenue", ascending=False)
    )
    total_rev = rev_plan_total["net_revenue"].sum()
    plan_cols = st.columns(max(len(rev_plan_total), 1))
    for col, (_, row) in zip(plan_cols, rev_plan_total.iterrows()):
        pct = row["net_revenue"] / total_rev * 100 if total_rev else 0
        col.markdown(f"""
        <div class="chart-card" style="text-align:center;padding:14px;">
          <div style="font-size:9.5px;color:#8A95AE;text-transform:uppercase;letter-spacing:0.5px;">{row['plan_name']}</div>
          <div style="font-size:20px;font-weight:700;font-family:'JetBrains Mono',monospace;color:#0F1729;margin:6px 0;">{fmt_usd(row['net_revenue'])}</div>
          <div style="font-size:11px;font-weight:600;color:#2563EB;">{pct:.1f}%</div>
        </div>
        """, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
#  PAGE 3 — GROWTH & COHORTS
# ═════════════════════════════════════════════════════════════════════════════
elif page == "🔄 Growth & Cohorts":

    if df_kpis.empty:
        st.warning("No KPI data found for the selected period.")
        st.stop()

    # Compute MoM deltas in Python (one lightweight pass)
    df_g = df_kpis.copy()
    df_g["mrr_growth"]     = df_g["total_mrr_usd"].diff()
    df_g["mrr_growth_pct"] = df_g["total_mrr_usd"].pct_change() * 100

    col1, col2 = st.columns([3, 2])

    # ── MRR Growth Bars ──
    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="card-head"><div><div class="section-title">Monthly MRR Growth</div><div class="section-sub">New MRR added each month</div></div><span class="tag">12 Months</span></div>', unsafe_allow_html=True)
        bar_colors = [COLORS["purple"] if i == len(df_g) - 1 else COLORS["blue"] for i in range(len(df_g))]
        fig_g = go.Figure(go.Bar(
            x=df_g["month"], y=df_g["mrr_growth"],
            marker_color=bar_colors, marker_line_width=0,
            text=[f"+{fmt_usd(v)}" if not pd.isna(v) else "" for v in df_g["mrr_growth"]],
            textposition="outside",
            textfont=dict(size=9, color="#5A6580"),
        ))
        fig_g.update_layout(**PLOTLY_LAYOUT, height=250, showlegend=False)
        fig_g.update_xaxes(tickformat="%b %Y")
        fig_g.update_yaxes(tickprefix="$", tickformat=",.0f")
        st.plotly_chart(fig_g, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    # ── MoM % ──
    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="card-head"><div><div class="section-title">MRR Growth Rate %</div><div class="section-sub">Month-over-month</div></div><span class="tag purple">MoM</span></div>', unsafe_allow_html=True)
        fig_pct = go.Figure(go.Scatter(
            x=df_g["month"], y=df_g["mrr_growth_pct"],
            mode="lines+markers",
            line=dict(color=COLORS["purple"], width=2.5),
            marker=dict(size=6, color=COLORS["purple"], line=dict(color="white", width=1.5)),
            fill="tozeroy", fillcolor="rgba(124,58,237,0.08)",
        ))
        fig_pct.update_layout(**PLOTLY_LAYOUT, height=250, showlegend=False)
        fig_pct.update_xaxes(tickformat="%b %Y")
        fig_pct.update_yaxes(ticksuffix="%")
        st.plotly_chart(fig_pct, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── Cohort Heatmap ──
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="card-head"><div><div class="section-title">Cohort Retention Heatmap</div><div class="section-sub">% of users from each cohort still active at month N</div></div><span class="tag purple">Retention</span></div>', unsafe_allow_html=True)

    if not df_cohorts.empty:
        cohort_pivot = df_cohorts.pivot_table(
            index="cohort_month",
            columns="months_since_signup",
            values="active_users",
            aggfunc="sum",
        )
        # Retention % normalised to M0
        cohort_pct = cohort_pivot.div(cohort_pivot.get(0, cohort_pivot.iloc[:, 0]), axis=0) * 100
        cohort_labels = cohort_pct.index.strftime("%b '%y").tolist()
        months_cols   = [f"M{int(c)}" for c in cohort_pct.columns]
        z_vals = cohort_pct.values.tolist()
        z_text = [[f"{v:.0f}%" if not np.isnan(v) else "—" for v in row] for row in z_vals]

        fig_h = go.Figure(go.Heatmap(
            z=z_vals, x=months_cols, y=cohort_labels,
            text=z_text, texttemplate="%{text}",
            colorscale=[
                [0.0,  "#DBEAFE"], [0.25, "#93C5FD"],
                [0.5,  "#60A5FA"], [0.75, "#2563EB"], [1.0, "#1D4ED8"],
            ],
            showscale=True,
            colorbar=dict(thickness=12, len=0.8, ticksuffix="%", tickfont_size=10, outlinewidth=0),
            zmin=0, zmax=100, xgap=3, ygap=3,
        ))
        fig_h.update_layout(
            **{**PLOTLY_LAYOUT,
               "xaxis": dict(side="top", tickfont_size=11, tickfont_color="#5A6580", showgrid=False),
               "yaxis": dict(tickfont_size=11, tickfont_color="#0F1729", showgrid=False, autorange="reversed"),
            },
            height=max(220, len(cohort_labels) * 44),
        )
        st.plotly_chart(fig_h, use_container_width=True, config={"displayModeBar": False})
    else:
        st.info("No cohort data available for the selected period.")

    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown("<hr>", unsafe_allow_html=True)

    # ── Growth stat cards ──
    last_k = df_kpis.iloc[-1]
    prev_k = df_kpis.iloc[-2] if len(df_kpis) >= 2 else last_k

    def gk(col):
        p = prev_k[col]
        return float((last_k[col] - p) / p * 100) if p else 0.0

    new_users = int(last_k["active_paying_users"] - prev_k["active_paying_users"])
    c1, c2, c3, c4 = st.columns(4)
    for col, label, val, grw, color in [
        (c1, "MRR Growth Rate",  f"{gk('total_mrr_usd'):+.1f}%",      gk("total_mrr_usd"),   "blue"),
        (c2, "New Paying Users", f"+{new_users:,}",                     gk("active_paying_users"), "green"),
        (c3, "Revenue Growth",   f"{gk('revenue_usd'):+.1f}%",         gk("revenue_usd"),     "purple"),
        (c4, "ARPU Change",      f"{gk('arpu'):+.1f}%",                 gk("arpu"),            "orange"),
    ]:
        col.markdown(kpi_card(label, val, grw, "vs prev month", color), unsafe_allow_html=True)
