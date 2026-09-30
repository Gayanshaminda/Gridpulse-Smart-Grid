import os
from datetime import timedelta
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import select, func

from grid.config import DAY_SECONDS, HOUSEHOLDS
from grid.queries import latest_readings, daily_results
from grid.store import engine, init, readings, rejects, health, get_state, put_state

st.set_page_config(page_title="GridPulse | Energy intelligence", page_icon="⚡", layout="wide")
st.markdown("""<style>
.stApp {background:#07111f;color:#e7eef8}
[data-testid="stSidebar"] {background:#0c1c2d}
[data-testid="stMetric"] {background:#102236;border:1px solid #1c3950;border-radius:14px;padding:20px}
[data-testid="stMetricLabel"] {color:#9fb2c9}
[data-testid="stMetricValue"] {color:#58e1c1;font-size:2rem}
.eyebrow {color:#58e1c1;letter-spacing:3px;font-size:12px;font-weight:700}
h1 {letter-spacing:-1.5px} div[data-testid="stToolbar"] {visibility:hidden}
</style>""", unsafe_allow_html=True)


@st.cache_resource
def connection():
    db = engine()
    init(db)
    return db


db = connection()
preview = os.getenv("DEMO_MODE", "false") == "true"
st.sidebar.markdown("## ⚡ GridPulse")
st.sidebar.caption("SMART GRID OBSERVATORY")
page = st.sidebar.radio("Workspace", ["Live energy", "Daily billing", "Pipeline health"])
st.sidebar.divider()
st.sidebar.write(f"**{HOUSEHOLDS}** households · **3** zones")
st.sidebar.caption(f"1 simulated day = {DAY_SECONDS} real seconds of uninterrupted emission")
st.sidebar.caption("Currency: LKR · Simplified teaching tariffs")
if preview:
    st.sidebar.warning("Offline preview\n\nSaved sample data. Kafka, Spark and Airflow are not running in this mode.")
else:
    cloudy = st.sidebar.toggle("Simulate cloudy weather", value=get_state(db, "cloudy", "false") == "true")
    with db.begin() as conn:
        put_state(conn, "cloudy", str(cloudy).lower())
    st.sidebar.caption("Clouds reduce solar generation on subsequent readings. Most visible during simulated daytime.")


def chart(fig):
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(family="Arial", color="#b8cadd"), margin=dict(l=0, r=15, t=20, b=0),
                      legend=dict(orientation="h", y=1.12, title=None))
    st.plotly_chart(fig, use_container_width=True)


@st.fragment(run_every="5s")
def render():
    st.markdown('<div class="eyebrow">ENERGY INTELLIGENCE / GRIDPULSE</div>', unsafe_allow_html=True)
    if page == "Live energy":
        st.title("Your grid, in focus.")
        st.caption("Provisional meter results · Automatically refreshed every 5 seconds")
        df = pd.DataFrame(latest_readings(db))
        if df.empty:
            st.info("Waiting for meter readings. Start the pipeline using the README instructions.")
            return
        latest = df.event_time.max()
        consumption = (df.consumption_kwh * 3600 / df.interval_seconds).sum()
        solar = (df.solar_generation_kwh * 3600 / df.interval_seconds).sum()
        imports = (df.grid_import_kwh * 3600 / df.interval_seconds).sum()
        share = 100 * df.solar_used_kwh.sum() / max(df.consumption_kwh.sum(), 0.000001)
        a, b, c, d = st.columns(4)
        a.metric("Demand", f"{consumption:.1f} kW")
        b.metric("Solar generation", f"{solar:.1f} kW")
        c.metric("Grid import", f"{imports:.1f} kW")
        d.metric("Demand supplied by solar", f"{share:.0f}%")
        st.caption(f"Latest simulated interval: {latest:%d %b %Y, %H:%M} UTC · {len(df)}/{HOUSEHOLDS} household readings")
        if 8 <= latest.hour < 17 and share < 15:
            st.warning("Low renewable contribution: solar supplies less than 15% of daytime demand.")
        if len(df) < HOUSEHOLDS:
            st.info("The latest interval is still arriving; the snapshot will update automatically.")
        with db.connect() as conn:
            history = pd.read_sql(select(readings).where(readings.c.event_time >= latest - timedelta(days=1)), conn)
        st.subheader("Demand and generation")
        for source, name in [("consumption_kwh", "Demand"), ("solar_generation_kwh", "Solar"), ("grid_import_kwh", "Grid import")]:
            history[name] = history[source] * 3600 / history.interval_seconds
        trend = history.groupby("event_time")[["Demand", "Solar", "Grid import"]].sum().reset_index()
        chart(px.line(trend, x="event_time", y=["Demand", "Solar", "Grid import"],
                      color_discrete_sequence=["#73a5ff", "#58e1c1", "#ffbf69"],
                      labels={"value": "Power (kW)", "event_time": "Simulated time"}))
        left, right = st.columns([1, 1])
        with left:
            st.subheader("Zone balance")
            zones = df.assign(Demand=df.consumption_kwh * 3600 / df.interval_seconds,
                              Solar=df.solar_generation_kwh * 3600 / df.interval_seconds)
            zones = zones.groupby("grid_zone")[["Demand", "Solar"]].sum().reset_index()
            chart(px.bar(zones, x="grid_zone", y=["Demand", "Solar"], barmode="group",
                         color_discrete_sequence=["#73a5ff", "#58e1c1"], labels={"value": "Power (kW)", "grid_zone": ""}))
        with right:
            st.subheader("Highest grid demand")
            ranked = df.assign(grid_kw=df.grid_import_kwh * 3600 / df.interval_seconds)
            st.dataframe(ranked.sort_values("grid_kw", ascending=False)[["household_id", "grid_zone", "grid_kw"]].head(8),
                         hide_index=True, use_container_width=True,
                         column_config={"household_id": "Household", "grid_zone": "Zone", "grid_kw": st.column_config.NumberColumn("Grid demand (kW)", format="%.2f")})
    elif page == "Daily billing":
        st.title("Every unit, accounted for.")
        st.caption("Authoritative daily snapshots · Rebuilt from historical readings and dated tariffs")
        df = pd.DataFrame(daily_results(db))
        if df.empty:
            st.info("The first daily report appears after one simulated day, plus the batch scheduling and processing delay.")
            return
        day = st.selectbox("Simulated billing date", sorted(df.simulated_date.unique(), reverse=True))
        current = df[df.simulated_date == day].copy()
        a, b, c, d = st.columns(4)
        a.metric("Households billed", len(current))
        b.metric("Grid energy", f"{current.grid_import_kwh.sum():.1f} kWh")
        c.metric("Solar used at home", f"{current.solar_used_kwh.sum():.1f} kWh")
        d.metric("Estimated bills (LKR)", f"{current.bill_lkr.sum():,.2f}")
        st.caption("Daily energy imports × tariff. Subsidised households receive 20% off. Exports receive no credit; taxes and fixed charges are excluded.")
        chart(px.bar(current.sort_values("bill_lkr", ascending=False), x="household_id", y="bill_lkr", color="grid_zone",
                     color_discrete_sequence=["#58e1c1", "#73a5ff", "#ffbf69"], labels={"bill_lkr": "Estimated bill (LKR)", "household_id": "Household"}))
        display_columns = ["household_id", "grid_zone", "bill_lkr", "grid_import_kwh", "tariff_rate",
                           "subsidy_flag", "consumption_kwh", "solar_generation_kwh", "solar_used_kwh",
                           "solar_export_kwh", "reading_count"]
        st.dataframe(current[display_columns], hide_index=True, use_container_width=True,
                     column_config={"household_id": "Household", "grid_zone": "Zone",
                                    "bill_lkr": st.column_config.NumberColumn("Bill (LKR)", format="%.2f"),
                                    "grid_import_kwh": st.column_config.NumberColumn("Grid import (kWh)", format="%.3f"),
                                    "tariff_rate": st.column_config.NumberColumn("LKR / kWh", format="%.2f"),
                                    "subsidy_flag": "Subsidised", "reading_count": "Readings"})
        st.download_button("Download daily report", current.to_csv(index=False), file_name=f"gridpulse-billing-{day}.csv", mime="text/csv")
        st.caption(f"Last reconciliation: {current.updated_at.max()} UTC. Live snapshots are displayed separately and are never added to daily totals.")
    else:
        st.title("A healthy flow of data.")
        st.caption("Arrival freshness, validation outcomes, and daily reconciliation")
        h = health(db)
        a, b, c, d = st.columns(4)
        a.metric("Accepted readings", f"{h['accepted_total']:,}")
        b.metric("Rejected readings", h["rejected_total"])
        c.metric("Recent rejection rate", f"{h['recent_error_rate']:.1%}")
        age = h["last_reading_age_seconds"]
        d.metric("Last valid arrival", "Waiting" if age is None else f"{age:.0f}s ago")
        if preview:
            st.info("Freshness alerts are expected in offline preview because its sample readings do not keep arriving.")
        for alert in h["alerts"]:
            st.warning(alert)
        if not h["alerts"]:
            st.success("Meter readings are arriving and no configured health rule is firing.")
        st.subheader("Daily processing")
        if h["batch_runs"]:
            st.dataframe(pd.DataFrame(h["batch_runs"]), hide_index=True, use_container_width=True)
        else:
            st.caption("No completed simulated days have been processed yet.")
        st.subheader("Processing checkpoints")
        st.write({"Last microbatch": get_state(db, "last_microbatch_id", "—"),
                  "Microbatch duration (seconds)": get_state(db, "last_microbatch_seconds", "—"),
                  "Stream heartbeat age (seconds)": h["stream_heartbeat_age_seconds"]})
        with db.connect() as conn:
            bad = pd.read_sql(select(rejects).order_by(rejects.c.received_at.desc()).limit(20), conn)
        if not bad.empty:
            st.subheader("Recent rejected readings")
            st.dataframe(bad, hide_index=True, use_container_width=True)
        st.caption("Metrics API: http://localhost:8000/docs · Airflow: http://localhost:8080 (full pipeline only)")


render()
