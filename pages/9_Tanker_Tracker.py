"""Fuel Tanker Tracker — placeholder for AIS-based live tracking."""

import streamlit as st
import plotly.graph_objects as go
from port_scraper import AU_PORT_COORDS

st.set_page_config(page_title="Fuel Tanker Tracker", page_icon="🗺️", layout="wide")
st.title("Fuel Tanker Tracker")
st.caption(
    "Real-time map of fuel tankers approaching Australian ports — "
    "with route overlays, days-to-arrival countdown, and vessel details."
)

st.info(
    "**Coming soon** — This page will show a live world map tracking fuel tankers "
    "en route to Australia using AIS (Automatic Identification System) data. "
    "Features planned:\n\n"
    "- Live tanker positions on an interactive map\n"
    "- Route lines showing approach to Australian ports\n"
    "- Days-away overlay labels on each vessel\n"
    "- Click a tanker to see cargo type, DWT, origin, ETA\n"
    "- Coverage of all Australian fuel ports including Kwinana, Adelaide, Darwin, Hobart\n\n"
    "**Data source:** AISStream.io WebSocket API (API key ready)"
)

# Placeholder map showing Australian ports
fig = go.Figure()

port_names = list(AU_PORT_COORDS.keys())
port_lats = [AU_PORT_COORDS[p][0] for p in port_names]
port_lons = [AU_PORT_COORDS[p][1] for p in port_names]

fig.add_trace(go.Scattergeo(
    lat=port_lats, lon=port_lons,
    text=port_names,
    mode="markers+text",
    textposition="top center",
    marker=dict(size=8, color="red", symbol="diamond"),
    name="Fuel Ports",
    textfont=dict(size=9),
))

fig.update_geos(
    projection_type="natural earth",
    showcoastlines=True, coastlinecolor="gray",
    showland=True, landcolor="rgb(243, 243, 243)",
    showocean=True, oceancolor="rgb(204, 224, 245)",
    center=dict(lat=-25, lon=134),
    lataxis_range=[-50, 10],
    lonaxis_range=[90, 180],
)
fig.update_layout(height=600, margin=dict(l=0, r=0, t=0, b=0), showlegend=False)
st.plotly_chart(fig, use_container_width=True)
