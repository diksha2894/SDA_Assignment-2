import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="Financial Market Streaming Dashboard",
    page_icon="📈",
    layout="wide"
)

# --------------------------------------------------
# MONGODB CONNECTION
# --------------------------------------------------

MONGO_URI = "mongodb://localhost:27017"

client = MongoClient(MONGO_URI)

db = client["finance"]
collection = db["stock_data"]


# --------------------------------------------------
# DASHBOARD TITLE
# --------------------------------------------------

st.title("📈 Real-Time Financial Market Streaming Analytics")

st.markdown(
    """
    **Data Pipeline:**  
    CSV → Python Producer → Apache Kafka → Consumer → MongoDB → Dashboard
    """
)


# --------------------------------------------------
# LIVE DASHBOARD FUNCTION
# --------------------------------------------------

@st.fragment(run_every="5s")
def live_dashboard():

    # Read data from MongoDB
    records = list(collection.find({}, {"_id": 0}))

    if not records:
        st.warning("No data found in MongoDB.")
        return

    # Convert MongoDB records into DataFrame
    df = pd.DataFrame(records)

    # Convert data types
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    numeric_columns = [
        "current_price",
        "volume",
        "previous_price",
        "price_change",
        "percentage_change",
        "moving_average_3"
    ]

    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # Sort data
    df = df.sort_values("timestamp")

    # --------------------------------------------------
    # SIDEBAR FILTER
    # --------------------------------------------------

    st.sidebar.header("Filters")

    symbols = sorted(df["symbol"].dropna().unique())

    selected_symbols = st.sidebar.multiselect(
        "Select Stock Symbol",
        symbols,
        default=symbols
    )

    filtered_df = df[
        df["symbol"].isin(selected_symbols)
    ]

    if filtered_df.empty:
        st.warning("No data available for the selected stock.")
        return

    # --------------------------------------------------
    # KPI SECTION
    # --------------------------------------------------

    latest = filtered_df.sort_values(
        "timestamp"
    ).iloc[-1]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Latest Price",
        f"{latest['current_price']:.2f}"
    )

    col2.metric(
        "Latest Volume",
        f"{int(latest['volume']):,}"
    )

    col3.metric(
        "Price Change",
        f"{latest['price_change']:.2f}"
    )

    col4.metric(
        "Percentage Change",
        f"{latest['percentage_change']:.4f}%"
    )

    st.divider()

    # --------------------------------------------------
    # CHART 1 — STOCK PRICE TREND
    # --------------------------------------------------

    st.subheader("1. Stock Price Trend")

    fig_price = px.line(
        filtered_df,
        x="timestamp",
        y="current_price",
        color="symbol",
        markers=True,
        title="Stock Price Movement Over Time"
    )

    fig_price.update_layout(
        xaxis_title="Time",
        yaxis_title="Price",
        legend_title="Stock"
    )

    st.plotly_chart(
        fig_price,
        use_container_width=True
    )

    # --------------------------------------------------
    # CHART 2 — TRADING VOLUME
    # --------------------------------------------------

    st.subheader("2. Trading Volume")

    fig_volume = px.bar(
        filtered_df,
        x="timestamp",
        y="volume",
        color="symbol",
        title="Trading Volume Over Time"
    )

    fig_volume.update_layout(
        xaxis_title="Time",
        yaxis_title="Volume"
    )

    st.plotly_chart(
        fig_volume,
        use_container_width=True
    )

    # --------------------------------------------------
    # CHART 3 — PERCENTAGE PRICE CHANGE
    # --------------------------------------------------

    st.subheader("3. Percentage Price Change")

    fig_change = px.line(
        filtered_df,
        x="timestamp",
        y="percentage_change",
        color="symbol",
        markers=True,
        title="Short-Term Percentage Price Change"
    )

    fig_change.update_layout(
        xaxis_title="Time",
        yaxis_title="Percentage Change (%)"
    )

    st.plotly_chart(
        fig_change,
        use_container_width=True
    )

    # --------------------------------------------------
    # CHART 4 — PRICE VS MOVING AVERAGE
    # --------------------------------------------------

    st.subheader("4. Price vs 3-Record Moving Average")

    fig_ma = px.line(
        filtered_df,
        x="timestamp",
        y=[
            "current_price",
            "moving_average_3"
        ],
        color="symbol",
        markers=True,
        title="Current Price vs 3-Record Moving Average"
    )

    st.plotly_chart(
        fig_ma,
        use_container_width=True
    )

    # --------------------------------------------------
    # DATA TABLE
    # --------------------------------------------------

    st.subheader("Latest Streaming Records")

    st.dataframe(
        filtered_df.sort_values(
            "timestamp",
            ascending=False
        ).head(10),
        use_container_width=True
    )


# --------------------------------------------------
# RUN DASHBOARD
# --------------------------------------------------

live_dashboard()
