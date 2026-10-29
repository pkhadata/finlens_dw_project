import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

# ============================================================
# FINLENS DATA WAREHOUSE - STREAMLIT SERVING LAYER
# PostgreSQL -> Snowflake Bronze -> Silver -> Gold -> Streamlit
# ============================================================

load_dotenv()

st.set_page_config(
    page_title="FinLens Data Warehouse",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# Configuration
# -----------------------------

def get_setting(name: str, default=None):
    """Read Streamlit Cloud secrets first, then local environment variables."""
    try:
        value = st.secrets.get(name)
        if value not in (None, ""):
            return value
    except Exception:
        pass
    return os.getenv(name, default)


SNOWFLAKE_CONFIG = {
    "account": get_setting("SNOWFLAKE_ACCOUNT"),
    "user": get_setting("SNOWFLAKE_USER"),
    "password": get_setting("SNOWFLAKE_PASSWORD"),
    "warehouse": get_setting("SNOWFLAKE_WAREHOUSE", "FINLENS_WH"),
    "database": get_setting("SNOWFLAKE_DATABASE", "FINLENS_DW"),
    "role": get_setting("SNOWFLAKE_ROLE", "SYSADMIN"),
}


# -----------------------------
# Snowflake connection
# -----------------------------

@st.cache_resource
def get_connection():
    missing = [
        key
        for key in ("account", "user", "password")
        if not SNOWFLAKE_CONFIG.get(key)
    ]

    if missing:
        raise RuntimeError(
            "Missing Snowflake configuration: "
            + ", ".join(missing)
            + ". Configure .env locally or Streamlit Secrets."
        )

    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        role=SNOWFLAKE_CONFIG["role"],
        client_session_keep_alive=False,
    )


@st.cache_data(ttl=300, show_spinner=False)
def query_df(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


def scalar(sql: str):
    df = query_df(sql)
    if df.empty:
        return None
    return df.iloc[0, 0]


# -----------------------------
# Data access
# -----------------------------

@st.cache_data(ttl=300, show_spinner=False)
def get_pipeline_log():
    return query_df(
        """
        SELECT
            RUN_ID,
            PIPELINE_NAME,
            TABLE_NAME,
            STARTED_AT,
            FINISHED_AT,
            STATUS,
            ROWS_LOADED,
            ERROR_MESSAGE
        FROM OPS.ETL_RUN_LOG
        ORDER BY STARTED_AT DESC
        LIMIT 100
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_latest_pipeline():
    return query_df(
        """
        SELECT
            RUN_ID,
            PIPELINE_NAME,
            MIN(STARTED_AT) AS STARTED_AT,
            MAX(FINISHED_AT) AS FINISHED_AT,
            COUNT(*) AS LOG_ENTRIES,
            COUNT_IF(STATUS = 'SUCCESS') AS SUCCESS_ENTRIES,
            COUNT_IF(STATUS <> 'SUCCESS') AS FAILED_ENTRIES,
            SUM(COALESCE(ROWS_LOADED, 0)) AS ROWS_LOADED
        FROM OPS.ETL_RUN_LOG
        WHERE RUN_ID = (
            SELECT RUN_ID
            FROM OPS.ETL_RUN_LOG
            ORDER BY STARTED_AT DESC
            LIMIT 1
        )
        GROUP BY RUN_ID, PIPELINE_NAME
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_gold_counts():
    return query_df(
        """
        SELECT 'STOCK_PERFORMANCE' AS TABLE_NAME, COUNT(*) AS ROW_COUNT
        FROM GOLD.STOCK_PERFORMANCE
        UNION ALL
        SELECT 'SECTOR_PERFORMANCE', COUNT(*)
        FROM GOLD.SECTOR_PERFORMANCE
        UNION ALL
        SELECT 'MARKET_PERFORMANCE', COUNT(*)
        FROM GOLD.MARKET_PERFORMANCE
        UNION ALL
        SELECT 'BOND_ANALYSIS', COUNT(*)
        FROM GOLD.BOND_ANALYSIS
        ORDER BY TABLE_NAME
        """
    )


# -----------------------------
# Gold analytics
# -----------------------------

@st.cache_data(ttl=300, show_spinner=False)
def get_stock_summary():
    return query_df(
        """
        SELECT
            SYMBOL,
            MAX(COMPANY_NAME) AS COMPANY_NAME,
            COUNT(*) AS OBSERVATION_DAYS,
            AVG(DAILY_RETURN_PCT) AS AVG_DAILY_RETURN_PCT,
            SUM(VOLUME) AS TOTAL_VOLUME,
            AVG(VOLATILITY_5D_PCT) AS AVG_VOLATILITY_5D_PCT,
            MAX(CUMULATIVE_RETURN_PCT) AS MAX_CUMULATIVE_RETURN_PCT
        FROM GOLD.STOCK_PERFORMANCE
        GROUP BY SYMBOL
        ORDER BY AVG_DAILY_RETURN_PCT DESC
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_stock_daily():
    return query_df(
        """
        SELECT
            DATE,
            SYMBOL,
            COMPANY_NAME,
            CLOSE,
            VOLUME,
            DAILY_RETURN_PCT,
            CUMULATIVE_RETURN_PCT,
            VOLATILITY_5D_PCT,
            INTRADAY_RANGE_PCT
        FROM GOLD.STOCK_PERFORMANCE
        ORDER BY DATE, SYMBOL
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_sector_summary():
    return query_df(
        """
        SELECT
            SECTOR_CODE,
            SECTOR_NAME,
            AVG(AVG_DAILY_RETURN_PCT) AS AVG_SECTOR_RETURN_PCT,
            AVG(AVG_INTRADAY_RANGE_PCT) AS AVG_INTRADAY_RANGE_PCT,
            SUM(TOTAL_VOLUME) AS TOTAL_VOLUME
        FROM GOLD.SECTOR_PERFORMANCE
        WHERE AVG_DAILY_RETURN_PCT IS NOT NULL
        GROUP BY SECTOR_CODE, SECTOR_NAME
        ORDER BY AVG_SECTOR_RETURN_PCT DESC
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_sector_daily():
    return query_df(
        """
        SELECT
            DATE,
            SECTOR_CODE,
            SECTOR_NAME,
            STOCK_COUNT,
            TOTAL_VOLUME,
            AVG_DAILY_RETURN_PCT,
            AVG_INTRADAY_RANGE_PCT,
            BEST_STOCK_RETURN_PCT,
            WORST_STOCK_RETURN_PCT
        FROM GOLD.SECTOR_PERFORMANCE
        ORDER BY DATE, SECTOR_NAME
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_market_daily():
    return query_df(
        """
        SELECT
            DATE,
            INDEX_COUNT,
            AVG_INDEX_CLOSE,
            TOTAL_VOLUME,
            AVG_DAILY_RETURN_PCT,
            BEST_INDEX_RETURN_PCT,
            WORST_INDEX_RETURN_PCT,
            MARKET_CUMULATIVE_RETURN_PCT
        FROM GOLD.MARKET_PERFORMANCE
        ORDER BY DATE
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_bond_summary():
    return query_df(
        """
        SELECT
            MATURITY_BUCKET,
            COUNT(*) AS BOND_COUNT,
            SUM(OUTSTANDING_VALUE_MVND) AS TOTAL_OUTSTANDING_MVND,
            AVG(COUPON_RATE_PCT) AS AVG_COUPON_RATE_PCT
        FROM GOLD.BOND_ANALYSIS
        GROUP BY MATURITY_BUCKET
        ORDER BY MATURITY_BUCKET
        """
    )


@st.cache_data(ttl=300, show_spinner=False)
def get_top_issuers():
    return query_df(
        """
        SELECT
            ISSUER,
            MAX(ISSUER_NAME) AS ISSUER_NAME,
            COUNT(*) AS BOND_COUNT,
            SUM(OUTSTANDING_VALUE_MVND) AS TOTAL_OUTSTANDING_MVND,
            AVG(COUPON_RATE_PCT) AS AVG_COUPON_RATE_PCT
        FROM GOLD.BOND_ANALYSIS
        GROUP BY ISSUER
        ORDER BY TOTAL_OUTSTANDING_MVND DESC
        LIMIT 10
        """
    )


# -----------------------------
# Helpers
# -----------------------------

def fmt_number(value, decimals=0):
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.{decimals}f}"


def safe_pct(value):
    if value is None or pd.isna(value):
        return "—"
    return f"{value:,.2f}%"


def status_badge(status):
    status = str(status).upper()
    if status == "SUCCESS":
        st.success("PASS")
    elif status in {"FAILED", "FAIL", "ERROR"}:
        st.error(status)
    else:
        st.warning(status)


# -----------------------------
# Sidebar
# -----------------------------

with st.sidebar:
    st.title("FinLens")
    st.caption("Data Warehouse & ETL Pipeline")

    st.markdown("---")
    st.markdown("### Architecture")
    st.markdown(
        """
        **PostgreSQL**
        ↓  
        **Snowflake Bronze**
        ↓  
        **Snowflake Silver**
        ↓  
        **Snowflake Gold**
        ↓  
        **Streamlit Serving**
        """
    )

    st.markdown("---")
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption("Data source: FINLENS_DW")
    st.caption(f"Warehouse: {SNOWFLAKE_CONFIG['warehouse']}")


# -----------------------------
# Header
# -----------------------------

st.title("FinLens Data Warehouse")
st.caption(
    "Snowflake Medallion Architecture • Data Quality • Gold Analytics"
)

try:
    latest = get_latest_pipeline()
    gold_counts = get_gold_counts()
except Exception as exc:
    st.error("Unable to connect to Snowflake.")
    st.exception(exc)
    st.stop()


# -----------------------------
# Overview
# -----------------------------

st.subheader("Pipeline Overview")

if latest.empty:
    st.warning("No ETL pipeline run was found in OPS.ETL_RUN_LOG.")
else:
    row = latest.iloc[0]

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.metric("Pipeline status", "PASS" if row["FAILED_ENTRIES"] == 0 else "FAIL")

    with c2:
        st.metric("Log entries", fmt_number(row["LOG_ENTRIES"]))

    with c3:
        st.metric("Successful steps", fmt_number(row["SUCCESS_ENTRIES"]))

    with c4:
        st.metric("Rows processed", fmt_number(row["ROWS_LOADED"]))

    st.caption(
        f"Run ID: {row['RUN_ID']} • "
        f"Pipeline: {row['PIPELINE_NAME']}"
    )


# -----------------------------
# Gold layer
# -----------------------------

st.subheader("Gold Layer")

cols = st.columns(4)

gold_labels = {
    "STOCK_PERFORMANCE": "Stock Performance",
    "SECTOR_PERFORMANCE": "Sector Performance",
    "MARKET_PERFORMANCE": "Market Performance",
    "BOND_ANALYSIS": "Bond Analysis",
}

for col, (_, item) in zip(cols, gold_counts.iterrows()):
    table_name = item["TABLE_NAME"]
    with col:
        st.metric(
            gold_labels.get(table_name, table_name),
            fmt_number(item["ROW_COUNT"]),
            help=f"Rows in GOLD.{table_name}",
        )


# -----------------------------
# Main tabs
# -----------------------------

tab_pipeline, tab_stocks, tab_sectors, tab_market, tab_bonds = st.tabs(
    [
        "⚙️ Pipeline & Quality",
        "📈 Stocks",
        "🏭 Sectors",
        "📊 Market",
        "💰 Bonds",
    ]
)


# ============================================================
# PIPELINE
# ============================================================

with tab_pipeline:
    st.subheader("Latest Pipeline Run")

    if latest.empty:
        st.info("No pipeline run available.")
    else:
        latest_run_id = latest.iloc[0]["RUN_ID"]
        log = get_pipeline_log()
        run_log = log[log["RUN_ID"] == latest_run_id].copy()

        if run_log.empty:
            st.info("No detailed log entries for the latest run.")
        else:
            display = run_log[
                [
                    "TABLE_NAME",
                    "STATUS",
                    "ROWS_LOADED",
                    "STARTED_AT",
                    "FINISHED_AT",
                    "ERROR_MESSAGE",
                ]
            ].copy()

            st.dataframe(
                display,
                use_container_width=True,
                hide_index=True,
            )

    st.markdown("### Gold Data Quality")

    quality = query_df(
        """
        SELECT
            'Stock grain' AS CHECK_NAME,
            COUNT(*) AS VIOLATIONS
        FROM (
            SELECT SYMBOL, DATE
            FROM GOLD.STOCK_PERFORMANCE
            GROUP BY SYMBOL, DATE
            HAVING COUNT(*) > 1
        )
        UNION ALL
        SELECT
            'Sector grain',
            COUNT(*)
        FROM (
            SELECT DATE, SECTOR_CODE, SECTOR_NAME
            FROM GOLD.SECTOR_PERFORMANCE
            GROUP BY DATE, SECTOR_CODE, SECTOR_NAME
            HAVING COUNT(*) > 1
        )
        UNION ALL
        SELECT
            'Market grain',
            COUNT(*)
        FROM (
            SELECT DATE
            FROM GOLD.MARKET_PERFORMANCE
            GROUP BY DATE
            HAVING COUNT(*) > 1
        )
        UNION ALL
        SELECT
            'Bond grain',
            COUNT(*)
        FROM (
            SELECT BOND_CODE
            FROM GOLD.BOND_ANALYSIS
            GROUP BY BOND_CODE
            HAVING COUNT(*) > 1
        )
        UNION ALL
        SELECT
            'Negative stock return below -100%',
            COUNT_IF(DAILY_RETURN_PCT < -100)
        FROM GOLD.STOCK_PERFORMANCE
        UNION ALL
        SELECT
            'Negative intraday range',
            COUNT_IF(INTRADAY_RANGE_PCT < 0)
        FROM GOLD.STOCK_PERFORMANCE
        UNION ALL
        SELECT
            'Negative outstanding ratio',
            COUNT_IF(OUTSTANDING_RATIO < 0)
        FROM GOLD.BOND_ANALYSIS
        """
    )

    quality["STATUS"] = quality["VIOLATIONS"].apply(
        lambda x: "PASS" if x == 0 else "FAIL"
    )

    st.dataframe(
        quality[["CHECK_NAME", "VIOLATIONS", "STATUS"]],
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# STOCKS
# ============================================================

with tab_stocks:
    st.subheader("Stock Performance")

    stocks = get_stock_summary()
    daily = get_stock_daily()

    if stocks.empty:
        st.info("No stock performance data available.")
    else:
        c1, c2 = st.columns(2)

        with c1:
            best = stocks.iloc[0]
            st.metric(
                "Best average daily return",
                f"{best['SYMBOL']} • {safe_pct(best['AVG_DAILY_RETURN_PCT'])}",
            )

        with c2:
            most_active = stocks.sort_values(
                "TOTAL_VOLUME", ascending=False
            ).iloc[0]
            st.metric(
                "Highest total volume",
                f"{most_active['SYMBOL']} • "
                f"{fmt_number(most_active['TOTAL_VOLUME'])}",
            )

        st.markdown("### Performance Summary")
        st.dataframe(
            stocks,
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("### Daily Return")

        symbols = sorted(daily["SYMBOL"].dropna().unique().tolist())
        selected = st.multiselect(
            "Select stocks",
            symbols,
            default=symbols[: min(3, len(symbols))],
        )

        filtered = daily[daily["SYMBOL"].isin(selected)].copy()

        if not filtered.empty:
            fig = px.line(
                filtered,
                x="DATE",
                y="DAILY_RETURN_PCT",
                color="SYMBOL",
                markers=True,
                title="Daily Return (%)",
            )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Daily Return (%)",
                legend_title="Symbol",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Cumulative Return")

        if not filtered.empty:
            fig = px.line(
                filtered,
                x="DATE",
                y="CUMULATIVE_RETURN_PCT",
                color="SYMBOL",
                markers=True,
                title="Cumulative Return (%)",
            )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Cumulative Return (%)",
            )
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SECTORS
# ============================================================

with tab_sectors:
    st.subheader("Sector Performance")

    sectors = get_sector_summary()
    sector_daily = get_sector_daily()

    if sectors.empty:
        st.info("No sector performance data available.")
    else:
        st.dataframe(
            sectors,
            use_container_width=True,
            hide_index=True,
        )

        fig = px.bar(
            sectors,
            x="SECTOR_NAME",
            y="AVG_SECTOR_RETURN_PCT",
            title="Average Sector Return (%)",
            text_auto=".2f",
        )
        fig.update_layout(
            xaxis_title="Sector",
            yaxis_title="Average Return (%)",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Sector Trading Activity")

        volume = (
            sector_daily.groupby("SECTOR_NAME", as_index=False)["TOTAL_VOLUME"]
            .sum()
            .sort_values("TOTAL_VOLUME", ascending=False)
        )

        fig = px.bar(
            volume,
            x="SECTOR_NAME",
            y="TOTAL_VOLUME",
            title="Total Sector Volume",
            text_auto=".2s",
        )
        fig.update_layout(
            xaxis_title="Sector",
            yaxis_title="Total Volume",
        )
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# MARKET
# ============================================================

with tab_market:
    st.subheader("Market Performance")

    market = get_market_daily()

    if market.empty:
        st.info("No market performance data available.")
    else:
        latest_market = market.iloc[-1]

        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric(
                "Latest index close",
                fmt_number(latest_market["AVG_INDEX_CLOSE"], 2),
            )

        with c2:
            st.metric(
                "Latest daily return",
                safe_pct(latest_market["AVG_DAILY_RETURN_PCT"]),
            )

        with c3:
            st.metric(
                "Market cumulative return",
                safe_pct(latest_market["MARKET_CUMULATIVE_RETURN_PCT"]),
            )

        fig = px.line(
            market,
            x="DATE",
            y="AVG_INDEX_CLOSE",
            title="Market Index Level",
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Average Index Close",
        )
        st.plotly_chart(fig, use_container_width=True)

        fig = px.bar(
            market,
            x="DATE",
            y="AVG_DAILY_RETURN_PCT",
            title="Daily Market Return (%)",
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Return (%)",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Market Data")
        st.dataframe(
            market,
            use_container_width=True,
            hide_index=True,
        )


# ============================================================
# BONDS
# ============================================================

with tab_bonds:
    st.subheader("Corporate Bond Analysis")

    bond_summary = get_bond_summary()
    issuers = get_top_issuers()

    c1, c2 = st.columns(2)

    with c1:
        total_bonds = scalar("SELECT COUNT(*) FROM GOLD.BOND_ANALYSIS")
        st.metric("Total bonds", fmt_number(total_bonds))

    with c2:
        total_outstanding = scalar(
            """
            SELECT SUM(OUTSTANDING_VALUE_MVND)
            FROM GOLD.BOND_ANALYSIS
            """
        )
        st.metric(
            "Outstanding value",
            f"{fmt_number(total_outstanding, 2)} MVND",
        )

    st.markdown("### Maturity Distribution")

    if not bond_summary.empty:
        st.dataframe(
            bond_summary,
            use_container_width=True,
            hide_index=True,
        )

        fig = px.bar(
            bond_summary,
            x="MATURITY_BUCKET",
            y="BOND_COUNT",
            title="Bonds by Maturity Bucket",
            text_auto=True,
        )
        fig.update_layout(
            xaxis_title="Maturity Bucket",
            yaxis_title="Bond Count",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Top Issuers by Outstanding Value")

    st.dataframe(
        issuers,
        use_container_width=True,
        hide_index=True,
    )

    if not issuers.empty:
        fig = px.bar(
            issuers.sort_values("TOTAL_OUTSTANDING_MVND"),
            x="TOTAL_OUTSTANDING_MVND",
            y="ISSUER_NAME",
            orientation="h",
            title="Top 10 Issuers",
            text_auto=".2s",
        )
        fig.update_layout(
            xaxis_title="Outstanding Value (MVND)",
            yaxis_title="Issuer",
        )
        st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# Footer
# -----------------------------

st.markdown("---")
st.caption(
    "FinLens Data Warehouse • PostgreSQL → Snowflake Bronze → Silver → Gold → Streamlit"
)
