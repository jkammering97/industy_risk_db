from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from fetch_data_products import load_partner_areas
from risk_sql_pipeline import get_sql_engine, normalize_country_code


def load_layer(engine, table_name: str, country_code: str) -> tuple[pd.DataFrame, str]:
    reporter_codes = [str(country_code)]
    if str(country_code).isdigit():
        compact = str(int(str(country_code)))
        if compact not in reporter_codes:
            reporter_codes.append(compact)

    code_params = {f"code_{idx}": code for idx, code in enumerate(reporter_codes)}
    placeholders = ", ".join(f":code_{idx}" for idx in range(len(reporter_codes)))
    query = text(
        f"""
        select *
        from mart.{table_name}
        where cast(reporter_code as varchar(16)) in ({placeholders})
        """
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params=code_params)
        return df, ""
    except Exception as exc:
        return pd.DataFrame(), str(exc)


def load_layer_all(engine, table_name: str) -> tuple[pd.DataFrame, str]:
    query = text(f"select * from mart.{table_name}")
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        return df, ""
    except Exception as exc:
        return pd.DataFrame(), str(exc)


def build_trade_sunburst(hhi_df: pd.DataFrame):
    if hhi_df.empty:
        return None, "No HHI rows available for sunburst."

    exports_shares = hhi_df[hhi_df["flow_code"].astype(str).str.upper() == "X"].copy()
    imports_shares = hhi_df[hhi_df["flow_code"].astype(str).str.upper() == "M"].copy()

    def ensure_country_col(df):
        if "country" in df.columns:
            return df
        if "supplier_country" in df.columns:
            df = df.copy()
            df["country"] = df["supplier_country"]
            return df
        if "supplier_country_code" in df.columns:
            df = df.copy()
            df["country"] = df["supplier_country_code"].astype(str)
            return df
        df = df.copy()
        df["country"] = df.index.astype(str)
        return df

    def ensure_value_col(df, name="value_for_chart"):
        df = df.copy()
        if "trade_value_usd" in df.columns:
            values = pd.to_numeric(df["trade_value_usd"], errors="coerce").fillna(0)
        else:
            values = pd.Series(1.0, index=df.index)

        values = values.clip(lower=0)
        if len(values) > 0 and values.sum() <= 0:
            values = pd.Series(1.0, index=df.index)

        df[name] = values
        return df

    exports_shares = ensure_country_col(exports_shares)
    imports_shares = ensure_country_col(imports_shares)
    exports_shares = ensure_value_col(exports_shares)
    imports_shares = ensure_value_col(imports_shares)

    if "value_share" not in exports_shares.columns and "hhi_component" in exports_shares.columns:
        exports_shares["value_share"] = pd.to_numeric(exports_shares["hhi_component"], errors="coerce").fillna(0)
    if "value_share" not in imports_shares.columns and "hhi_component" in imports_shares.columns:
        imports_shares["value_share"] = pd.to_numeric(imports_shares["hhi_component"], errors="coerce").fillna(0)

    exports_shares["flow"] = "Exports"
    imports_shares["flow"] = "Imports"
    combined = pd.concat([exports_shares, imports_shares], ignore_index=True, sort=False)

    if combined.empty:
        return None, "No import/export rows are available for this country and filters."

    combined["color_val"] = 0.0
    for flow_label, lo, hi in [("Exports", 0.0, 0.5), ("Imports", 0.5, 1.0)]:
        mask = combined["flow"] == flow_label
        if "value_share" in combined.columns:
            arr = combined.loc[mask, "value_share"].fillna(0).to_numpy()
        elif "weight_share" in combined.columns:
            arr = combined.loc[mask, "weight_share"].fillna(0).to_numpy()
        else:
            arr = combined.loc[mask, "value_for_chart"].fillna(0).to_numpy()

        if arr.size > 0 and arr.max() > arr.min():
            norm = (arr - arr.min()) / (arr.max() - arr.min())
        else:
            norm = np.zeros_like(arr)

        combined.loc[mask, "color_val"] = lo + norm * (hi - lo)

    blues = px.colors.sequential.Blues
    oranges = px.colors.sequential.Oranges
    scale = []
    for i, c in enumerate(blues):
        pos = (i / (len(blues) - 1)) * 0.5
        scale.append((pos, c))
    for i, c in enumerate(oranges):
        pos = 0.5 + (i / (len(oranges) - 1)) * 0.5
        scale.append((pos, c))

    weights = combined["value_for_chart"].fillna(0).to_numpy()
    if weights.sum() > 0:
        midpoint = np.average(combined["color_val"], weights=weights)
    else:
        midpoint = float(combined["color_val"].mean()) if len(combined) else 0.5

    fig = px.sunburst(
        combined,
        path=["flow", "country"],
        values="value_for_chart",
        color="color_val",
        color_continuous_scale=scale,
        color_continuous_midpoint=midpoint,
    )
    fig.update_layout(
        margin=dict(t=8, l=8, r=8, b=8),
        coloraxis_showscale=False,
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig, ""


st.set_page_config(page_title="SME Import Risk Observer (SQL)", layout="wide")
st.title("SME Import Risk Observer (SQL)")
st.caption("Azure SQL + dbt marts: HHI, logistics, policy")

script_dir = Path(__file__).parent
partner_areas_file = script_dir / "partnerAreas.json"
partner_map = load_partner_areas(str(partner_areas_file))

country_names = sorted(partner_map.values()) if partner_map else ["Austria"]
selected_country_name = st.sidebar.selectbox(
    "Country at risk",
    country_names,
    index=country_names.index("Austria") if "Austria" in country_names else 0,
)

country_code = None
for code, name in partner_map.items():
    if name == selected_country_name:
        country_code = code
        break
country_code = normalize_country_code(country_code or "040")

try:
    engine = get_sql_engine()
except Exception as exc:
    st.error(f"Azure SQL connection settings missing or invalid: {exc}")
    st.stop()

supplier_df, supplier_err = load_layer(engine, "supplier_risk", country_code)
hhi_df, hhi_err = load_layer(engine, "hhi_layer", country_code)
logistics_df, logistics_err = load_layer(engine, "logistics_layer", country_code)
policy_df, policy_err = load_layer(engine, "policy_layer", country_code)

errors = [err for err in [supplier_err, hhi_err, logistics_err, policy_err] if err]
if errors:
    st.error(f"SQL read error: {errors[0]}")
    st.stop()

st.subheader(f"Selected Country: {selected_country_name} ({country_code})")

sunburst_fig, sunburst_msg = build_trade_sunburst(hhi_df)

left_col, right_col = st.columns([1.1, 1], gap="large")

with left_col:
    metric_row_1 = st.columns(2)
    metric_row_2 = st.columns(2)
    metric_row_1[0].metric("HHI Risk (avg)", f"{hhi_df['risk_score'].mean():.2f}" if not hhi_df.empty else "0.00")
    metric_row_1[1].metric("Logistics Risk (avg)", f"{logistics_df['risk_score'].mean():.2f}" if not logistics_df.empty else "0.00")
    metric_row_2[0].metric("Policy Risk (avg)", f"{policy_df['risk_score'].mean():.2f}" if not policy_df.empty else "0.00")
    metric_row_2[1].metric("Overall Risk (avg)", f"{supplier_df['overall_risk'].mean():.2f}" if not supplier_df.empty else "0.00")

    if supplier_df.empty:
        st.info("No supplier risk rows for this selected country.")
    else:
        fig = px.bar(
            supplier_df.sort_values("overall_risk", ascending=False).head(20),
            x="supplier_country",
            y="overall_risk",
            title="Top Supplier Risk (Import View)",
            labels={"supplier_country": "Supplier Country", "overall_risk": "Risk Score"},
        )
        fig.update_layout(
            margin=dict(t=52, l=8, r=8, b=8),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

with right_col:
    with st.container(border=True):
        st.markdown("#### Trade Shares")
        st.caption("Imports and exports by trading partner")
        if sunburst_fig is None:
            st.info(sunburst_msg or "No trade share rows available.")
        else:
            st.plotly_chart(sunburst_fig, use_container_width=True)

if supplier_df.empty:
    # If reporter code mapping differs (e.g., 040 vs 40), surface available data.
    supplier_all_df, all_err = load_layer_all(engine, "supplier_risk")
    if all_err:
        st.error(f"Could not query mart.supplier_risk: {all_err}")
        st.stop()
    if supplier_all_df.empty:
        st.info("No rows in mart.supplier_risk yet. Run ingestion + dbt first.")
    else:
        available_codes = sorted(
            supplier_all_df["reporter_code"].astype(str).dropna().unique().tolist()
        )
        st.warning(
            f"No rows for selected reporter_code {country_code}. "
            f"Available reporter_code values in mart.supplier_risk: {', '.join(available_codes)}"
        )
        with st.expander("Debug: unfiltered supplier_risk rows"):
            st.dataframe(supplier_all_df, use_container_width=True)

tabs = st.tabs(["Supplier Risk", "HHI Layer", "Logistics Layer", "Policy Layer"])
with tabs[0]:
    st.dataframe(supplier_df, use_container_width=True)
with tabs[1]:
    st.dataframe(hhi_df, use_container_width=True)
with tabs[2]:
    st.dataframe(logistics_df, use_container_width=True)
with tabs[3]:
    st.dataframe(policy_df, use_container_width=True)
