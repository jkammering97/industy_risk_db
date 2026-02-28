import streamlit as st
from fetch_data_products import load_partner_areas
import pandas as pd
from calculate_trade_risk import get_trade_risk
from pathlib import Path
import plotly.express as px
import numpy as np

st.title("Trade Risk Observer")

# Load partner areas
script_dir = Path(__file__).parent
partner_areas_file = script_dir / "partnerAreas.json"
partner_map = load_partner_areas(str(partner_areas_file))

# Get sorted list of country names
country_names = sorted(partner_map.values())

# Sidebar dropdown for country selection
st.sidebar.title("Select Country at Risk")
selected_country_name = st.sidebar.selectbox(
    "Choose a country:",
    country_names
)

# Find partner area id corresponding to the selected country name
country_id = None
for pid, name in partner_map.items():
    if name == selected_country_name:
        country_id = pid
        break

if country_id is None:
    # partner_map might be name->id (inverse); try direct lookup,
    # otherwise fall back to the selected name itself
    country_id = partner_map.get(selected_country_name, selected_country_name)

# Keep `selected_country` variable for backward compatibility
selected_country = selected_country_name

if selected_country:
    st.write(f"Selected country: {selected_country} (ID: {country_id})")

    # export/import trade risk data
    hhi_value_export, hhi_weight_export, exports_shares = get_trade_risk(country=country_id, flow_code="X", exclude_partner="World")
    hhi_value_import, hhi_weight_import, imports_shares = get_trade_risk(country=country_id, flow_code="M", exclude_partner="World")

    # Ensure `country` column exists
    def ensure_country_col(df):
        if 'country' in df.columns:
            return df
        if 'partnerText' in df.columns:
            df = df.copy()
            df['country'] = df['partnerText']
            return df
        if 'partnerCode' in df.columns:
            df = df.copy()
            df['country'] = df['partnerCode'].map(partner_map).fillna(df['partnerCode'])
            return df
        df = df.copy()
        df['country'] = df.index.astype(str)
        return df

    exports_shares = ensure_country_col(exports_shares)
    imports_shares = ensure_country_col(imports_shares)

    # Determine value for sizing the sunburst (prefer 'pop', then 'tradeValueUSD', then 'netWeightKg')
    def ensure_value_col(df, name='value_for_chart'):
        df = df.copy()
        if 'pop' in df.columns:
            values = pd.to_numeric(df['pop'], errors='coerce').fillna(0)
        elif 'tradeValueUSD' in df.columns:
            values = pd.to_numeric(df['tradeValueUSD'], errors='coerce').fillna(0)
        elif 'netWeightKg' in df.columns:
            values = pd.to_numeric(df['netWeightKg'], errors='coerce').fillna(0)
        else:
            values = pd.Series(1.0, index=df.index)

        values = values.clip(lower=0)
        if len(values) > 0 and values.sum() <= 0:
            # Keep tiny/zero-only countries visible instead of failing chart math.
            values = pd.Series(1.0, index=df.index)

        df[name] = values
        return df

    exports_shares = ensure_value_col(exports_shares)
    imports_shares = ensure_value_col(imports_shares)

    exports_shares['flow'] = 'Exports'
    imports_shares['flow'] = 'Imports'

    combined = pd.concat([exports_shares, imports_shares], ignore_index=True, sort=False)

    # Build a color value that maps exports to [0,0.5] and imports to [0.5,1.0]
    combined['color_val'] = 0.0
    for flow_label, lo, hi in [('Exports', 0.0, 0.5), ('Imports', 0.5, 1.0)]:
        mask = combined['flow'] == flow_label
        if 'value_share' in combined.columns:
            arr = combined.loc[mask, 'value_share'].fillna(0).to_numpy()
        elif 'weight_share' in combined.columns:
            arr = combined.loc[mask, 'weight_share'].fillna(0).to_numpy()
        else:
            arr = combined.loc[mask, 'value_for_chart'].fillna(0).to_numpy()

        if arr.size > 0 and arr.max() > arr.min():
            norm = (arr - arr.min()) / (arr.max() - arr.min())
        else:
            norm = np.zeros_like(arr)

        combined.loc[mask, 'color_val'] = lo + norm * (hi - lo)

    # Create a combined continuous color scale: Blues for exports (0-0.5), Oranges for imports (0.5-1)
    blues = px.colors.sequential.Blues
    oranges = px.colors.sequential.Oranges
    scale = []
    # map blues to 0..0.5
    for i, c in enumerate(blues):
        pos = (i / (len(blues) - 1)) * 0.5
        scale.append((pos, c))
    # map oranges to 0.5..1.0
    for i, c in enumerate(oranges):
        pos = 0.5 + (i / (len(oranges) - 1)) * 0.5
        scale.append((pos, c))

    if combined.empty:
        st.info('No import/export rows are available for this country and filters.')
    else:
        weights = combined['value_for_chart'].fillna(0).to_numpy()
        if weights.sum() > 0:
            midpoint = np.average(combined['color_val'], weights=weights)
        else:
            midpoint = float(combined['color_val'].mean()) if len(combined) else 0.5

        # Build sunburst
        fig = px.sunburst(
            combined,
            path=['flow', 'country'],
            values='value_for_chart',
            color='color_val',
            color_continuous_scale=scale,
            color_continuous_midpoint=midpoint
        )

        st.plotly_chart(fig, use_container_width=True)

    # Display HHI values and the shares dataframes for each flow
    st.subheader('HHI and Shares')
    col1, col2 = st.columns(2)
    with col1:
        st.write('Exports')
        st.write(f'HHI (value): {hhi_value_export:.6f}')
        st.write(f'HHI (weight): {hhi_weight_export:.6f}')
        st.dataframe(exports_shares.head(100))
    with col2:
        st.write('Imports')
        st.write(f'HHI (value): {hhi_value_import:.6f}')
        st.write(f'HHI (weight): {hhi_weight_import:.6f}')
        st.dataframe(imports_shares.head(100))

    
    
