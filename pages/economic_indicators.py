import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE', 'DATAEXPERT_STUDENT')
snowflake_schema = os.getenv('STUDENT_SCHEMA')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')

# Page configuration
st.set_page_config(page_title="Economic Indicators", page_icon="📈", layout="wide")

# Title and description
st.title("Economic Indicators Analysis")
st.markdown("""
This dashboard analyzes economic indicators from the World Bank to provide context for inflation trends.
Explore how GDP per capita, GDP growth, and other economic factors relate to inflation rates.
""")

# Connect to Snowflake
@st.cache_data(ttl=3600)
def get_snowflake_connection():
    try:
        return snowflake.connector.connect(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse
        )
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        return None

# Function to load economic data
@st.cache_data(ttl=3600)
def load_economic_data():
    try:
        conn = get_snowflake_connection()
        if conn is None:
            raise Exception("Could not establish Snowflake connection")
            
        query = """
        SELECT 
            e.country_code,
            e.country_name,
            e.year,
            e.gdp_per_capita,
            e.cpi,
            e.inflation_rate,
            e.gdp_growth_rate
        FROM 
            INT_ECONOMIC_INDICATORS e
        ORDER BY
            e.country_name, e.year
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample economic data for demonstration purposes...")
        
        # Create sample data for demonstration
        countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium', 'Austria', 'Portugal']
        country_codes = ['DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT']
        
        # Generate years from 2018 to 2023
        years = list(range(2018, 2024))
        
        # Create sample data with realistic economic patterns
        data = []
        for i, country in enumerate(countries):
            base_gdp = 30000 + (i * 5000)  # Base GDP per capita varies by country
            for year in years:
                # GDP per capita grows over time with some country variation
                gdp_per_capita = base_gdp + ((year - 2018) * 1000) + (np.random.random() * 500)
                
                # CPI increases over time
                cpi = 100 + ((year - 2018) * 2) + (np.random.random() * 1.5)
                
                # Inflation rate varies by year with realistic patterns
                if year <= 2019:
                    inflation_rate = 1.5 + (np.random.random() - 0.5)
                elif year == 2020:
                    inflation_rate = 0.8 + (np.random.random() - 0.5)  # Lower in pandemic
                elif year == 2021:
                    inflation_rate = 2.5 + (np.random.random() * 1.5)  # Rising
                elif year == 2022:
                    inflation_rate = 5.0 + (np.random.random() * 3.0) + (i % 3)  # Peak
                else:  # 2023
                    inflation_rate = 3.0 + (np.random.random() * 2.0) - (i % 2)  # Declining
                
                # GDP growth rate also follows realistic patterns
                if year <= 2019:
                    gdp_growth_rate = 2.0 + (np.random.random() - 0.5) + (i % 2)
                elif year == 2020:
                    gdp_growth_rate = -5.0 + (np.random.random() * 3.0) - (i % 2)  # Pandemic contraction
                elif year == 2021:
                    gdp_growth_rate = 5.0 + (np.random.random() * 2.0) + (i % 3)  # Recovery
                elif year == 2022:
                    gdp_growth_rate = 2.5 + (np.random.random() - 0.5) - (i % 2)  # Normalizing
                else:  # 2023
                    gdp_growth_rate = 1.0 + (np.random.random() - 0.5) + (i % 2)  # Slowing
                
                data.append({
                    'country_code': country_codes[i],
                    'country_name': country,
                    'year': year,
                    'gdp_per_capita': gdp_per_capita,
                    'cpi': cpi,
                    'inflation_rate': inflation_rate,
                    'gdp_growth_rate': gdp_growth_rate
                })
        
        return pd.DataFrame(data)

# Function to load inflation data for comparison
@st.cache_data(ttl=3600)
def load_inflation_data():
    try:
        conn = get_snowflake_connection()
        if conn is None:
            raise Exception("Could not establish Snowflake connection")
        query = """
        SELECT 
            i.country_code,
            i.country_name,
            i.year,
            AVG(CASE WHEN i.product_code = 'CP00' THEN i.inflation_rate_yoy ELSE NULL END) as overall_inflation,
            AVG(CASE WHEN i.product_code = 'CP01' THEN i.inflation_rate_yoy ELSE NULL END) as food_inflation,
            AVG(CASE WHEN i.product_code = 'CP04' THEN i.inflation_rate_yoy ELSE NULL END) as housing_inflation,
            AVG(CASE WHEN i.product_code = 'CP07' THEN i.inflation_rate_yoy ELSE NULL END) as transport_inflation
        FROM 
            FACT_INFLATION_RATES i
        WHERE
            i.product_code IN ('CP00', 'CP01', 'CP04', 'CP07')
        GROUP BY
            i.country_code, i.country_name, i.year
        ORDER BY
            i.country_name, i.year
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample inflation category data for demonstration purposes...")
        
        # Create sample data for demonstration - using the same countries as economic data
        countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium', 'Austria', 'Portugal']
        country_codes = ['DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT']
        
        # Generate years from 2018 to 2023
        years = list(range(2018, 2024))
        
        # Create sample data with realistic inflation patterns by category
        data = []
        for i, country in enumerate(countries):
            for year in years:
                # Overall inflation follows economic patterns
                if year <= 2019:
                    overall_base = 1.5 + (i % 3) * 0.3
                elif year == 2020:
                    overall_base = 0.8 + (i % 3) * 0.2  # Lower in pandemic
                elif year == 2021:
                    overall_base = 2.5 + (i % 3) * 0.5  # Rising
                elif year == 2022:
                    overall_base = 5.0 + (i % 3) * 1.0  # Peak
                else:  # 2023
                    overall_base = 3.0 + (i % 3) * 0.5  # Declining
                
                # Add randomness
                overall_inflation = overall_base + (np.random.random() - 0.5)
                
                # Food inflation is typically higher
                food_inflation = overall_inflation * 1.2 + (np.random.random() - 0.5) * 0.5
                
                # Housing inflation varies more by country
                housing_inflation = overall_inflation * (0.8 + (i % 4) * 0.1) + (np.random.random() - 0.5) * 0.7
                
                # Transport inflation is more volatile
                if year == 2022:
                    # Transport inflation peaked higher in 2022 due to fuel prices
                    transport_inflation = overall_inflation * 1.5 + (np.random.random() - 0.5) * 1.0
                else:
                    transport_inflation = overall_inflation * 1.1 + (np.random.random() - 0.5) * 0.8
                
                data.append({
                    'country_code': country_codes[i],
                    'country_name': country,
                    'year': year,
                    'overall_inflation': overall_inflation,
                    'food_inflation': food_inflation,
                    'housing_inflation': housing_inflation,
                    'transport_inflation': transport_inflation
                })
        
        return pd.DataFrame(data)

# Load data
economic_data = load_economic_data()
inflation_data = load_inflation_data()

# Merge datasets
combined_data = pd.merge(
    economic_data,
    inflation_data,
    on=['country_code', 'country_name', 'year'],
    how='left'
)

# Sidebar filters
st.sidebar.header("Filters")

# Country filter
countries = sorted(combined_data['country_name'].unique())
selected_countries = st.sidebar.multiselect("Select Countries", countries, default=countries[:5] if len(countries) > 5 else countries)

# Year range filter
years = sorted(combined_data['year'].unique())
selected_years = st.sidebar.slider("Select Year Range", min_value=min(years), max_value=max(years), value=(min(years), max(years)))

# Filter data based on selections
filtered_data = combined_data[
    (combined_data['country_name'].isin(selected_countries)) &
    (combined_data['year'] >= selected_years[0]) &
    (combined_data['year'] <= selected_years[1])
]

# Get the latest data for each country
latest_year = filtered_data['year'].max()
latest_data = filtered_data[filtered_data['year'] == latest_year]

# Display metrics
st.subheader("Key Economic Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    avg_gdp = latest_data['gdp_per_capita'].mean()
    st.metric("Avg. GDP per Capita", f"${avg_gdp:,.2f}")

with col2:
    avg_growth = latest_data['gdp_growth_rate'].mean()
    st.metric("Avg. GDP Growth", f"{avg_growth:.2f}%")

with col3:
    avg_inflation = latest_data['inflation_rate'].mean()
    st.metric("Avg. Inflation Rate", f"{avg_inflation:.2f}%")

with col4:
    avg_food_inflation = latest_data['food_inflation'].mean()
    st.metric("Avg. Food Inflation", f"{avg_food_inflation:.2f}%")

# Visualizations
st.subheader("Economic Indicators Analysis")

# Tab layout for different visualizations
tab1, tab2, tab3 = st.tabs(["GDP vs Inflation", "Economic Trends", "Country Comparison"])

with tab1:
    # Scatter plot of GDP per capita vs. inflation rate
    # Create a copy of the dataframe with adjusted gdp_growth_rate for sizing
    plot_data = filtered_data.copy()
    # Add 10 to ensure all values are positive for marker sizing
    plot_data['size_value'] = plot_data['gdp_growth_rate'] + 10
    
    fig1 = px.scatter(
        plot_data,
        x='gdp_per_capita',
        y='inflation_rate',
        color='country_name',
        size='size_value',  # Use the adjusted positive values
        hover_name='country_name',
        hover_data=['year', 'overall_inflation', 'food_inflation', 'gdp_growth_rate'],
        animation_frame='year',
        title='GDP per Capita vs. Inflation Rate',
        labels={
            'gdp_per_capita': 'GDP per Capita (USD)',
            'inflation_rate': 'Inflation Rate (%)',
            'country_name': 'Country',
            'gdp_growth_rate': 'GDP Growth Rate (%)'
        }
    )
    fig1.update_layout(height=600)
    st.plotly_chart(fig1, use_container_width=True)
    
    # Correlation heatmap
    st.subheader("Correlation Between Economic Indicators")
    
    # Calculate correlation matrix
    corr_columns = ['gdp_per_capita', 'gdp_growth_rate', 'inflation_rate', 
                   'overall_inflation', 'food_inflation', 'housing_inflation', 'transport_inflation']
    corr_data = filtered_data[corr_columns].corr()
    
    fig2 = px.imshow(
        corr_data,
        labels=dict(x="Indicator", y="Indicator", color="Correlation"),
        x=corr_data.columns,
        y=corr_data.index,
        color_continuous_scale='RdBu_r',
        title='Correlation Between Economic Indicators'
    )
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    # Time series of economic indicators
    st.subheader("Economic Trends Over Time")
    
    # Select indicator to visualize
    indicator_options = {
        'gdp_per_capita': 'GDP per Capita (USD)',
        'gdp_growth_rate': 'GDP Growth Rate (%)',
        'inflation_rate': 'World Bank Inflation Rate (%)',
        'overall_inflation': 'Overall HICP Inflation (%)',
        'food_inflation': 'Food Inflation (%)',
        'housing_inflation': 'Housing Inflation (%)',
        'transport_inflation': 'Transport Inflation (%)'
    }
    
    selected_indicator = st.selectbox("Select Economic Indicator", list(indicator_options.keys()), format_func=lambda x: indicator_options[x])
    
    # Create line chart
    fig3 = px.line(
        filtered_data,
        x='year',
        y=selected_indicator,
        color='country_name',
        title=f'{indicator_options[selected_indicator]} Trends by Country',
        labels={'year': 'Year', selected_indicator: indicator_options[selected_indicator], 'country_name': 'Country'}
    )
    st.plotly_chart(fig3, use_container_width=True)
    
    # Create area chart for GDP composition
    if selected_indicator == 'gdp_per_capita':
        # Stacked area chart showing GDP growth over time for all countries
        fig4 = px.area(
            filtered_data,
            x='year',
            y='gdp_per_capita',
            color='country_name',
            title='GDP per Capita Composition by Country',
            labels={'year': 'Year', 'gdp_per_capita': 'GDP per Capita (USD)', 'country_name': 'Country'}
        )
        st.plotly_chart(fig4, use_container_width=True)

with tab3:
    # Country comparison
    st.subheader("Country Economic Comparison")
    
    # Bar chart comparing latest economic indicators across countries
    # Melt the data for easier plotting
    indicators_to_compare = ['gdp_per_capita', 'gdp_growth_rate', 'inflation_rate', 'food_inflation']
    indicator_names = ['GDP per Capita', 'GDP Growth Rate', 'Inflation Rate', 'Food Inflation']
    
    melted_data = pd.melt(
        latest_data,
        id_vars=['country_name'],
        value_vars=indicators_to_compare,
        var_name='indicator',
        value_name='value'
    )
    
    # Map indicator codes to readable names
    indicator_map = dict(zip(indicators_to_compare, indicator_names))
    melted_data['indicator'] = melted_data['indicator'].map(indicator_map)
    
    # Create grouped bar chart
    fig5 = px.bar(
        melted_data,
        x='country_name',
        y='value',
        color='indicator',
        barmode='group',
        title=f'Economic Indicators by Country ({latest_year})',
        labels={'country_name': 'Country', 'value': 'Value', 'indicator': 'Economic Indicator'}
    )
    
    # Adjust y-axis for GDP per capita which has much larger values
    fig5.update_layout(
        yaxis=dict(
            title='Value',
            type='log'  # Use log scale to handle different magnitudes
        )
    )
    st.plotly_chart(fig5, use_container_width=True)
    
    # Radar chart for country comparison
    st.subheader("Country Economic Profile Comparison")
    
    # Prepare data for radar chart
    # Normalize values to 0-1 scale for comparison
    radar_data = latest_data.copy()
    
    for col in indicators_to_compare:
        if col == 'gdp_growth_rate' or col == 'inflation_rate' or col == 'food_inflation':
            # For rates, we want values closer to 0 to be better
            max_val = radar_data[col].abs().max()
            if max_val > 0:
                radar_data[col] = 1 - (radar_data[col].abs() / max_val)
        else:
            # For other indicators like GDP, higher is better
            min_val = radar_data[col].min()
            max_val = radar_data[col].max()
            if max_val > min_val:
                radar_data[col] = (radar_data[col] - min_val) / (max_val - min_val)
    
    # Create radar chart
    fig6 = go.Figure()
    
    for country in radar_data['country_name'].unique():
        country_data = radar_data[radar_data['country_name'] == country]
        
        fig6.add_trace(go.Scatterpolar(
            r=[country_data[col].iloc[0] for col in indicators_to_compare],
            theta=indicator_names,
            fill='toself',
            name=country
        ))
    
    fig6.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1]
            )),
        showlegend=True,
        title='Economic Profile Comparison (Normalized Values)'
    )
    st.plotly_chart(fig6, use_container_width=True)

# Data table
st.subheader("Economic Data")
st.dataframe(
    filtered_data[[
        'country_name', 'year', 'gdp_per_capita', 'gdp_growth_rate',
        'inflation_rate', 'overall_inflation', 'food_inflation'
    ]].sort_values(['country_name', 'year'], ascending=[True, False]),
    use_container_width=True
)

# This try/except is no longer needed since we handle exceptions in the load functions
