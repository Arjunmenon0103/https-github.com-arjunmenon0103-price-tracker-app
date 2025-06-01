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

# Flag to force sample data (useful for deployment without Snowflake credentials)
USE_SAMPLE_DATA = True  # Set to True for Streamlit Community Cloud deployment

# Snowflake connection parameters
snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
snowflake_user = os.getenv('SNOWFLAKE_USER')
snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
snowflake_database = os.getenv('SNOWFLAKE_DATABASE', 'DATAEXPERT_STUDENT')
snowflake_schema = os.getenv('STUDENT_SCHEMA')
snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')

# Page configuration
st.set_page_config(page_title="Price Tracker Inflation Monitor", page_icon="📈", layout="wide")

# Title and description
st.title("Price Tracker Inflation Monitor")
st.markdown("""
This dashboard provides insights into inflation trends across EU countries, combining data from:
- Eurostat HICP (Harmonised Index of Consumer Prices) data for official inflation rates
- World Bank economic indicators for context
- Open Food Facts product pricing data for actual price trends

Use the sidebar to navigate to different analysis pages.
""")

# Connect to Snowflake
@st.cache_data(ttl=3600)
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=snowflake_account,
        user=snowflake_user,
        password=snowflake_password,
        database=snowflake_database,
        schema=snowflake_schema,
        warehouse=snowflake_warehouse
    )

# Function to load data
@st.cache_data(ttl=3600)
def load_inflation_data():
    if USE_SAMPLE_DATA:
        st.info("Using sample data for demonstration purposes...")
        # Skip Snowflake connection attempt
        return generate_sample_data()
    
    try:
        conn = get_snowflake_connection()
        query = """
        SELECT 
            i.country_code,
            i.country_name,
            i.product_code,
            i.product_name,
            i.date_key,
            i.year,
            i.month,
            i.inflation_rate_yoy,
            i.inflation_rate_mom,
            i.price_index,
            i.gdp_per_capita
        FROM 
            FACT_INFLATION_RATES i
        WHERE
            i.product_code = 'CP00' -- Overall inflation
        ORDER BY
            i.country_name, i.date_key
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample data for demonstration purposes...")
        return generate_sample_data()
        
def generate_sample_data():
        # Create sample data for demonstration
        countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium', 'Austria', 'Portugal']
        country_codes = ['DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT']
        
        # Generate dates from 2020-01 to 2023-12
        dates = pd.date_range(start='2020-01-01', end='2023-12-01', freq='MS')
        years = [d.year for d in dates]
        months = [d.month for d in dates]
        date_keys = [d.strftime('%Y-%m-%d') for d in dates]
        
        # Create sample data with realistic inflation patterns
        data = []
        for i, country in enumerate(countries):
            base_inflation = 2.0  # Starting inflation rate
            for j, date in enumerate(dates):
                # Create realistic inflation patterns with some randomness and trends
                if date.year == 2021 and date.month >= 6:
                    # Inflation spike in mid-2021
                    base_inflation = 3.5 + (i % 3)
                elif date.year == 2022:
                    # Higher inflation in 2022
                    base_inflation = 5.0 + (i % 4)
                elif date.year == 2023 and date.month <= 6:
                    # Gradually decreasing in 2023
                    base_inflation = 4.0 - (date.month * 0.2) + (i % 3)
                elif date.year == 2023 and date.month > 6:
                    # Further decrease in late 2023
                    base_inflation = 2.5 - ((date.month - 6) * 0.1) + (i % 2)
                
                # Add some randomness
                inflation_rate_yoy = base_inflation + (np.random.random() - 0.5)
                inflation_rate_mom = inflation_rate_yoy / 12 + (np.random.random() - 0.5) * 0.2
                
                # Calculate price index (base 100 in 2020-01)
                price_index = 100 + (j * inflation_rate_mom / 10)
                
                # GDP per capita varies by country
                gdp_per_capita = 30000 + (i * 5000) + (date.year - 2020) * 1000
                
                data.append({
                    'country_code': country_codes[i],
                    'country_name': country,
                    'product_code': 'CP00',
                    'product_name': 'All Items',
                    'date_key': date_keys[j],
                    'year': years[j],
                    'month': months[j],
                    'inflation_rate_yoy': inflation_rate_yoy,
                    'inflation_rate_mom': inflation_rate_mom,
                    'price_index': price_index,
                    'gdp_per_capita': gdp_per_capita
                })
        
        return pd.DataFrame(data)

# Load data
inflation_data = load_inflation_data()

# Continue with the dashboard
# Sidebar filters
st.sidebar.header("Filters")

# Country filter
countries = sorted(inflation_data['country_name'].unique())
selected_countries = st.sidebar.multiselect("Select Countries", countries, default=countries[:5] if len(countries) > 5 else countries)

# Date range filter
years = sorted(inflation_data['year'].unique())
selected_years = st.sidebar.slider("Select Year Range", min_value=min(years), max_value=max(years), value=(min(years), max(years)))

# Filter data based on selections
filtered_data = inflation_data[
    (inflation_data['country_name'].isin(selected_countries)) &
    (inflation_data['year'] >= selected_years[0]) &
    (inflation_data['year'] <= selected_years[1])
]
    
# Display metrics
st.subheader("Key Inflation Metrics")
col1, col2, col3 = st.columns(3)

with col1:
    latest_year = filtered_data['year'].max()
    latest_inflation = filtered_data[filtered_data['year'] == latest_year]['inflation_rate_yoy'].mean()
    st.metric("Latest Average Inflation", f"{latest_inflation:.2f}%")

with col2:
    max_inflation = filtered_data['inflation_rate_yoy'].max()
    max_country = filtered_data.loc[filtered_data['inflation_rate_yoy'].idxmax(), 'country_name']
    max_date = filtered_data.loc[filtered_data['inflation_rate_yoy'].idxmax(), 'date_key']
    st.metric("Highest Inflation", f"{max_inflation:.2f}%", f"{max_country} ({max_date})")

with col3:
    min_inflation = filtered_data['inflation_rate_yoy'].min()
    min_country = filtered_data.loc[filtered_data['inflation_rate_yoy'].idxmin(), 'country_name']
    min_date = filtered_data.loc[filtered_data['inflation_rate_yoy'].idxmin(), 'date_key']
    st.metric("Lowest Inflation", f"{min_inflation:.2f}%", f"{min_country} ({min_date})")

# Visualizations
st.subheader("Inflation Trends")
    
# Line chart of inflation rates over time by country
fig1 = px.line(
    filtered_data,
    x='date_key',
    y='inflation_rate_yoy',
    color='country_name',
    title='Inflation Rate Trends by Country',
    labels={'date_key': 'Date', 'inflation_rate_yoy': 'Inflation Rate (%)', 'country_name': 'Country'}
)
st.plotly_chart(fig1, use_container_width=True)

# Bar chart of latest inflation rates by country
latest_data = filtered_data.sort_values('date_key').groupby('country_name').last().reset_index()
latest_data = latest_data.sort_values('inflation_rate_yoy', ascending=False)
    
fig2 = px.bar(
    latest_data,
    x='country_name',
    y='inflation_rate_yoy',
    title=f'Latest Inflation Rates by Country ({latest_data["date_key"].iloc[0]})',
    labels={'country_name': 'Country', 'inflation_rate_yoy': 'Inflation Rate (%)'},
    color='inflation_rate_yoy',
    color_continuous_scale='RdYlGn_r'
)
st.plotly_chart(fig2, use_container_width=True)

# Scatter plot of inflation vs GDP per capita
fig3 = px.scatter(
    latest_data,
    x='gdp_per_capita',
    y='inflation_rate_yoy',
    color='country_name',
    size='price_index',
    hover_name='country_name',
    title='Inflation Rate vs. GDP per Capita',
    labels={'gdp_per_capita': 'GDP per Capita (USD)', 'inflation_rate_yoy': 'Inflation Rate (%)', 'country_name': 'Country'}
)
st.plotly_chart(fig3, use_container_width=True)

# Data table
st.subheader("Inflation Data")
st.dataframe(
    filtered_data[['country_name', 'date_key', 'inflation_rate_yoy', 'inflation_rate_mom', 'price_index', 'gdp_per_capita']],
    use_container_width=True
)

# Information about other pages
st.subheader("Explore More Data")
st.markdown("""
Navigate to other pages using the sidebar to explore:
- **Product Categories**: Detailed inflation breakdown by COICOP product categories
- **Product Prices**: Actual product pricing data from Open Food Facts compared to inflation rates
- **Economic Indicators**: Additional economic context from World Bank data
""")

# This try/except is no longer needed since we handle exceptions in the load_inflation_data function
