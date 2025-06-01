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
st.set_page_config(page_title="Product Price Analysis", page_icon="🛒", layout="wide")

# Title and description
st.title("Product Price Analysis")
st.markdown("""
This dashboard analyzes product-level pricing data from Open Food Facts in relation to inflation rates.
Explore how actual product prices compare to what would be expected based on inflation rates.
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

# Function to load data
@st.cache_data(ttl=3600)
def load_product_price_data():
    try:
        conn = get_snowflake_connection()
        if conn is None:
            raise Exception("Could not establish Snowflake connection")
        query = """
        SELECT 
            p.record_id,
            p.product_id,
            p.product_name,
            p.brand,
            p.country_code,
            p.country_name,
            p.food_category,
            p.price_value,
            p.price_currency,
            p.price_per_standard_unit,
            p.standard_unit,
            p.date_key,
            p.year,
            p.month,
            p.nutrition_grade,
            p.category_inflation_rate,
            p.overall_inflation_rate,
            p.gdp_per_capita,
            p.gdp_growth_rate,
            p.price_deviation_from_inflation
        FROM 
            FACT_PRODUCT_PRICES p
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample product price data for demonstration purposes...")
        
        # Create sample data for demonstration
        # Define countries and food categories
        countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium', 'Austria', 'Portugal']
        country_codes = ['DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT']
        
        food_categories = [
            'Dairy products', 'Cereals and cereal products', 'Fruits', 'Vegetables', 
            'Meat products', 'Fish products', 'Beverages', 'Snacks', 'Bakery products',
            'Confectionery', 'Prepared meals', 'Condiments and sauces'
        ]
        
        # Define brands
        brands = [
            'Nestlé', 'Danone', 'Unilever', 'Kraft Heinz', 'Kellogg\'s', 'Mondelez', 
            'Carrefour', 'Lidl', 'Aldi', 'Tesco', 'Barilla', 'Ferrero', 'Dr. Oetker',
            'Coca-Cola', 'PepsiCo', 'Mars', 'Bonduelle', 'Arla', 'Müller', 'Lavazza'
        ]
        
        # Generate dates from 2020-01 to 2023-12
        dates = pd.date_range(start='2020-01-01', end='2023-12-01', freq='MS')
        years = [d.year for d in dates]
        months = [d.month for d in dates]
        date_keys = [d.strftime('%Y-%m-%d') for d in dates]
        
        # Create sample product data
        np.random.seed(42)  # For reproducibility
        
        # Create product names based on food categories
        product_templates = {
            'Dairy products': ['Milk', 'Yogurt', 'Cheese', 'Butter', 'Cream'],
            'Cereals and cereal products': ['Oats', 'Cornflakes', 'Rice', 'Pasta', 'Quinoa'],
            'Fruits': ['Apples', 'Bananas', 'Oranges', 'Berries', 'Grapes'],
            'Vegetables': ['Carrots', 'Tomatoes', 'Potatoes', 'Broccoli', 'Spinach'],
            'Meat products': ['Chicken', 'Beef', 'Pork', 'Turkey', 'Lamb'],
            'Fish products': ['Salmon', 'Tuna', 'Cod', 'Shrimp', 'Sardines'],
            'Beverages': ['Water', 'Juice', 'Soda', 'Coffee', 'Tea'],
            'Snacks': ['Chips', 'Crackers', 'Nuts', 'Popcorn', 'Pretzels'],
            'Bakery products': ['Bread', 'Rolls', 'Cake', 'Cookies', 'Muffins'],
            'Confectionery': ['Chocolate', 'Candy', 'Gum', 'Licorice', 'Marshmallows'],
            'Prepared meals': ['Pizza', 'Lasagna', 'Soup', 'Salad', 'Sandwich'],
            'Condiments and sauces': ['Ketchup', 'Mustard', 'Mayonnaise', 'Salsa', 'Pesto']
        }
        
        # Standard units by category
        standard_units = {
            'Dairy products': 'kg',
            'Cereals and cereal products': 'kg',
            'Fruits': 'kg',
            'Vegetables': 'kg',
            'Meat products': 'kg',
            'Fish products': 'kg',
            'Beverages': 'l',
            'Snacks': 'kg',
            'Bakery products': 'kg',
            'Confectionery': 'kg',
            'Prepared meals': 'kg',
            'Condiments and sauces': 'kg'
        }
        
        # Base prices by category (per standard unit)
        base_prices = {
            'Dairy products': 3.5,
            'Cereals and cereal products': 2.8,
            'Fruits': 2.5,
            'Vegetables': 2.2,
            'Meat products': 8.5,
            'Fish products': 10.0,
            'Beverages': 1.5,
            'Snacks': 7.0,
            'Bakery products': 4.0,
            'Confectionery': 9.0,
            'Prepared meals': 6.5,
            'Condiments and sauces': 5.0
        }
        
        # Create sample data with realistic price patterns
        data = []
        record_id = 10000
        product_id = 1000
        
        for country_idx, country in enumerate(countries):
            # Country-specific price factor (some countries are more expensive)
            country_factor = 1.0 + (country_idx % 3) * 0.1
            
            for category in food_categories:
                # Get base price for this category
                category_base_price = base_prices[category]
                standard_unit = standard_units[category]
                
                # Create 5 products per category per country
                for product_idx in range(5):
                    product_id += 1
                    product_type = product_templates[category][product_idx]
                    product_name = f"{product_type} {product_idx+1}"
                    brand = brands[np.random.randint(0, len(brands))]
                    
                    # Product-specific price factor
                    product_factor = 0.9 + (product_idx * 0.05) + np.random.random() * 0.2
                    
                    for date_idx, date in enumerate(dates):
                        record_id += 1
                        
                        # Time-based inflation effects
                        time_factor = 1.0
                        if date.year == 2021 and date.month >= 6:
                            # Price increase in mid-2021
                            time_factor = 1.03
                        elif date.year == 2022:
                            # Higher prices in 2022 (inflation peak)
                            time_factor = 1.08
                            # Energy crisis affected food prices
                            if category in ['Dairy products', 'Meat products', 'Prepared meals']:
                                time_factor *= 1.04
                        elif date.year == 2023 and date.month <= 6:
                            # Gradually decreasing in 2023
                            time_factor = 1.06 - (date.month * 0.002)
                        elif date.year == 2023 and date.month > 6:
                            # Further stabilization in late 2023
                            time_factor = 1.05 - ((date.month - 6) * 0.001)
                        
                        # Calculate price with all factors
                        base_price = category_base_price * country_factor * product_factor * time_factor
                        
                        # Add some randomness to price
                        price_value = base_price * (0.98 + np.random.random() * 0.04)
                        price_per_standard_unit = price_value
                        
                        # Calculate inflation rates
                        # Overall inflation rate pattern
                        if date.year == 2020:
                            overall_inflation = 1.5 + (np.random.random() - 0.5)
                        elif date.year == 2021 and date.month < 6:
                            overall_inflation = 2.0 + (np.random.random() - 0.5)
                        elif date.year == 2021 and date.month >= 6:
                            overall_inflation = 3.0 + (np.random.random() - 0.5)
                        elif date.year == 2022 and date.month < 6:
                            overall_inflation = 5.0 + (np.random.random() - 0.5)
                        elif date.year == 2022 and date.month >= 6:
                            overall_inflation = 8.0 + (np.random.random() - 0.5)
                        elif date.year == 2023 and date.month < 6:
                            overall_inflation = 6.5 + (np.random.random() - 0.5)
                        else:
                            overall_inflation = 4.0 + (np.random.random() - 0.5)
                        
                        # Category-specific inflation (some categories inflate more than others)
                        category_factor = 1.0
                        if category in ['Dairy products', 'Meat products']:
                            category_factor = 1.2
                        elif category in ['Fruits', 'Vegetables']:
                            category_factor = 1.3
                        elif category in ['Beverages']:
                            category_factor = 0.8
                        
                        category_inflation = overall_inflation * category_factor
                        
                        # GDP data
                        gdp_per_capita = 30000 + (country_idx * 5000) + (date.year - 2020) * 1000
                        gdp_growth_rate = 1.5 + (date.year - 2020) * 0.5 + (np.random.random() - 0.5)
                        if date.year == 2020:  # COVID impact
                            gdp_growth_rate = -3.0 + (np.random.random() * 2)
                        
                        # Price deviation from inflation
                        expected_price_increase = overall_inflation / 100
                        actual_price_increase = (time_factor - 1)
                        price_deviation = actual_price_increase - expected_price_increase
                        
                        # Nutrition grade (A-E)
                        nutrition_grades = ['A', 'B', 'C', 'D', 'E']
                        nutrition_weights = [0.2, 0.3, 0.3, 0.15, 0.05]  # Most products are B or C
                        nutrition_grade = np.random.choice(nutrition_grades, p=nutrition_weights)
                        
                        data.append({
                            'record_id': record_id,
                            'product_id': product_id,
                            'product_name': f"{brand} {product_name}",
                            'brand': brand,
                            'country_code': country_codes[country_idx],
                            'country_name': country,
                            'food_category': category,
                            'price_value': round(price_value, 2),
                            'price_currency': 'EUR',
                            'price_per_standard_unit': round(price_per_standard_unit, 2),
                            'standard_unit': standard_unit,
                            'date_key': date_keys[date_idx],
                            'year': years[date_idx],
                            'month': months[date_idx],
                            'nutrition_grade': nutrition_grade,
                            'category_inflation_rate': round(category_inflation, 2),
                            'overall_inflation_rate': round(overall_inflation, 2),
                            'gdp_per_capita': gdp_per_capita,
                            'gdp_growth_rate': round(gdp_growth_rate, 2),
                            'price_deviation_from_inflation': round(price_deviation * 100, 2)
                        })
        
        return pd.DataFrame(data)

# Function to load inflation data for comparison
@st.cache_data(ttl=3600)
def load_inflation_data():
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
            i.inflation_rate_yoy
        FROM 
            FACT_INFLATION_RATES i
        WHERE
            i.product_code IN ('CP00', 'CP01')
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample inflation data for comparison...")
        
        # Create sample inflation data that matches the countries in product_price_data
        countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium', 'Austria', 'Portugal']
        country_codes = ['DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT']
        
        # Generate dates from 2020-01 to 2023-12
        dates = pd.date_range(start='2020-01-01', end='2023-12-01', freq='MS')
        years = [d.year for d in dates]
        months = [d.month for d in dates]
        date_keys = [d.strftime('%Y-%m-%d') for d in dates]
        
        # Product codes and names for inflation data
        products = [
            {'code': 'CP00', 'name': 'All Items'},
            {'code': 'CP01', 'name': 'Food and Non-Alcoholic Beverages'}
        ]
        
        # Create sample data with realistic inflation patterns
        data = []
        for i, country in enumerate(countries):
            for product in products:
                for j, date in enumerate(dates):
                    # Create realistic inflation patterns with some randomness and trends
                    base_inflation = 2.0  # Starting inflation rate
                    
                    # Food inflation is typically higher than overall inflation
                    product_factor = 1.0 if product['code'] == 'CP00' else 1.3
                    
                    # Create realistic inflation patterns with some randomness and trends
                    if date.year == 2021 and date.month >= 6:
                        # Inflation spike in mid-2021
                        base_inflation = 3.5 * product_factor + (i % 3) * 0.3
                    elif date.year == 2022:
                        # Higher inflation in 2022
                        base_inflation = 5.0 * product_factor + (i % 4) * 0.4
                        # Food prices also spiked
                        if product['code'] == 'CP01':
                            base_inflation *= 1.2
                    elif date.year == 2023 and date.month <= 6:
                        # Gradually decreasing in 2023
                        base_inflation = 4.0 * product_factor - (date.month * 0.1) + (i % 3) * 0.2
                    elif date.year == 2023 and date.month > 6:
                        # Further decrease in late 2023
                        base_inflation = 2.5 * product_factor - ((date.month - 6) * 0.1) + (i % 2) * 0.2
                    
                    # Add some randomness
                    inflation_rate_yoy = base_inflation + (np.random.random() - 0.5) * 0.8
                    
                    data.append({
                        'country_code': country_codes[i],
                        'country_name': country,
                        'product_code': product['code'],
                        'product_name': product['name'],
                        'date_key': date_keys[j],
                        'year': years[j],
                        'month': months[j],
                        'inflation_rate_yoy': round(inflation_rate_yoy, 2)
                    })
        
        return pd.DataFrame(data)

# Load data
product_data = load_product_price_data()
inflation_data = load_inflation_data()

# Sidebar filters
st.sidebar.header("Filters")

# Country filter
countries = sorted(product_data['country_name'].unique())
selected_countries = st.sidebar.multiselect("Select Countries", countries, default=countries[:3] if len(countries) > 3 else countries, key='product_prices_countries')

# Food category filter
categories = sorted(product_data['food_category'].unique())
selected_categories = st.sidebar.multiselect("Select Food Categories", categories, default=categories[:3] if len(categories) > 3 else categories, key='product_prices_categories')

# Brand filter (optional)
brands = sorted(product_data['brand'].dropna().unique())
selected_brands = st.sidebar.multiselect("Select Brands (Optional)", brands, key='product_prices_brands')

# Date range filter
# Convert string dates to datetime objects if they're not already
if isinstance(product_data['date_key'].iloc[0], str):
    product_data['date_key'] = pd.to_datetime(product_data['date_key'])
    
min_date = product_data['date_key'].min().date()
max_date = product_data['date_key'].max().date()
date_range = st.sidebar.slider("Select Date Range", min_value=min_date, max_value=max_date, value=(min_date, max_date), key='product_prices_date_range')

# Filter data based on selections
# Convert date_range from date to datetime for filtering
start_date = pd.Timestamp(date_range[0])
end_date = pd.Timestamp(date_range[1])

filtered_data = product_data[
    (product_data['country_name'].isin(selected_countries)) &
    (product_data['food_category'].isin(selected_categories)) &
    (product_data['date_key'] >= start_date) &
    (product_data['date_key'] <= end_date)
]

if selected_brands:
    filtered_data = filtered_data[filtered_data['brand'].isin(selected_brands)]

# Filter inflation data
# Convert inflation data date_key to datetime if it's not already
if isinstance(inflation_data['date_key'].iloc[0], str):
    inflation_data['date_key'] = pd.to_datetime(inflation_data['date_key'])

filtered_inflation = inflation_data[
    (inflation_data['country_name'].isin(selected_countries)) &
    (inflation_data['date_key'] >= start_date) &
    (inflation_data['date_key'] <= end_date)
]

# Display metrics
st.subheader("Key Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    avg_price = filtered_data['price_per_standard_unit'].mean()
    st.metric("Avg. Price per Unit", f"{avg_price:.2f}")

with col2:
    avg_inflation = filtered_data['category_inflation_rate'].mean()
    st.metric("Avg. Category Inflation", f"{avg_inflation:.2f}%")

with col3:
    avg_overall_inflation = filtered_data['overall_inflation_rate'].mean()
    st.metric("Avg. Overall Inflation", f"{avg_overall_inflation:.2f}%")

with col4:
    avg_deviation = filtered_data['price_deviation_from_inflation'].mean()
    st.metric("Avg. Price Deviation", f"{avg_deviation:.2f}")

# Visualizations
st.subheader("Price vs. Inflation Analysis")

# Tab layout for different visualizations
tab1, tab2, tab3 = st.tabs(["Price by Category", "Price vs. Inflation", "Price Trends"])

with tab1:
    # Average price by food category
    category_price_data = filtered_data.groupby('food_category')['price_per_standard_unit'].mean().reset_index()
    category_price_data = category_price_data.sort_values('price_per_standard_unit', ascending=False)
    
    fig1 = px.bar(
        category_price_data,
        x='food_category',
        y='price_per_standard_unit',
        title='Average Price per Standard Unit by Food Category',
        labels={'food_category': 'Food Category', 'price_per_standard_unit': 'Avg. Price per Unit'},
        color='food_category'
    )
    st.plotly_chart(fig1, use_container_width=True, key='price_category_bar')
    
with tab2:
    # Scatter plot of price vs. inflation rate
    fig2 = px.scatter(
        filtered_data,
        x='category_inflation_rate',
        y='price_per_standard_unit',
        color='food_category',
        size='price_value',
        hover_name='product_name',
        hover_data=['brand', 'country_name', 'date_key'],
        title='Product Price vs. Category Inflation Rate',
        labels={
            'category_inflation_rate': 'Category Inflation Rate (%)',
            'price_per_standard_unit': 'Price per Standard Unit',
            'food_category': 'Food Category'
        }
    )
    st.plotly_chart(fig2, use_container_width=True, key='price_inflation_scatter')
    
with tab3:
    # Time series of prices and inflation
    # Aggregate by month and country
    time_data = filtered_data.groupby(['year', 'month', 'country_name'])[
        ['price_per_standard_unit', 'category_inflation_rate', 'overall_inflation_rate']
    ].mean().reset_index()
    
    # Create date column for plotting
    time_data['date'] = pd.to_datetime(time_data['year'].astype(str) + '-' + time_data['month'].astype(str))
    
    # Create time series plot
    fig3 = go.Figure()
    
    for country in time_data['country_name'].unique():
        country_data = time_data[time_data['country_name'] == country]
        
        fig3.add_trace(go.Scatter(
            x=country_data['date'],
            y=country_data['price_per_standard_unit'],
            mode='lines+markers',
            name=f'{country} - Price',
            line=dict(dash='solid')
        ))
        
        fig3.add_trace(go.Scatter(
            x=country_data['date'],
            y=country_data['overall_inflation_rate'],
            mode='lines',
            name=f'{country} - Inflation',
            line=dict(dash='dash')
        ))
    
    fig3.update_layout(
        title='Average Price and Inflation Rate Over Time by Country',
        xaxis_title='Date',
        yaxis_title='Value',
        legend_title='Metric'
    )
    
    st.plotly_chart(fig3, use_container_width=True, key='price_time_series')

# Data table
st.subheader("Product Data")
st.dataframe(
    filtered_data[[
        'product_name', 'brand', 'country_name', 'food_category',
        'price_value', 'price_currency', 'price_per_standard_unit', 'standard_unit',
        'category_inflation_rate', 'overall_inflation_rate', 'date_key'
    ]].sort_values('price_per_standard_unit', ascending=False),
    use_container_width=True,
    key='product_data_table'
)

# Exception handling is now done inside the load functions
