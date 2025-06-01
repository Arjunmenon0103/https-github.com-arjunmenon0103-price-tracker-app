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
st.set_page_config(page_title="Product Category Analysis", page_icon="ud83dudcca", layout="wide")

# Title and description
st.title("Product Category Inflation Analysis")
st.markdown("""
This dashboard analyzes inflation rates across different COICOP product categories and subcategories.
Explore how inflation varies across different types of products and services.
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
def load_inflation_data():
    try:
        conn = get_snowflake_connection()
        if conn is None:
            raise Exception("Could not establish Snowflake connection")
            
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
            i.product_level,
            i.parent_product_code
        FROM 
            FACT_INFLATION_RATES i
        ORDER BY
            i.country_name, i.product_code, i.date_key
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample data for demonstration purposes...")
        
        # Create sample data for demonstration
        countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium', 'Austria', 'Portugal']
        country_codes = ['DE', 'FR', 'ES', 'IT', 'NL', 'BE', 'AT', 'PT']
        
        # COICOP product categories
        product_categories = [
            # Level 1 - Main categories
            {'code': 'CP00', 'name': 'All Items', 'level': 1, 'parent': None},
            {'code': 'CP01', 'name': 'Food and Non-Alcoholic Beverages', 'level': 1, 'parent': None},
            {'code': 'CP02', 'name': 'Alcoholic Beverages and Tobacco', 'level': 1, 'parent': None},
            {'code': 'CP03', 'name': 'Clothing and Footwear', 'level': 1, 'parent': None},
            {'code': 'CP04', 'name': 'Housing, Water, Electricity, Gas and Other Fuels', 'level': 1, 'parent': None},
            {'code': 'CP05', 'name': 'Furnishings, Household Equipment and Routine Household Maintenance', 'level': 1, 'parent': None},
            {'code': 'CP06', 'name': 'Health', 'level': 1, 'parent': None},
            {'code': 'CP07', 'name': 'Transport', 'level': 1, 'parent': None},
            {'code': 'CP08', 'name': 'Communications', 'level': 1, 'parent': None},
            {'code': 'CP09', 'name': 'Recreation and Culture', 'level': 1, 'parent': None},
            {'code': 'CP10', 'name': 'Education', 'level': 1, 'parent': None},
            {'code': 'CP11', 'name': 'Restaurants and Hotels', 'level': 1, 'parent': None},
            {'code': 'CP12', 'name': 'Miscellaneous Goods and Services', 'level': 1, 'parent': None},
            
            # Level 2 - Subcategories for Food (CP01)
            {'code': 'CP011', 'name': 'Food', 'level': 2, 'parent': 'CP01'},
            {'code': 'CP012', 'name': 'Non-Alcoholic Beverages', 'level': 2, 'parent': 'CP01'},
            
            # Level 3 - Detailed food categories
            {'code': 'CP0111', 'name': 'Bread and Cereals', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0112', 'name': 'Meat', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0113', 'name': 'Fish', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0114', 'name': 'Milk, Cheese and Eggs', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0115', 'name': 'Oils and Fats', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0116', 'name': 'Fruit', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0117', 'name': 'Vegetables', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0118', 'name': 'Sugar, Jam, Honey, Chocolate and Confectionery', 'level': 3, 'parent': 'CP011'},
            {'code': 'CP0119', 'name': 'Food Products n.e.c.', 'level': 3, 'parent': 'CP011'},
            
            # Level 2 - Subcategories for Housing (CP04)
            {'code': 'CP041', 'name': 'Actual Rentals for Housing', 'level': 2, 'parent': 'CP04'},
            {'code': 'CP042', 'name': 'Imputed Rentals for Housing', 'level': 2, 'parent': 'CP04'},
            {'code': 'CP043', 'name': 'Maintenance and Repair of the Dwelling', 'level': 2, 'parent': 'CP04'},
            {'code': 'CP044', 'name': 'Water Supply and Miscellaneous Services', 'level': 2, 'parent': 'CP04'},
            {'code': 'CP045', 'name': 'Electricity, Gas and Other Fuels', 'level': 2, 'parent': 'CP04'},
            
            # Level 2 - Subcategories for Transport (CP07)
            {'code': 'CP071', 'name': 'Purchase of Vehicles', 'level': 2, 'parent': 'CP07'},
            {'code': 'CP072', 'name': 'Operation of Personal Transport Equipment', 'level': 2, 'parent': 'CP07'},
            {'code': 'CP073', 'name': 'Transport Services', 'level': 2, 'parent': 'CP07'}
        ]
        
        # Generate dates from 2020-01 to 2023-12
        dates = pd.date_range(start='2020-01-01', end='2023-12-01', freq='MS')
        years = [d.year for d in dates]
        months = [d.month for d in dates]
        date_keys = [d.strftime('%Y-%m-%d') for d in dates]
        
        # Create sample data with realistic inflation patterns
        data = []
        for i, country in enumerate(countries):
            for product in product_categories:
                base_inflation = 2.0  # Starting inflation rate
                
                # Different product categories have different inflation patterns
                category_factor = 1.0
                if product['code'] == 'CP01' or product['code'].startswith('CP01'):  # Food
                    category_factor = 1.3  # Food inflation typically higher
                elif product['code'] == 'CP04' or product['code'].startswith('CP04'):  # Housing
                    category_factor = 1.1
                elif product['code'] == 'CP07' or product['code'].startswith('CP07'):  # Transport
                    category_factor = 1.4  # Transport more volatile
                elif product['code'] == 'CP06':  # Health
                    category_factor = 0.7  # Health typically lower
                elif product['code'] == 'CP08':  # Communications
                    category_factor = 0.5  # Communications often deflationary
                
                for j, date in enumerate(dates):
                    # Create realistic inflation patterns with some randomness and trends
                    if date.year == 2021 and date.month >= 6:
                        # Inflation spike in mid-2021
                        base_inflation = 3.5 * category_factor + (i % 3)
                    elif date.year == 2022:
                        # Higher inflation in 2022
                        base_inflation = 5.0 * category_factor + (i % 4)
                        # Energy crisis affected housing costs more
                        if product['code'] == 'CP045':
                            base_inflation *= 2.0
                        # Food prices also spiked
                        elif product['code'].startswith('CP011'):
                            base_inflation *= 1.5
                    elif date.year == 2023 and date.month <= 6:
                        # Gradually decreasing in 2023
                        base_inflation = 4.0 * category_factor - (date.month * 0.1) + (i % 3)
                    elif date.year == 2023 and date.month > 6:
                        # Further decrease in late 2023
                        base_inflation = 2.5 * category_factor - ((date.month - 6) * 0.1) + (i % 2)
                    
                    # Add some randomness
                    inflation_rate_yoy = base_inflation + (np.random.random() - 0.5)
                    inflation_rate_mom = inflation_rate_yoy / 12 + (np.random.random() - 0.5) * 0.2
                    
                    # Calculate price index (base 100 in 2020-01)
                    price_index = 100 + (j * inflation_rate_mom / 10)
                    
                    data.append({
                        'country_code': country_codes[i],
                        'country_name': country,
                        'product_code': product['code'],
                        'product_name': product['name'],
                        'date_key': date_keys[j],
                        'year': years[j],
                        'month': months[j],
                        'inflation_rate_yoy': inflation_rate_yoy,
                        'inflation_rate_mom': inflation_rate_mom,
                        'price_index': price_index,
                        'product_level': product['level'],
                        'parent_product_code': product['parent']
                    })
        
        return pd.DataFrame(data)

# Function to load product hierarchy
@st.cache_data(ttl=3600)
def load_product_hierarchy():
    try:
        conn = get_snowflake_connection()
        query = """
        SELECT 
            p.product_code,
            p.product_name,
            p.category_description,
            p.main_category
        FROM 
            DIM_PRODUCTS p
        ORDER BY
            p.product_code
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample product hierarchy data for demonstration purposes...")
        
        # Create sample product hierarchy data that matches the COICOP categories in load_inflation_data
        product_hierarchy = [
            # Level 1 - Main categories
            {'product_code': 'CP00', 'product_name': 'All Items', 'category_description': 'Overall HICP', 'main_category': 'Overall'},
            {'product_code': 'CP01', 'product_name': 'Food and Non-Alcoholic Beverages', 'category_description': 'Food and beverages consumed at home', 'main_category': 'Food'},
            {'product_code': 'CP02', 'product_name': 'Alcoholic Beverages and Tobacco', 'category_description': 'Alcoholic drinks and tobacco products', 'main_category': 'Food'},
            {'product_code': 'CP03', 'product_name': 'Clothing and Footwear', 'category_description': 'Garments, footwear and related services', 'main_category': 'Clothing'},
            {'product_code': 'CP04', 'product_name': 'Housing, Water, Electricity, Gas and Other Fuels', 'category_description': 'Housing costs and utilities', 'main_category': 'Housing'},
            {'product_code': 'CP05', 'product_name': 'Furnishings, Household Equipment and Routine Household Maintenance', 'category_description': 'Furniture, appliances and services', 'main_category': 'Housing'},
            {'product_code': 'CP06', 'product_name': 'Health', 'category_description': 'Medical products and services', 'main_category': 'Health'},
            {'product_code': 'CP07', 'product_name': 'Transport', 'category_description': 'Vehicles, fuel and transport services', 'main_category': 'Transport'},
            {'product_code': 'CP08', 'product_name': 'Communications', 'category_description': 'Postal and telecommunication services', 'main_category': 'Communications'},
            {'product_code': 'CP09', 'product_name': 'Recreation and Culture', 'category_description': 'Recreational goods, services and cultural activities', 'main_category': 'Recreation'},
            {'product_code': 'CP10', 'product_name': 'Education', 'category_description': 'Educational services', 'main_category': 'Education'},
            {'product_code': 'CP11', 'product_name': 'Restaurants and Hotels', 'category_description': 'Catering and accommodation services', 'main_category': 'Food'},
            {'product_code': 'CP12', 'product_name': 'Miscellaneous Goods and Services', 'category_description': 'Personal care, insurance and financial services', 'main_category': 'Other'},
            
            # Level 2 - Subcategories for Food (CP01)
            {'product_code': 'CP011', 'product_name': 'Food', 'category_description': 'Food products', 'main_category': 'Food'},
            {'product_code': 'CP012', 'product_name': 'Non-Alcoholic Beverages', 'category_description': 'Coffee, tea, water, juices', 'main_category': 'Food'},
            
            # Level 3 - Detailed food categories
            {'product_code': 'CP0111', 'product_name': 'Bread and Cereals', 'category_description': 'Bread, rice, pasta, cereals', 'main_category': 'Food'},
            {'product_code': 'CP0112', 'product_name': 'Meat', 'category_description': 'All types of meat products', 'main_category': 'Food'},
            {'product_code': 'CP0113', 'product_name': 'Fish', 'category_description': 'Fresh, chilled, frozen fish and seafood', 'main_category': 'Food'},
            {'product_code': 'CP0114', 'product_name': 'Milk, Cheese and Eggs', 'category_description': 'Dairy products and eggs', 'main_category': 'Food'},
            {'product_code': 'CP0115', 'product_name': 'Oils and Fats', 'category_description': 'Butter, margarine, oils', 'main_category': 'Food'},
            {'product_code': 'CP0116', 'product_name': 'Fruit', 'category_description': 'Fresh and preserved fruits', 'main_category': 'Food'},
            {'product_code': 'CP0117', 'product_name': 'Vegetables', 'category_description': 'Fresh and preserved vegetables', 'main_category': 'Food'},
            {'product_code': 'CP0118', 'product_name': 'Sugar, Jam, Honey, Chocolate and Confectionery', 'category_description': 'Sweet products', 'main_category': 'Food'},
            {'product_code': 'CP0119', 'product_name': 'Food Products n.e.c.', 'category_description': 'Other food products', 'main_category': 'Food'},
            
            # Level 2 - Subcategories for Housing (CP04)
            {'product_code': 'CP041', 'product_name': 'Actual Rentals for Housing', 'category_description': 'Rent payments', 'main_category': 'Housing'},
            {'product_code': 'CP042', 'product_name': 'Imputed Rentals for Housing', 'category_description': 'Imputed rent for owner-occupiers', 'main_category': 'Housing'},
            {'product_code': 'CP043', 'product_name': 'Maintenance and Repair of the Dwelling', 'category_description': 'Materials and services for maintenance', 'main_category': 'Housing'},
            {'product_code': 'CP044', 'product_name': 'Water Supply and Miscellaneous Services', 'category_description': 'Water, sewerage, waste collection', 'main_category': 'Housing'},
            {'product_code': 'CP045', 'product_name': 'Electricity, Gas and Other Fuels', 'category_description': 'Energy for the home', 'main_category': 'Housing'},
            
            # Level 2 - Subcategories for Transport (CP07)
            {'product_code': 'CP071', 'product_name': 'Purchase of Vehicles', 'category_description': 'Cars, motorcycles, bicycles', 'main_category': 'Transport'},
            {'product_code': 'CP072', 'product_name': 'Operation of Personal Transport Equipment', 'category_description': 'Fuel, maintenance, parts', 'main_category': 'Transport'},
            {'product_code': 'CP073', 'product_name': 'Transport Services', 'category_description': 'Rail, road, air transport', 'main_category': 'Transport'}
        ]
        
        return pd.DataFrame(product_hierarchy)

# Load data
inflation_data = load_inflation_data()
product_hierarchy = load_product_hierarchy()

# Sidebar filters
st.sidebar.header("Filters")
# Country filter
countries = sorted(inflation_data['country_name'].unique())
selected_countries = st.sidebar.multiselect("Select Countries", default=countries[:3] if len(countries) > 3 else countries, options=countries, key='sidebar_countries_select')

# Product level filter
product_levels = sorted(inflation_data['product_level'].unique())
selected_levels = st.sidebar.multiselect("Select Product Hierarchy Level", default=product_levels, options=product_levels, format_func=lambda x: f"Level {x}: {'Main Categories' if x == 1 else 'Subcategories' if x == 2 else 'Detailed Categories'}", key='sidebar_levels_select')

# Date range filter
years = sorted(inflation_data['year'].unique())
selected_years = st.sidebar.slider("Select Year Range", min_value=min(years), max_value=max(years), value=(min(years), max(years)))

# Filter data based on selections
filtered_data = inflation_data[
    (inflation_data['country_name'].isin(selected_countries)) &
    (inflation_data['product_level'].isin(selected_levels)) &
    (inflation_data['year'] >= selected_years[0]) &
    (inflation_data['year'] <= selected_years[1])
]

# Get the latest data for each country and product
latest_date = filtered_data['date_key'].max()
latest_data = filtered_data[filtered_data['date_key'] == latest_date]

# Display metrics
st.subheader("Latest Inflation Metrics by Product Category")

# Create tabs for different visualizations
tab1, tab2, tab3 = st.tabs(["Category Comparison", "Hierarchy Analysis", "Time Trends"])

with tab1:
    # Heatmap of latest inflation rates by country and main product categories
    pivot_data = latest_data.pivot_table(
        values='inflation_rate_yoy',
        index='country_name',
        columns='product_name',
        aggfunc='mean'
    )
    
    fig1 = px.imshow(
        pivot_data,
        labels=dict(x="Product Category", y="Country", color="Inflation Rate (%)"),
        x=pivot_data.columns,
        y=pivot_data.index,
        color_continuous_scale='RdYlGn_r',
        title='Latest Inflation Rates by Country and Product Category'
    )
    fig1.update_layout(height=600)
    st.plotly_chart(fig1, use_container_width=True, key='tab1_heatmap_country_product')
    
    # Bar chart of average inflation by product category
    category_avg = latest_data.groupby('product_name')['inflation_rate_yoy'].mean().reset_index()
    category_avg = category_avg.sort_values('inflation_rate_yoy', ascending=False)
    
    fig2 = px.bar(
        category_avg,
        x='product_name',
        y='inflation_rate_yoy',
        title='Average Inflation Rate by Product Category',
        labels={'product_name': 'Product Category', 'inflation_rate_yoy': 'Avg. Inflation Rate (%)'},
        color='inflation_rate_yoy',
        color_continuous_scale='RdYlGn_r'
    )
    st.plotly_chart(fig2, use_container_width=True, key='tab1_bar_product_inflation')

with tab2:
    # Hierarchical analysis
    st.subheader("Product Category Hierarchy")
    
    # Allow selection of a main category to explore
    main_categories = product_hierarchy[product_hierarchy['product_code'].str.len() == 4]['product_name'].unique()
    selected_main_category = st.selectbox("Select Main Category to Explore", main_categories, key='tab2_main_category_select')
    
    # Get the product code for the selected main category
    main_category_code = product_hierarchy[
        (product_hierarchy['product_name'] == selected_main_category) & 
        (product_hierarchy['product_code'].str.len() == 4)
    ]['product_code'].iloc[0]
    
    # Get all subcategories for this main category
    subcategory_codes = product_hierarchy[
        product_hierarchy['product_code'].str.startswith(main_category_code[:2]) & 
        (product_hierarchy['product_code'].str.len() > 4)
    ]['product_code'].tolist()
    
    # Filter data for the selected main category and its subcategories
    hierarchy_data = filtered_data[
        ((filtered_data['product_code'] == main_category_code) | 
        (filtered_data['product_code'].isin(subcategory_codes))) &
        (filtered_data['year'] >= selected_years[0]) &
        (filtered_data['year'] <= selected_years[1])
    ]
    
    # Get the latest data for each country and product
    latest_date = filtered_data['date_key'].max()
    latest_data = filtered_data[filtered_data['date_key'] == latest_date]
    
    # Display metrics
    st.subheader("Latest Inflation Metrics by Product Category")
    
    # Create tabs for different visualizations
    tab1, tab2, tab3 = st.tabs(["Category Comparison", "Hierarchy Analysis", "Time Trends"])
    
    with tab1:
        # Heatmap of latest inflation rates by country and main product categories
        pivot_data = latest_data.pivot_table(
            values='inflation_rate_yoy',
            index='country_name',
            columns='product_name',
            aggfunc='mean'
        )
        
        fig1 = px.imshow(
            pivot_data,
            labels=dict(x="Product Category", y="Country", color="Inflation Rate (%)"),
            x=pivot_data.columns,
            y=pivot_data.index,
            color_continuous_scale='RdYlGn_r',
            title='Latest Inflation Rates by Country and Product Category'
        )
        fig1.update_layout(height=600)
        st.plotly_chart(fig1, use_container_width=True, key='tab2_heatmap_country_product')
        
        # Bar chart of average inflation by product category
        category_avg = latest_data.groupby('product_name')['inflation_rate_yoy'].mean().reset_index()
        category_avg = category_avg.sort_values('inflation_rate_yoy', ascending=False)
        
        fig2 = px.bar(
            category_avg,
            x='product_name',
            y='inflation_rate_yoy',
            title='Average Inflation Rate by Product Category',
            labels={'product_name': 'Product Category', 'inflation_rate_yoy': 'Avg. Inflation Rate (%)'},
            color='inflation_rate_yoy',
            color_continuous_scale='RdYlGn_r'
        )
        st.plotly_chart(fig2, use_container_width=True, key='tab2_bar_product_inflation')
    
    with tab2:
        # Hierarchical analysis
        st.subheader("Product Category Hierarchy")
        
        # Allow selection of a main category to explore
        main_categories = product_hierarchy[product_hierarchy['product_code'].str.len() == 4]['product_name'].unique()
        selected_main_category = st.selectbox("Select Main Category to Explore", main_categories, key='tab3_main_category_select')
        
        # Get the product code for the selected main category
        main_category_code = product_hierarchy[
            (product_hierarchy['product_name'] == selected_main_category) & 
            (product_hierarchy['product_code'].str.len() == 4)
        ]['product_code'].iloc[0]
        
        # Get all subcategories for this main category
        subcategory_codes = product_hierarchy[
            product_hierarchy['product_code'].str.startswith(main_category_code[:2]) & 
            (product_hierarchy['product_code'].str.len() > 4)
        ]['product_code'].tolist()
        
        # Filter data for the selected main category and its subcategories
        hierarchy_data = filtered_data[
            ((filtered_data['product_code'] == main_category_code) | 
            (filtered_data['product_code'].isin(subcategory_codes)))
        ]
        
        # Create a treemap of inflation rates by product hierarchy
        latest_hierarchy = hierarchy_data[hierarchy_data['date_key'] == latest_date]
        
        fig3 = px.treemap(
            latest_hierarchy,
            path=[px.Constant("All"), 'product_level', 'product_name'],
            values='price_index',
            color='inflation_rate_yoy',
            color_continuous_scale='RdYlGn_r',
            title=f'Hierarchical View of {selected_main_category} and Subcategories',
            hover_data=['inflation_rate_yoy', 'price_index']
        )
        fig3.update_layout(height=600)
        st.plotly_chart(fig3, use_container_width=True, key='treemap_hierarchy')
        
        # Line chart showing inflation trends for the main category and subcategories
        fig4 = px.line(
            hierarchy_data,
            x='date_key',
            y='inflation_rate_yoy',
            color='product_name',
            title=f'Inflation Trends for {selected_main_category} and Subcategories',
            labels={'date_key': 'Date', 'inflation_rate_yoy': 'Inflation Rate (%)', 'product_name': 'Product Category'}
        )
        st.plotly_chart(fig4, use_container_width=True, key='sunburst_hierarchy')
    
    with tab3:
        # Time series analysis
        st.subheader("Inflation Time Trends")
        
        # Select specific product categories to compare
        all_products = sorted(filtered_data['product_name'].unique())
        selected_products = st.multiselect("Select Product Categories to Compare", all_products, default=all_products[:5] if len(all_products) > 5 else all_products, key='tab3_product_multiselect')
        
        # Filter for selected products
        product_data = filtered_data[filtered_data['product_name'].isin(selected_products)]
        
        # Create a line chart of inflation rates over time by product category
        fig5 = px.line(
            product_data,
            x='date_key',
            y='inflation_rate_yoy',
            color='product_name',
            facet_col='country_name',
            facet_col_wrap=2,
            title='Inflation Rate Trends by Product Category and Country',
            labels={'date_key': 'Date', 'inflation_rate_yoy': 'Inflation Rate (%)', 'product_name': 'Product Category'}
        )
        fig5.update_layout(height=800)
        st.plotly_chart(fig5, use_container_width=True, key='line_product_time')
        
        # Heatmap of inflation rates over time
        # Aggregate by year and product
        yearly_data = product_data.groupby(['year', 'product_name'])['inflation_rate_yoy'].mean().reset_index()
        pivot_yearly = yearly_data.pivot_table(
            values='inflation_rate_yoy',
            index='product_name',
            columns='year',
            aggfunc='mean'
        )
        
        fig6 = px.imshow(
            pivot_yearly,
            labels=dict(x="Year", y="Product Category", color="Inflation Rate (%)"),
            x=pivot_yearly.columns,
            y=pivot_yearly.index,
            color_continuous_scale='RdYlGn_r',
            title='Inflation Rate Heatmap by Product Category and Year'
        )
        st.plotly_chart(fig6, use_container_width=True, key='heatmap_product_year')
    
    # Data table
    st.subheader("Detailed Inflation Data")
    st.dataframe(
        filtered_data[[
            'country_name', 'product_code', 'product_name', 'product_level',
            'parent_product_code', 'date_key', 'inflation_rate_yoy', 'price_index'
        ]].sort_values(['country_name', 'product_code', 'date_key']),
        use_container_width=True
    )

# Sample inflation data
inflation_data = pd.DataFrame({
    'country_code': ['DE', 'FR', 'ES', 'IT', 'DE', 'FR', 'ES', 'IT'] * 50,
    'country_name': ['Germany', 'France', 'Spain', 'Italy', 'Germany', 'France', 'Spain', 'Italy'] * 50,
    'product_code': ['CP00', 'CP01', 'CP02', 'CP03', 'CP04', 'CP05', 'CP06', 'CP07'] * 50,
    'product_name': ['All Items', 'Food and Beverages', 'Clothing', 'Housing', 'Furniture', 'Health', 'Transport', 'Communication'] * 50,
    'date_key': pd.date_range(start='2020-01-01', periods=400, freq='M').tolist(),
    'year': [d.year for d in pd.date_range(start='2020-01-01', periods=400, freq='M')] * 1,
    'month': [d.month for d in pd.date_range(start='2020-01-01', periods=400, freq='M')] * 1,
    'inflation_rate_yoy': [2.1, 3.2, 1.5, 4.3, 2.8, 1.9, 3.7, 2.5] * 50 + [x/10 for x in range(-20, 60, 10)] * 50,
    'inflation_rate_mom': [0.2, 0.3, 0.1, 0.4, 0.3, 0.2, 0.4, 0.3] * 50,
    'price_index': [105, 110, 103, 115, 108, 104, 112, 106] * 50,
    'product_level': [1, 1, 1, 1, 2, 2, 2, 2] * 50,
    'parent_product_code': [None, None, None, None, 'CP00', 'CP00', 'CP00', 'CP00'] * 50
})

# Sample product hierarchy
product_hierarchy = pd.DataFrame({
    'product_code': ['CP00', 'CP01', 'CP02', 'CP03', 'CP04', 'CP05', 'CP06', 'CP07'],
    'product_name': ['All Items', 'Food and Beverages', 'Clothing', 'Housing', 'Furniture', 'Health', 'Transport', 'Communication'],
    'category_description': ['All COICOP categories', 'Food, beverages and tobacco', 'Clothing and footwear', 'Housing, water, electricity, gas and other fuels', 
                           'Furniture, household equipment and maintenance', 'Health', 'Transport', 'Communications'],
    'main_category': ['Overall', 'Food', 'Clothing', 'Housing', 'Household', 'Health', 'Transport', 'Communication']
})
