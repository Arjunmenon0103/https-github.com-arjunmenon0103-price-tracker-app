import streamlit as st
import pandas as pd
import plotly.express as px
import os
from PIL import Image

# Page configuration
st.set_page_config(page_title="Project Documentation", page_icon="📚", layout="wide")

# Title and description
st.title("Price Tracker Inflation Monitor: Project Documentation")
st.markdown("""
This page provides comprehensive documentation for the Price Tracker Inflation Monitor project,
including the data model, architecture, and implementation details.
""")

# Create tabs for different documentation sections
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Overview", "Data Model", "Architecture", "ETL Pipeline", "Dashboard"])

with tab1:
    st.header("Project Overview")
    st.markdown("""
    ### Price Tracker Inflation Monitor
    
    The Price Tracker Inflation Monitor is a comprehensive data pipeline and analytics dashboard that combines
    multiple data sources to provide insights into inflation trends and their impact on product prices.
    
    #### Data Sources
    
    1. **Eurostat HICP Data**: Detailed inflation rates across various product categories and subcategories for European countries.
    2. **World Bank Economic Indicators**: GDP, CPI, and other economic metrics that provide context for inflation trends.
    3. **Open Food Facts Product Pricing**: Real-world product prices from the Open Food Facts database, enabling analysis of price trends relative to inflation.
    
    #### Key Features
    
    - **Multi-source Data Integration**: Combines official inflation statistics with real-world product pricing data.
    - **Hierarchical Product Category Analysis**: Explores inflation across detailed COICOP product subcategories.
    - **Economic Context**: Correlates inflation with economic indicators like GDP per capita and growth rates.
    - **Price vs. Inflation Analysis**: Compares actual product price changes to official inflation rates.
    - **Data Quality Monitoring**: Comprehensive data quality checks across all data sources.
    - **Incremental Data Loading**: Supports daily updates with incremental loading patterns.
    
    #### Project Requirements
    
    - Uses 3 different data sources/formats (Eurostat API, World Bank API, Open Food Facts API)
    - Processes over 1 million rows of combined data
    - Implements a complete ETL pipeline with Airflow, dbt, and Snowflake
    - Includes comprehensive data quality checks
    - Provides interactive visualizations through a Streamlit dashboard
    """)

with tab2:
    st.header("Data Model")
    st.markdown("""
    ### Data Model Overview
    
    The data model follows a layered approach with staging, intermediate, and marts layers to ensure
    modularity, maintainability, and performance.
    
    #### Staging Layer
    
    The staging layer contains minimally transformed data from source systems:
    
    - `stg_eurostat_inflation`: Standardized Eurostat HICP data with product categories and subcategories
    - `stg_worldbank_economic`: Standardized World Bank economic indicators
    - `stg_open_food_facts`: Standardized Open Food Facts product pricing data
    
    #### Intermediate Layer
    
    The intermediate layer contains business logic transformations:
    
    - `int_inflation_rates`: Calculates month-over-month and year-over-year inflation rates
    - `int_economic_indicators`: Transforms economic indicators into analysis-ready format
    - `int_product_prices`: Standardizes product quantities, units, and categories
    
    #### Marts Layer
    
    The marts layer contains analytics-ready models for specific use cases:
    
    - `dim_countries`: Country dimension with metadata
    - `dim_products`: Product category dimension with hierarchy
    - `dim_date`: Date dimension with various date attributes
    - `fact_inflation_rates`: Fact table with inflation rates by country, product, and date
    - `fact_economic_indicators`: Fact table with economic indicators by country and date
    - `fact_product_prices`: Fact table with product prices and inflation comparison
    """)
    
    # Data model diagram
    st.subheader("Data Model Diagram")
    
    # Create a simple data model diagram using Plotly
    # Define the tables and their relationships
    tables = [
        {"name": "dim_countries", "layer": "Marts", "type": "Dimension"},
        {"name": "dim_products", "layer": "Marts", "type": "Dimension"},
        {"name": "dim_date", "layer": "Marts", "type": "Dimension"},
        {"name": "fact_inflation_rates", "layer": "Marts", "type": "Fact"},
        {"name": "fact_economic_indicators", "layer": "Marts", "type": "Fact"},
        {"name": "fact_product_prices", "layer": "Marts", "type": "Fact"},
        {"name": "int_inflation_rates", "layer": "Intermediate", "type": "Model"},
        {"name": "int_economic_indicators", "layer": "Intermediate", "type": "Model"},
        {"name": "int_product_prices", "layer": "Intermediate", "type": "Model"},
        {"name": "stg_eurostat_inflation", "layer": "Staging", "type": "Model"},
        {"name": "stg_worldbank_economic", "layer": "Staging", "type": "Model"},
        {"name": "stg_open_food_facts", "layer": "Staging", "type": "Model"},
        {"name": "raw_eurostat_inflation", "layer": "Raw", "type": "Source"},
        {"name": "raw_worldbank_economic", "layer": "Raw", "type": "Source"},
        {"name": "raw_open_food_facts", "layer": "Raw", "type": "Source"}
    ]
    
    # Create a DataFrame for visualization
    df_tables = pd.DataFrame(tables)
    
    # Create a treemap visualization of the data model
    fig = px.treemap(
        df_tables,
        path=["layer", "type", "name"],
        color="layer",
        color_discrete_map={
            "Raw": "#FFCCCC",
            "Staging": "#CCFFCC",
            "Intermediate": "#CCCCFF",
            "Marts": "#FFFFCC"
        },
        title="Data Model Hierarchy"
    )
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig, use_container_width=True)
    
    # Data dictionary
    st.subheader("Data Dictionary")
    st.markdown("""
    #### Fact Tables
    
    **fact_inflation_rates**
    - `country_code`: ISO country code (FK to dim_countries)
    - `country_name`: Country name
    - `product_code`: COICOP product code (FK to dim_products)
    - `product_name`: Product category name
    - `date_key`: Date in YYYY-MM-DD format (FK to dim_date)
    - `year`: Year of the measurement
    - `month`: Month of the measurement
    - `inflation_rate_mom`: Month-over-month inflation rate (%)
    - `inflation_rate_yoy`: Year-over-year inflation rate (%)
    - `price_index`: HICP price index value (2015=100)
    - `product_level`: Level in the product hierarchy (1=main, 2=subcategory, 3=detailed)
    - `parent_product_code`: Parent product code for hierarchical relationships
    
    **fact_economic_indicators**
    - `country_code`: ISO country code (FK to dim_countries)
    - `country_name`: Country name
    - `date_key`: Date in YYYY-MM-DD format (FK to dim_date)
    - `year`: Year of the measurement
    - `gdp_per_capita`: GDP per capita in USD
    - `cpi`: Consumer Price Index
    - `inflation_rate`: Annual inflation rate (%)
    - `gdp_growth_rate`: Annual GDP growth rate (%)
    
    **fact_product_prices**
    - `product_id`: Unique product identifier
    - `country_code`: ISO country code (FK to dim_countries)
    - `country_name`: Country name
    - `date_key`: Date in YYYY-MM-DD format (FK to dim_date)
    - `year`: Year of the measurement
    - `month`: Month of the measurement
    - `product_name`: Product name
    - `brand`: Product brand
    - `category`: Product category (mapped to COICOP)
    - `price_value`: Price in local currency
    - `price_per_unit`: Price per standard unit (kg or liter)
    - `quantity`: Product quantity
    - `unit`: Product unit (standardized)
    - `inflation_rate`: Corresponding inflation rate for this product category
    - `price_deviation`: Deviation of price from expected based on inflation
    
    #### Dimension Tables
    
    **dim_countries**
    - `country_code`: ISO country code (PK)
    - `country_name`: Country name
    - `region`: Geographic region
    - `currency_code`: Currency code
    - `eu_member`: Flag indicating EU membership
    
    **dim_products**
    - `product_code`: COICOP product code (PK)
    - `product_name`: Product category name
    - `product_level`: Level in the product hierarchy
    - `parent_product_code`: Parent product code
    - `category_description`: Detailed description
    - `main_category`: Top-level category
    
    **dim_date**
    - `date_key`: Date in YYYY-MM-DD format (PK)
    - `year`: Year
    - `month`: Month
    - `month_name`: Month name
    - `quarter`: Quarter
    - `year_month`: Year and month (YYYY-MM)
    - `is_end_of_month`: Flag for last day of month
    - `is_end_of_quarter`: Flag for last day of quarter
    - `is_end_of_year`: Flag for last day of year
    """)

with tab3:
    st.header("Architecture")
    st.markdown("""
    ### System Architecture
    
    The Price Tracker Inflation Monitor uses a modern data stack with the following components:
    
    #### Data Sources
    - **Eurostat API**: Provides HICP inflation data with detailed product categories
    - **World Bank API**: Provides economic indicators for context
    - **Open Food Facts API**: Provides product pricing data
    
    #### Data Ingestion
    - **Apache Airflow**: Orchestrates the ETL workflows
    - **Python**: Extracts data from APIs and transforms it into a standardized format
    - **S3/Iceberg**: Stores raw data in a scalable, versioned format
    
    #### Data Transformation
    - **dbt (data build tool)**: Transforms raw data into analytics-ready models
    - **Snowflake**: Serves as the data warehouse for storage and processing
    
    #### Data Visualization
    - **Streamlit**: Provides interactive dashboards for data exploration and analysis
    
    #### Data Quality
    - **dbt tests**: Validates data integrity and relationships
    - **Custom data quality checks**: Monitors data quality across all sources
    """)
    
    # Architecture diagram
    st.subheader("Architecture Diagram")
    
    # Create a simple architecture diagram using Plotly
    # Define the components and their relationships
    components = [
        {"name": "Eurostat API", "category": "Data Sources", "order": 1},
        {"name": "World Bank API", "category": "Data Sources", "order": 1},
        {"name": "Open Food Facts API", "category": "Data Sources", "order": 1},
        {"name": "Airflow DAGs", "category": "Data Ingestion", "order": 2},
        {"name": "Python Extractors", "category": "Data Ingestion", "order": 2},
        {"name": "S3/Iceberg", "category": "Data Storage", "order": 3},
        {"name": "Snowflake", "category": "Data Warehouse", "order": 4},
        {"name": "dbt Models", "category": "Data Transformation", "order": 5},
        {"name": "Data Quality Checks", "category": "Data Quality", "order": 6},
        {"name": "Streamlit Dashboard", "category": "Data Visualization", "order": 7}
    ]
    
    # Create a DataFrame for visualization
    df_components = pd.DataFrame(components)
    
    # Create a sunburst visualization of the architecture
    fig = px.sunburst(
        df_components,
        path=["order", "category", "name"],
        color="category",
        title="System Architecture"
    )
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.header("ETL Pipeline")
    st.markdown("""
    ### ETL Pipeline Overview
    
    The ETL pipeline is orchestrated by Apache Airflow and consists of the following components:
    
    #### Extraction
    
    1. **Eurostat DAG**:
       - Extracts HICP inflation data from the Eurostat API
       - Filters for specific countries and product categories
       - Handles the nested JSON structure and converts to tabular format
       - Loads data to Snowflake raw tables
    
    2. **World Bank DAG**:
       - Extracts economic indicators from the World Bank API
       - Filters for GDP, CPI, inflation, and other relevant indicators
       - Handles pagination and rate limiting
       - Loads data to Snowflake raw tables
    
    3. **Open Food Facts DAG**:
       - Extracts product pricing data from the Open Food Facts API
       - Implements incremental loading based on extraction date
       - Handles large data volumes with pagination
       - Loads data to Snowflake raw tables
    
    #### Transformation
    
    The transformation layer uses dbt to transform raw data into analytics-ready models:
    
    1. **Staging Models**:
       - Standardize field names and data types
       - Apply basic filtering and cleaning
       - Add metadata columns like load_date
    
    2. **Intermediate Models**:
       - Apply business logic transformations
       - Calculate derived metrics like inflation rates
       - Standardize units and categories
       - Map relationships between datasets
    
    3. **Marts Models**:
       - Create dimension and fact tables
       - Implement star schema for analytics
       - Join related data for comprehensive analysis
    
    #### Loading
    
    The transformed data is loaded into Snowflake tables for querying and analysis:
    
    - **Incremental Loading**: Updates only new or changed records
    - **Full Refresh**: Rebuilds tables completely when needed
    - **Materialized Views**: Optimizes query performance for dashboards
    """)
    
    # DAG workflow diagram
    st.subheader("Airflow DAG Workflow")
    
    # Create a simple DAG workflow diagram using Plotly
    # Define the DAG tasks and their dependencies
    dag_tasks = [
        {"dag": "eurostat_inflation_dag", "task": "extract_eurostat_data", "type": "Extract", "order": 1},
        {"dag": "eurostat_inflation_dag", "task": "transform_eurostat_data", "type": "Transform", "order": 2},
        {"dag": "eurostat_inflation_dag", "task": "load_to_snowflake", "type": "Load", "order": 3},
        {"dag": "eurostat_inflation_dag", "task": "run_dbt_models", "type": "dbt", "order": 4},
        {"dag": "worldbank_economic_dag", "task": "extract_worldbank_data", "type": "Extract", "order": 1},
        {"dag": "worldbank_economic_dag", "task": "transform_worldbank_data", "type": "Transform", "order": 2},
        {"dag": "worldbank_economic_dag", "task": "load_to_snowflake", "type": "Load", "order": 3},
        {"dag": "worldbank_economic_dag", "task": "run_dbt_models", "type": "dbt", "order": 4},
        {"dag": "open_food_facts_dag", "task": "extract_open_food_facts_data", "type": "Extract", "order": 1},
        {"dag": "open_food_facts_dag", "task": "transform_open_food_facts_data", "type": "Transform", "order": 2},
        {"dag": "open_food_facts_dag", "task": "load_to_snowflake", "type": "Load", "order": 3},
        {"dag": "open_food_facts_dag", "task": "run_dbt_models", "type": "dbt", "order": 4},
        {"dag": "data_quality_dag", "task": "run_data_quality_checks", "type": "Quality", "order": 5},
        {"dag": "data_quality_dag", "task": "notify_on_failure", "type": "Notification", "order": 6}
    ]
    
    # Create a DataFrame for visualization
    df_dag = pd.DataFrame(dag_tasks)
    
    # Create a grouped bar chart for the DAG workflow
    fig = px.bar(
        df_dag,
        x="order",
        y="dag",
        color="type",
        title="Airflow DAG Workflow",
        labels={"order": "Execution Order", "dag": "DAG", "type": "Task Type"},
        hover_data=["task"]
    )
    fig.update_layout(xaxis=dict(tickmode='linear'))
    st.plotly_chart(fig, use_container_width=True)
    
    # dbt model dependencies
    st.subheader("dbt Model Dependencies")
    st.markdown("""
    The dbt models follow a layered approach with dependencies flowing from staging to marts:
    
    ```
    # Staging Layer
    stg_eurostat_inflation
    stg_worldbank_economic
    stg_open_food_facts
    
    # Intermediate Layer
    int_inflation_rates (depends on: stg_eurostat_inflation)
    int_economic_indicators (depends on: stg_worldbank_economic)
    int_product_prices (depends on: stg_open_food_facts)
    
    # Dimension Tables
    dim_countries (depends on: int_inflation_rates, int_economic_indicators, int_product_prices)
    dim_products (depends on: int_inflation_rates)
    dim_date (depends on: int_inflation_rates, int_economic_indicators, int_product_prices)
    
    # Fact Tables
    fact_inflation_rates (depends on: int_inflation_rates, dim_countries, dim_products, dim_date)
    fact_economic_indicators (depends on: int_economic_indicators, dim_countries, dim_date)
    fact_product_prices (depends on: int_product_prices, fact_inflation_rates, dim_countries, dim_date)
    
    # Data Quality
    dq_checks (depends on: all raw sources)
    ```
    """)

with tab5:
    st.header("Dashboard")
    st.markdown("""
    ### Dashboard Overview
    
    The Price Tracker Inflation Monitor dashboard is built with Streamlit and provides interactive
    visualizations for exploring inflation trends and product prices.
    
    #### Dashboard Pages
    
    1. **Main Dashboard**: Overview of inflation trends across countries and product categories
    2. **Product Categories**: Detailed analysis of inflation by product category and subcategory
    3. **Economic Indicators**: Analysis of economic indicators and their relationship to inflation
    4. **Product Prices**: Comparison of actual product prices to inflation rates
    5. **Data Quality**: Monitoring of data quality metrics across all data sources
    6. **Documentation**: Project documentation and data dictionary
    
    #### Key Features
    
    - **Interactive Filters**: Filter by country, product category, date range, and more
    - **Multiple Visualizations**: Charts, tables, and maps for comprehensive analysis
    - **Drill-down Capability**: Explore from high-level trends to detailed subcategories
    - **Comparative Analysis**: Compare inflation across countries, categories, and time periods
    - **Data Quality Monitoring**: Track data quality metrics and issues
    """)
    
    # Dashboard screenshot placeholder
    st.subheader("Dashboard Screenshots")
    
    # Create tabs for different dashboard pages
    screenshot_tabs = st.tabs(["Main Dashboard", "Product Categories", "Economic Indicators", "Product Prices", "Data Quality"])
    
    with screenshot_tabs[0]:
        st.markdown("### Main Dashboard")
        st.markdown("""
        The main dashboard provides an overview of inflation trends across countries and product categories.
        It includes:
        - Country comparison charts
        - Inflation trend line charts
        - Product category heatmaps
        - Key metrics and KPIs
        """)
    
    with screenshot_tabs[1]:
        st.markdown("### Product Categories Dashboard")
        st.markdown("""
        The product categories dashboard allows detailed exploration of inflation by COICOP category and subcategory.
        It includes:
        - Hierarchical category treemaps
        - Category comparison charts
        - Time trend analysis by category
        - Subcategory drill-down capabilities
        """)
    
    with screenshot_tabs[2]:
        st.markdown("### Economic Indicators Dashboard")
        st.markdown("""
        The economic indicators dashboard analyzes the relationship between inflation and economic factors.
        It includes:
        - GDP vs. inflation scatter plots
        - Economic indicator trend charts
        - Country economic profile comparisons
        - Correlation analysis between indicators
        """)
    
    with screenshot_tabs[3]:
        st.markdown("### Product Prices Dashboard")
        st.markdown("""
        The product prices dashboard compares actual product prices to inflation rates.
        It includes:
        - Price vs. inflation comparison charts
        - Product category price analysis
        - Brand and product comparisons
        - Price deviation metrics
        """)
    
    with screenshot_tabs[4]:
        st.markdown("### Data Quality Dashboard")
        st.markdown("""
        The data quality dashboard monitors the quality of data across all sources.
        It includes:
        - Data quality check results
        - Source-specific quality metrics
        - Data volume tracking
        - Quality trend monitoring
        """)

# Footer
st.markdown("---")
st.markdown("""
**Price Tracker Inflation Monitor** | Developed for the Data Engineering Capstone Project | © 2025
""")
