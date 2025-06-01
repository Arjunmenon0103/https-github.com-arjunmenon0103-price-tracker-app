import streamlit as st
import pandas as pd
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
st.set_page_config(page_title="Data Quality Dashboard", page_icon="🔍", layout="wide")

# Title and description
st.title("Data Quality Dashboard")
st.markdown("""
This dashboard monitors the quality of data in the Price Tracker Inflation Monitor project.
It displays the results of various data quality checks performed on the Eurostat inflation data,
World Bank economic data, and Open Food Facts product pricing data.
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

# Function to load data quality check results
@st.cache_data(ttl=3600)
def load_dq_checks():
    try:
        conn = get_snowflake_connection()
        if conn is None:
            raise Exception("Could not establish Snowflake connection")
            
        query = """
        SELECT *
        FROM DQ_CHECKS
        ORDER BY check_name
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        # Create sample data for demonstration
        return pd.DataFrame({
            'check_name': ['eurostat_null_check', 'eurostat_date_check', 'eurostat_value_check', 'worldbank_null_check', 
                         'worldbank_year_check', 'openfood_null_check', 'openfood_price_check', 'openfood_duplicate_check', 
                         'volume_check'],
            'check_description': ['Checks for null values in critical Eurostat data columns', 
                               'Validates date format and range in Eurostat data',
                               'Checks for outliers in index values in Eurostat data',
                               'Checks for null values in critical World Bank data columns',
                               'Validates year format and range in World Bank data',
                               'Checks for null values in critical Open Food Facts data columns',
                               'Validates price values in Open Food Facts data',
                               'Checks for duplicate record_ids in Open Food Facts data',
                               'Checks if total data volume meets the 1 million rows requirement'],
            'check_status': ['PASS', 'PASS', 'PASS', 'PASS', 'FAIL', 'PASS', 'FAIL', 'PASS', 'PASS'],
            'checked_at': [pd.Timestamp.now()] * 9,
            'total_records': [250000, 250000, 250000, 150000, 150000, 600000, 600000, 600000, 1000000],
            'country_code_nulls': [0, None, None, 0, None, None, None, None, None],
            'product_code_nulls': [0, None, None, None, None, None, None, None, None],
            'date_nulls': [0, None, None, None, None, None, None, None, None],
            'index_value_nulls': [0, None, None, None, None, None, None, None, None],
            'invalid_dates': [None, 0, None, None, 150, None, None, None, None],
            'min_date': [None, '2020-01-01', None, None, None, None, None, None, None],
            'max_date': [None, '2023-12-31', None, None, None, None, None, None, None],
            'min_index': [None, None, 95.2, None, None, None, None, None, None],
            'max_index': [None, None, 125.7, None, None, None, None, None, None],
            'outlier_count': [None, None, 0, None, None, None, None, None, None],
            'negative_or_zero_prices': [None, None, None, None, None, None, 25, None, None],
            'duplicate_count': [None, None, None, None, None, None, None, 0, None],
            'eurostat_rows': [None, None, None, None, None, None, None, None, 250000],
            'worldbank_rows': [None, None, None, None, None, None, None, None, 150000],
            'openfood_rows': [None, None, None, None, None, None, None, None, 600000],
            'total_rows': [None, None, None, None, None, None, None, None, 1000000]
        })

# Function to load data volume metrics
@st.cache_data(ttl=3600)
def load_volume_metrics():
    try:
        conn = get_snowflake_connection()
        if conn is None:
            raise Exception("Could not establish Snowflake connection")
            
        query = """
        SELECT 
            'Eurostat' as Source,
            COUNT(*) as Rows
        FROM 
            FACT_INFLATION_RATES
        UNION ALL
        SELECT 
            'World Bank' as Source,
            COUNT(*) as Rows
        FROM 
            INT_ECONOMIC_INDICATORS
        UNION ALL
        SELECT 
            'Open Food Facts' as Source,
            COUNT(*) as Rows
        FROM 
            FACT_PRODUCT_PRICES
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"Could not connect to Snowflake: {e}")
        st.info("Loading sample volume metrics data for demonstration purposes...")
        
        # Create sample data for demonstration
        return pd.DataFrame({
            'Source': ['Eurostat', 'World Bank', 'Open Food Facts'],
            'Rows': [250000, 150000, 600000]
        })

# Load data
dq_checks = load_dq_checks()
volume_metrics = load_volume_metrics()

# Continue with the dashboard

# Summary metrics
st.subheader("Data Quality Summary")

# Calculate summary metrics
total_checks = len(dq_checks)
passing_checks = len(dq_checks[dq_checks['check_status'] == 'PASS'])
failing_checks = total_checks - passing_checks
pass_rate = (passing_checks / total_checks) * 100 if total_checks > 0 else 0

# Display metrics in columns
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Checks", total_checks)

with col2:
    st.metric("Passing Checks", passing_checks)

with col3:
    st.metric("Failing Checks", failing_checks)

with col4:
    st.metric("Pass Rate", f"{pass_rate:.1f}%")

# Display data volume metrics if available
if not volume_metrics.empty:
    st.subheader("Data Volume Metrics")
    
    # Get total rows
    total_rows = volume_metrics['Rows'].sum()
    
    # Create columns for metrics
    vol_col1, vol_col2, vol_col3, vol_col4 = st.columns(4)
    
    # Find rows for each source
    eurostat_rows = volume_metrics[volume_metrics['Source'] == 'Eurostat']['Rows'].iloc[0] if 'Eurostat' in volume_metrics['Source'].values else 0
    worldbank_rows = volume_metrics[volume_metrics['Source'] == 'World Bank']['Rows'].iloc[0] if 'World Bank' in volume_metrics['Source'].values else 0
    openfood_rows = volume_metrics[volume_metrics['Source'] == 'Open Food Facts']['Rows'].iloc[0] if 'Open Food Facts' in volume_metrics['Source'].values else 0
    
    with vol_col1:
        st.metric("Eurostat Rows", f"{eurostat_rows:,}")
    
    with vol_col2:
        st.metric("World Bank Rows", f"{worldbank_rows:,}")
    
    with vol_col3:
        st.metric("Open Food Facts Rows", f"{openfood_rows:,}")
    
    with vol_col4:
        st.metric("Total Rows", f"{total_rows:,}", 
                 delta=f"{total_rows - 1000000:,}",
                 delta_color="normal")
    
    # Use the volume_metrics DataFrame directly for the pie chart
    volume_data = volume_metrics
    
    fig_pie = px.pie(
        volume_data,
        values='Rows',
        names='Source',
        title='Data Volume by Source',
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    st.plotly_chart(fig_pie)

# Create tabs for different views
tab1, tab2, tab3 = st.tabs(["Check Results", "Source-specific Checks", "Detailed Metrics"])

with tab1:
    # Create a bar chart of check status by check name
    fig1 = px.bar(
        dq_checks,
        x='check_name',
        y=[1] * len(dq_checks),  # Just to create bars of equal height
        color='check_status',
        title='Data Quality Check Results',
        labels={'check_name': 'Check Name', 'y': 'Status', 'check_status': 'Status'},
        color_discrete_map={'PASS': 'green', 'FAIL': 'red'},
        hover_data=['check_description', 'checked_at']
    )
    fig1.update_layout(showlegend=True, xaxis_tickangle=-45)
    st.plotly_chart(fig1, use_container_width=True)
    
    # Group checks by source and status
    source_groups = {
        'eurostat': 'Eurostat',
        'worldbank': 'World Bank',
        'openfood': 'Open Food Facts',
        'volume': 'Volume'
    }
    
    source_status = []
    for source_prefix, source_name in source_groups.items():
        source_checks = dq_checks[dq_checks['check_name'].str.startswith(source_prefix)]
        passing = len(source_checks[source_checks['check_status'] == 'PASS'])
        failing = len(source_checks[source_checks['check_status'] == 'FAIL'])
        total = len(source_checks)
        if total > 0:
            source_status.append({
                'Source': source_name,
                'Passing': passing,
                'Failing': failing,
                'Total': total,
                'Pass Rate': (passing / total) * 100
            })
    
    source_status_df = pd.DataFrame(source_status)
    
    if not source_status_df.empty:
        # Create a grouped bar chart of check status by source
        fig2 = px.bar(
            source_status_df,
            x='Source',
            y=['Passing', 'Failing'],
            title='Data Quality Checks by Source',
            labels={'value': 'Number of Checks', 'variable': 'Status'},
            color_discrete_map={'Passing': 'green', 'Failing': 'red'},
            barmode='group'
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        # Create a gauge chart for overall pass rate
        fig3 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pass_rate,
            title={'text': "Overall Pass Rate"},
            domain={'x': [0, 1], 'y': [0, 1]},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': "green"},
                'steps': [
                    {'range': [0, 60], 'color': "red"},
                    {'range': [60, 80], 'color': "orange"},
                    {'range': [80, 100], 'color': "lightgreen"}
                ],
                'threshold': {
                    'line': {'color': "darkgreen", 'width': 4},
                    'thickness': 0.75,
                    'value': 95
                }
            }
        ))
        fig3.update_layout(height=300)
        st.plotly_chart(fig3, use_container_width=True)
    
with tab2:
    # Create source filter
    source_prefixes = ['eurostat', 'worldbank', 'openfood', 'volume']
    source_names = ['Eurostat', 'World Bank', 'Open Food Facts', 'Volume']
    selected_source = st.selectbox("Select Data Source", 
                                  range(len(source_names)), 
                                  format_func=lambda i: source_names[i])
    
    # Filter checks by selected source
    selected_prefix = source_prefixes[selected_source]
    source_checks = dq_checks[dq_checks['check_name'].str.startswith(selected_prefix)]
    
    if not source_checks.empty:
        # Display source-specific metrics
        source_total = len(source_checks)
        source_passing = len(source_checks[source_checks['check_status'] == 'PASS'])
        source_failing = source_total - source_passing
        source_pass_rate = (source_passing / source_total) * 100 if source_total > 0 else 0
        
        src_col1, src_col2, src_col3 = st.columns(3)
        
        with src_col1:
            st.metric(f"{source_names[selected_source]} Total Checks", source_total)
        
        with src_col2:
            st.metric(f"{source_names[selected_source]} Passing Checks", source_passing)
        
        with src_col3:
            st.metric(f"{source_names[selected_source]} Pass Rate", f"{source_pass_rate:.1f}%")
        
        # Create a table of source-specific checks
        st.subheader(f"{source_names[selected_source]} Data Quality Checks")
        
        # Format the table for display
        display_cols = ['check_name', 'check_description', 'check_status', 'checked_at']
        
        # Add relevant metrics based on source
        if selected_prefix == 'eurostat':
            additional_cols = ['total_records', 'country_code_nulls', 'product_code_nulls', 
                             'date_nulls', 'index_value_nulls', 'invalid_dates', 'outlier_count']
        elif selected_prefix == 'worldbank':
            additional_cols = ['total_records', 'country_code_nulls', 'indicator_code_nulls', 
                             'year_nulls', 'value_nulls', 'invalid_years']
        elif selected_prefix == 'openfood':
            additional_cols = ['total_records', 'record_id_nulls', 'product_id_nulls', 
                             'price_value_nulls', 'negative_or_zero_prices', 'duplicate_count']
        elif selected_prefix == 'volume':
            additional_cols = ['eurostat_rows', 'worldbank_rows', 'openfood_rows', 'total_rows']
        else:
            additional_cols = []
        
        display_cols.extend([col for col in additional_cols if col in source_checks.columns])
        
        # Display the table
        st.dataframe(source_checks[display_cols], use_container_width=True)
        
        # Create visualizations based on source
        if selected_prefix == 'eurostat':
            # Show index value distribution if available
            if 'min_index' in source_checks.columns and 'max_index' in source_checks.columns:
                index_check = source_checks[source_checks['check_name'] == 'eurostat_value_check']
                if not index_check.empty:
                    st.subheader("Eurostat Index Value Distribution")
                    # Get available columns and provide fallback values for missing ones
                    min_val = index_check['min_index'].iloc[0] if 'min_index' in index_check.columns else 0
                    avg_val = index_check['avg_index'].iloc[0] if 'avg_index' in index_check.columns else min_val * 1.5
                    median_val = index_check['median_index'].iloc[0] if 'median_index' in index_check.columns else avg_val
                    max_val = index_check['max_index'].iloc[0] if 'max_index' in index_check.columns else min_val * 2
                    
                    index_data = pd.DataFrame({
                        'Metric': ['Min', 'Average', 'Median', 'Max'],
                        'Value': [min_val, avg_val, median_val, max_val]
                    })
                    
                    fig4 = px.bar(
                        index_data,
                        x='Metric',
                        y='Value',
                        title='Eurostat Index Value Distribution',
                        color='Metric'
                    )
                    st.plotly_chart(fig4, use_container_width=True)
            
            # Display the table
            st.dataframe(source_checks[display_cols], use_container_width=True)
            
            # Additional visualizations for other data sources
            if selected_prefix == 'openfood':
                # Show price distribution if available
                price_check = source_checks[source_checks['check_name'] == 'openfood_price_check']
                if not price_check.empty and 'min_price' in price_check.columns:
                    st.subheader("Open Food Facts Price Distribution")
                    # Get available columns and provide fallback values for missing ones
                    min_val = price_check['min_price'].iloc[0] if 'min_price' in price_check.columns else 0
                    avg_val = price_check['avg_price'].iloc[0] if 'avg_price' in price_check.columns else min_val * 1.5
                    median_val = price_check['median_price'].iloc[0] if 'median_price' in price_check.columns else avg_val
                    max_val = price_check['max_price'].iloc[0] if 'max_price' in price_check.columns else min_val * 2
                    
                    price_data = pd.DataFrame({
                        'Metric': ['Min', 'Average', 'Median', 'Max'],
                        'Value': [min_val, avg_val, median_val, max_val]
                    })
                    
                    fig5 = px.bar(
                        price_data,
                        x='Metric',
                        y='Value',
                        title='Open Food Facts Price Distribution',
                        color='Metric'
                    )
                    st.plotly_chart(fig5, use_container_width=True)
    
with tab3:
    # Display detailed metrics for all checks
    st.subheader("Detailed Data Quality Metrics")
    
    # Allow filtering by check status
    status_filter = st.radio("Filter by Status", ["All", "PASS", "FAIL"], horizontal=True)
    
    if status_filter != "All":
        filtered_checks = dq_checks[dq_checks['check_status'] == status_filter]
    else:
        filtered_checks = dq_checks
    
    # Display the filtered checks
    if not filtered_checks.empty:
        st.dataframe(filtered_checks, use_container_width=True)
    else:
        st.info("No checks match the selected filter.")
