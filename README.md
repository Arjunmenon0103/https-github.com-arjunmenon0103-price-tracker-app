# Price Tracker Inflation Monitor Dashboard

This Streamlit dashboard visualizes the relationship between product prices and inflation rates across different countries, categories, and time periods. It provides interactive filters, visualizations, and data tables to explore economic trends and price changes.

Link to the dashboard - https://analyticeengineeringbootcampmay2025pricetracker.streamlit.app/


## Features

- **Multiple Data Sources**: Combines Eurostat inflation data and Open Food Facts product data
- **Interactive Filters**: Filter by country, product category, brand, and date range
- **Visualizations**: Charts for price trends, inflation rates, and category comparisons
- **Data Quality**: Metrics and checks for data completeness and accuracy
- **Offline Mode**: Works with sample data when database connections are unavailable

## Project Overview

This dashboard is part of a larger data engineering project that includes:

- **Data Ingestion**: Airflow DAGs for extracting data from Eurostat and Open Food Facts APIs
- **Data Lake**: Apache Iceberg implementation for storing raw and processed data
- **Data Warehouse**: Snowflake integration for analytics queries
- **Transformation**: dbt models for creating analytics-ready data
- **Visualization**: This Streamlit dashboard for exploring the data

## Setup Instructions

### Prerequisites

- Python 3.7+ installed
- Git (for cloning the repository)

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Arjunmenon0103/https-github.com-arjunmenon0103-price-tracker-app.git
   cd https-github.com-arjunmenon0103-price-tracker-app
   ```

2. **Create and activate a virtual environment** (recommended):
   ```bash
   # On macOS/Linux
   python -m venv venv
   source venv/bin/activate
   
   # On Windows
   python -m venv venv
   .\venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables** (optional, for Snowflake connection):
   Create a `.env` file in the project root with your Snowflake credentials:
   ```
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   ```

### Running the Dashboard

1. **Start the Streamlit app**:
   ```bash
   cd /path/to/capstone-project
   streamlit run streamlit_app/app.py
   ```

2. **Access the dashboard** in your web browser at:
   ```
   http://localhost:8501
   ```

3. **Navigate the dashboard** using the sidebar to select different tabs:
   - Home/Overview
   - Data Quality
   - Documentation
   - Economic Indicators
   - Product Categories
   - Product Prices

## Troubleshooting

- **Missing dependencies**: If you encounter import errors, ensure all dependencies are installed with `pip install -r requirements.txt`
- **Connection errors**: The app will automatically fall back to sample data if Snowflake connection fails
- **UI errors**: If you encounter UI issues, try refreshing the page or restarting the app

## Development Notes

- The dashboard uses synthetic sample data when database connections are unavailable
- Each visualization has unique keys to prevent duplicate element ID errors
- Date fields are properly converted to datetime objects for filtering and display
