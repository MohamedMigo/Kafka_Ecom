import os
import streamlit as st
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page Configuration
st.set_page_config(page_title="Live Data Dashboard", layout="wide")
st.title("📊 Real-Time Analytics Dashboard")

# Function to initialize Snowflake connection safely
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),          
        password=os.getenv("SNOWFLAKE_PASSWORD"),  
        account=os.getenv("SNOWFLAKE_ACCOUNT"),    
        warehouse='COMPUTE_WH',
        database='ECOMMERCE_DB',
        schema='PUBLIC'
    )

try:
    conn = init_connection()
    
    # Fetch the latest 100 events from Snowflake
    query = "SELECT * FROM clean_events ORDER BY EVENT_TIMESTAMP DESC LIMIT 100;"
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        # Format Timestamp for plotting
        df['EVENT_TIMESTAMP'] = pd.to_datetime(df['EVENT_TIMESTAMP'])
        
        # Calculate KPIs
        purchases_df = df[df['EVENT_TYPE'] == 'purchase']
        total_revenue = purchases_df['AMOUNT'].sum()
        total_events = len(df)
        avg_purchase = purchases_df['AMOUNT'].mean() if not purchases_df.empty else 0
        
        # Display Metrics Side-by-Side
        col1, col2, col3 = st.columns(3)
        col1.metric("💰 Total Revenue", f"${total_revenue:.2f}")
        col2.metric("🔄 Total Live Events", total_events)
        col3.metric("💳 Avg Purchase Value", f"${avg_purchase:.2f}")
        
        st.markdown("---")
        
        # Row 1: Two charts side-by-side
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.subheader("📈 Event Types Distribution")
            event_counts = df['EVENT_TYPE'].value_counts()
            st.bar_chart(event_counts)
            
        with col_chart2:
            st.subheader("🚀 Revenue Trend Over Time")
            if not purchases_df.empty:
                # Set index to timestamp for line chart
                trend_data = purchases_df.set_index('EVENT_TIMESTAMP')['AMOUNT']
                st.line_chart(trend_data)
            else:
                st.info("Waiting for 'purchase' events to draw revenue trend...")

        st.markdown("---")
        
        # Row 2: Full-width scatter chart
        st.subheader("⏱️ Live Event Activity Scatter")
        # Scatter chart to show distribution of amounts over time colored by event type
        st.scatter_chart(df, x='EVENT_TIMESTAMP', y='AMOUNT', color='EVENT_TYPE')
        
        st.markdown("---")
        
        # Data table for live stream
        st.subheader("📋 Live Data Stream (Latest Logs)")
        st.dataframe(df, use_container_width=True)
        
        # Refresh Button
        if st.button("🔄 Refresh Data"):
            st.rerun()
            
    else:
        st.info("Waiting for incoming data... Please ensure both the Producer and Consumer are actively running!")

except Exception as e:
    st.error(f"Connection Error: {e}")