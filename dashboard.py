import streamlit as st
import snowflake.connector
import pandas as pd

# Page Configuration
st.set_page_config(page_title="Live Data Dashboard", layout="wide")
st.title("📊 Real-Time Analytics Dashboard")

# Function to initialize Snowflake connection
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user='MohamedAshraf', 
        password='MohamedAshrafMigo2006#',
        account='kshuthc-lp84019',
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
        # Calculate quick KPIs
        total_revenue = df[df['EVENT_TYPE'] == 'purchase']['AMOUNT'].sum()
        total_events = len(df)
        
        # Display Metrics Side-by-Side
        col1, col2 = st.columns(2)
        col1.metric("Total Revenue (Last 100 Events)", f"${total_revenue:.2f}")
        col2.metric("Total Live Events Count", total_events)
        
        st.markdown("---")
        
        # Bar chart for event types
        st.subheader("📈 Live Interaction by Event Type")
        event_counts = df['EVENT_TYPE'].value_counts()
        st.bar_chart(event_counts)
        
        # Data table for live stream
        st.subheader("📋 Live Data Stream (Latest Logs)")
        st.dataframe(df)
        
        # Refresh Button
        if st.button("🔄 Refresh Data"):
            st.rerun()
            
    else:
        st.info("Waiting for incoming data... Please ensure both the Producer and Consumer are actively running!")

except Exception as e:
    st.error(f"Connection Error: {e}")